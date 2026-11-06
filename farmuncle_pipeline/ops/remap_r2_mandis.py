"""Point archived R2 price days at the mandi each row now belongs to, and report which mandi pairs ever traded the same day.

Usage: python -m farmuncle_pipeline.ops.remap_r2_mandis OUT_DIR [--apply]
Without --apply it only reads. With it, a day whose file names a merged mandi is rewritten with the surviving id
(source_mandi_id keeps the id the row was archived under, as union_merge_mandi does in the warehouse). A rewrite
keeps every row: it is refused unless the new file has exactly the rows of the old one and of its manifest.

Writes to OUT_DIR: days.csv (per day: rows, rows remapped), pair_overlap.csv (per candidate pair: days each traded,
days both traded), mandi_presence.csv (per mandi id as archived: first/last day, days, rows).
"""

from __future__ import annotations

import csv
import difflib
import hashlib
import io
import json
import re
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from farmuncle_pipeline.archive.export import manifest_key
from farmuncle_pipeline.archive.store import R2Store


def survivors(mandis: list[dict]) -> dict[int, int]:
    """Merged id -> its ACTIVE survivor, following chains; ids with no active end are left out."""
    by_id = {m["id"]: m for m in mandis}
    out = {}
    for m in mandis:
        if m["status"] != "MERGED":
            continue
        cur = m
        for _ in range(10):
            cur = by_id.get(cur.get("merged_into_id"))
            if cur is None or cur["status"] == "ACTIVE":
                break
        if cur is not None and cur["status"] == "ACTIVE":
            out[m["id"]] = cur["id"]
    return out


_NOISE = re.compile(r"\b(apmc|rm|mandi|market|vfpck|f\s*&\s*v|grain|yard|sub)\b|[^a-z ]")


def _key(name: str) -> str:
    return re.sub(r"\s+", " ", _NOISE.sub(" ", (name or "").lower())).strip()


def candidate_pairs(mandis: list[dict], merged: dict[int, int]) -> dict[tuple[int, int], str]:
    """(a, b) -> kind: every merge, same-name active mandis left apart, and near-identical names in one state."""
    pairs = {tuple(sorted((loser, kept))): "merged" for loser, kept in merged.items()}
    active = [m for m in mandis if m["status"] == "ACTIVE"]
    by_state: dict[str, list[dict]] = defaultdict(list)
    for m in active:
        by_state[m["state"]].append(m)
    for rows in by_state.values():
        keys = [_key(m["name"]) for m in rows]
        for i, a in enumerate(rows):
            for j in range(i + 1, len(rows)):
                b = rows[j]
                if a["normalized_name"] == b["normalized_name"]:
                    kind = "same_name_not_merged"
                elif keys[i] == keys[j] or difflib.SequenceMatcher(None, keys[i], keys[j]).ratio() >= 0.85:
                    kind = "similar_name_not_merged"
                else:
                    continue
                pairs.setdefault(tuple(sorted((a["id"], b["id"]))), kind)
    return pairs


def remap_table(table, merged: dict[int, int]):
    """Return (new table, rows changed). source_mandi_id is filled with the archived id where it was empty."""
    import pyarrow as pa

    ids = table.column("mandi_id").to_pylist()
    new_ids = [merged.get(i, i) for i in ids]
    changed = sum(1 for a, b in zip(ids, new_ids) if a != b)
    if not changed:
        return table, 0
    src = table.column("source_mandi_id").to_pylist()
    src = [s if s is not None else (a if a != b else None) for s, a, b in zip(src, ids, new_ids)]
    table = table.set_column(table.schema.get_field_index("mandi_id"), "mandi_id", pa.array(new_ids, type=table.schema.field("mandi_id").type))
    field = table.schema.field("source_mandi_id")
    src_type = field.type if not pa.types.is_null(field.type) else pa.int64()
    table = table.set_column(table.schema.get_field_index("source_mandi_id"), pa.field("source_mandi_id", src_type), pa.array(src, type=src_type))
    return table, changed


def main() -> None:
    import pyarrow.parquet as pq

    out_dir, apply = Path(sys.argv[1]), "--apply" in sys.argv
    out_dir.mkdir(parents=True, exist_ok=True)
    import os

    from supabase import create_client

    db = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])  # needs only the warehouse and R2
    mandis, start = [], 0
    while True:
        page = db.table("mandis").select("id,name,normalized_name,state,district,status,merged_into_id").order("id").range(start, start + 999).execute().data or []
        mandis += page
        if len(page) < 1000:
            break
        start += 1000
    merged = survivors(mandis)
    pairs = candidate_pairs(mandis, merged)
    watch = {i for p in pairs for i in p}
    pairs_of: dict[int, list[tuple[int, int]]] = defaultdict(list)
    for pair in pairs:
        pairs_of[pair[0]].append(pair)
    store = R2Store.from_env()
    keys = sorted(k for k in store.list("facts/daily/") if k.endswith(".parquet"))
    print(f"[remap] {len(mandis)} mandis, {len(merged)} merged, {len(pairs)} candidate pairs, {len(keys)} archived days, apply={apply}")

    def one(key: str):
        day = key.split("date=")[1][:10]
        table = pq.read_table(io.BytesIO(store.get(key)))
        ids = table.column("mandi_id").to_pylist()
        present = defaultdict(int)
        for i in ids:
            present[i] += 1
        # Same day, same crop/variety/grade at both mandis of a pair: is it one market listed twice (same price)?
        prices: dict[int, dict] = defaultdict(dict)
        cols = [table.column(c).to_pylist() for c in ("crop_id", "variety_id", "grade_id", "modal_price")]
        for i, crop, variety, grade, modal in zip(ids, *cols):
            if i in watch:
                prices[i][(crop, variety, grade)] = modal
        agree = {}
        for a in prices:
            for pair in pairs_of.get(a, ()):
                other = prices.get(pair[1])
                if other:
                    shared = prices[a].keys() & other.keys()
                    agree[pair] = (len(shared), sum(1 for k in shared if prices[a][k] == other[k]))
        new, changed = remap_table(table, merged)
        status = "unchanged"
        if changed and apply:
            from datetime import date

            buf = io.BytesIO()
            pq.write_table(new, buf, compression="zstd")
            data = buf.getvalue()
            check = pq.read_table(io.BytesIO(data))
            try:
                manifest = json.loads(store.get(manifest_key(date.fromisoformat(day))))
            except Exception:
                manifest = None
            if check.num_rows != table.num_rows or (manifest and int(manifest["rows"]) != table.num_rows):
                status = f"REFUSED rows file={table.num_rows} new={check.num_rows} manifest={manifest and manifest['rows']}"
            else:
                store.put(key, data)
                if store.size(key) != len(data):
                    raise RuntimeError(f"{key}: stored size does not match")
                if manifest:
                    manifest.update(bytes=len(data), sha256=hashlib.sha256(data).hexdigest(), mandis_remapped="2026-11-06")
                    store.put(manifest_key(date.fromisoformat(day)), json.dumps(manifest, indent=1).encode(), "application/json")
                status = "rewritten"
        elif changed:
            status = "would_rewrite"
        return day, table.num_rows, changed, status, dict(present), agree

    presence = defaultdict(lambda: [None, None, 0, 0])  # first, last, days, rows (by archived id)
    traded: dict[int, set[str]] = defaultdict(set)
    compared: dict[tuple[int, int], list[int]] = defaultdict(lambda: [0, 0])  # shared combos, same modal price
    with open(out_dir / "days.csv", "w", newline="", encoding="utf-8") as f, ThreadPoolExecutor(16) as pool:
        w = csv.writer(f)
        w.writerow(["day", "rows", "rows_remapped", "status"])
        for day, rows, changed, status, present, agree in pool.map(one, keys):
            w.writerow([day, rows, changed, status])
            for pair, (shared, same) in agree.items():
                compared[pair][0] += shared
                compared[pair][1] += same
            for i, n in present.items():
                p = presence[i]
                p[0] = day if p[0] is None or day < p[0] else p[0]
                p[1] = day if p[1] is None or day > p[1] else p[1]
                p[2] += 1
                p[3] += n
                if i in watch:
                    traded[i].add(day)
    by_id = {m["id"]: m for m in mandis}
    with open(out_dir / "pair_overlap.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["kind", "state", "id_a", "name_a", "district_a", "id_b", "name_b", "district_b", "days_a", "days_b", "days_both", "shared_prices", "same_price", "first_a", "last_a", "first_b", "last_b"])
        for (a, b), kind in sorted(pairs.items(), key=lambda x: (x[1], by_id[x[0][0]]["state"])):
            ma, mb = by_id[a], by_id[b]
            w.writerow([kind, ma["state"], a, ma["name"], ma["district"], b, mb["name"], mb["district"],
                        len(traded[a]), len(traded[b]), len(traded[a] & traded[b]), compared[(a, b)][0], compared[(a, b)][1],
                        presence[a][0], presence[a][1], presence[b][0], presence[b][1]])
    with open(out_dir / "mandi_presence.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["archived_mandi_id", "kept_id", "first_day", "last_day", "days", "rows"])
        for i, (first, last, days, rows) in sorted(presence.items(), key=lambda x: (x[0] is None, x[0] or 0)):
            w.writerow([i, merged.get(i, i), first, last, days, rows])
    print(f"[remap] done; reports in {out_dir}")


if __name__ == "__main__":
    main()
