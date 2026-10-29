"""One-off: copy clean mandi names from the app DB (mandi_display_names) into the warehouse's mandis.display_name.

Skips names still marked needs_locality_review. Safe to re-run. Needs SUPABASE_* (warehouse) and STOREFRONT_SUPABASE_* (app DB).
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor

from supabase import create_client

PAGE = 1000


def main() -> None:
    app = create_client(os.environ["STOREFRONT_SUPABASE_URL"], os.environ["STOREFRONT_SUPABASE_SERVICE_KEY"])
    wh = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])
    rows, start = [], 0
    while True:
        page = (app.table("mandi_display_names").select("mandi_id,display_name,name_status").order("mandi_id")
                .range(start, start + PAGE - 1).execute().data or [])
        rows.extend(page)
        if len(page) < PAGE:
            break
        start += PAGE
    todo = [r for r in rows if r["name_status"] in ("normalised", "reviewed_override") and (r["display_name"] or "").strip()]

    def put(r):
        wh.table("mandis").update({"display_name": r["display_name"].strip()}).eq("id", r["mandi_id"]).execute()

    with ThreadPoolExecutor(8) as pool:
        list(pool.map(put, todo))
    print(f"[copy_display_names] {len(todo)} of {len(rows)} names copied")


if __name__ == "__main__":
    main()
