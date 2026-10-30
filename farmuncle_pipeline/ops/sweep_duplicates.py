"""Nightly: look for duplicate mandis, crops and varieties among everything created so far, and list them for review.

Only fills the review tables (mandi_duplicate_candidates, crop_duplicate_review_queue, variety_duplicate_review_queue);
nothing is ever merged here. Each function skips pairs it already listed, so a re-run only adds new ones.
"""

from __future__ import annotations

import os

SWEEPS = ("find_mandi_duplicates", "sweep_duplicate_crops", "sweep_duplicate_varieties")


def sweep(db) -> dict:
    return {name: db.rpc(name).execute().data for name in SWEEPS}


def main() -> None:
    from supabase import create_client

    db = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])
    print(f"[sweep_duplicates] new pairs listed: {sweep(db)}")


if __name__ == "__main__":
    main()
