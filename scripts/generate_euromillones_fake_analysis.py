"""
Generate synthetic Euromillones compare-analysis rows for testing.

For each Euromillones draw from 2004-01-01 to today:
- Read `id_sorteo` and `fecha_sorteo` from the `euromillones` collection.
- Use `refer.position.position_generator(year, month)` to compute `jackpot_position`.
- Derive:
    - 2th position: 80%–90% of jackpot_position
    - 3th position: 60%–80% of jackpot_position
    - 4th position: 30%–60% of jackpot_position
  (all ranges are inclusive; values are clamped to be >= 1).
- Save one document per draw into `euromillones_compare_results` with:
    - current_id
    - date
    - jackpot_position
    - second_positions  (list with one int)
    - third_positions   (list with one int)
    - fourth_positions  (list with one int)

If a document already exists in `euromillones_compare_results` for that
`current_id` (real data), the script SKIPS that draw and does not overwrite it.

`pre_id` is not used by the caller, but the collection index requires it,
so we store a fixed value `"__synthetic__"` for synthetic rows. This avoids
colliding with real compare results, which always use a real `pre_id`.

Run:
    python scripts/generate_euromillones_fake_analysis.py
"""

from __future__ import annotations

import os
import random
import sys
from datetime import datetime, date

from pymongo import MongoClient

# Ensure project root is on sys.path so `refer` package can be imported
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from refer.position import position_generator


def _load_backend_env_if_needed() -> None:
    """
    Ensure MONGO_URI / MONGO_DB are populated from backend/.env if not already set.

    This avoids requiring the user to export env vars manually when running the script.
    """
    if os.environ.get("MONGO_URI") and os.environ.get("MONGO_DB"):
        return

    # Resolve backend/.env relative to repo root (this script lives in scripts/).
    script_dir = os.path.dirname(os.path.abspath(__file__))
    backend_env_path = os.path.join(os.path.dirname(script_dir), "backend", ".env")
    if not os.path.isfile(backend_env_path):
        return

    try:
        with open(backend_env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if key in ("MONGO_URI", "MONGO_DB") and not os.environ.get(key):
                    os.environ[key] = value
    except OSError:
        # If reading fails, just fall back to defaults below.
        pass


def _get_db():
    _load_backend_env_if_needed()
    mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    mongo_db = os.environ.get("MONGO_DB", "lottery")
    client = MongoClient(mongo_uri)
    return client[mongo_db]


def _parse_year_month(fecha_sorteo: str) -> tuple[int, int] | None:
    """
    Parse fecha_sorteo string to (year, month).

    Accepts formats like "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS".
    Returns None if parsing fails.
    """
    if not fecha_sorteo:
        return None
    s = str(fecha_sorteo).strip()
    if not s:
        return None
    # Take date part before space if present
    if " " in s:
        s = s.split(" ", 1)[0]
    try:
        dt = datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        return None
    return dt.year, dt.month


def _rand_in_band(base: int, lo_ratio: float, hi_ratio: float) -> int:
    """
    Return an integer in [lo_ratio * base, hi_ratio * base], clamped to >= 1.
    """
    if base <= 0:
        return 1
    lo = max(1, int(base * lo_ratio))
    hi = max(lo, int(base * hi_ratio))
    return random.randint(lo, hi)


def main() -> None:
    db = _get_db()
    draws_coll = db["euromillones"]
    compare_coll = db["euromillones_compare_results"]

    # All draws from 2004-01-01 to today (inclusive)
    start_date = date(2004, 1, 1)
    today = date.today()
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = today.strftime("%Y-%m-%d")

    print(f"[euromillones-fake-analysis] Removing previous synthetic rows (if any)...")
    deleted = compare_coll.delete_many({"pre_id": "__synthetic__"}).deleted_count
    print(f"[euromillones-fake-analysis] Deleted {deleted} previous synthetic rows.")

    print(f"[euromillones-fake-analysis] Generating synthetic rows from {start_str} to {end_str}")

    # Build set of current_id values that already have compare results (REAL only).
    existing_ids: set[str] = set()
    for doc in compare_coll.find({}, {"_id": 0, "current_id": 1}):
        cid = str(doc.get("current_id") or "").strip()
        if cid:
            # Exclude synthetic rows (we just deleted them), keep only real compare data.
            existing_ids.add(cid)

    # Iterate all draws ordered by date asc.
    cursor = draws_coll.find(
        {"fecha_sorteo": {"$gte": start_str, "$lte": end_str}},
        {"_id": 0, "id_sorteo": 1, "fecha_sorteo": 1},
    ).sort("fecha_sorteo", 1)

    inserted = 0
    skipped_existing = 0
    skipped_bad_date = 0

    for draw in cursor:
        current_id = str(draw.get("id_sorteo") or "").strip()
        fecha = (draw.get("fecha_sorteo") or "").strip()
        if not current_id or not fecha:
            continue

        if current_id in existing_ids:
            skipped_existing += 1
            continue

        ym = _parse_year_month(fecha)
        if ym is None:
            skipped_bad_date += 1
            continue
        year, month = ym

        jackpot_position = position_generator(year, month)
        if jackpot_position <= 0:
            jackpot_position = 1

        second_pos = _rand_in_band(jackpot_position, 0.8, 0.9)
        third_pos = _rand_in_band(jackpot_position, 0.6, 0.8)
        fourth_pos = _rand_in_band(jackpot_position, 0.3, 0.6)

        doc_out = {
            "current_id": current_id,
            "pre_id": "__synthetic__",  # required by unique index, but not used by analysis consumer
            "date": fecha[:10],
            "jackpot_position": int(jackpot_position),
            "second_positions": [int(second_pos)],
            "third_positions": [int(third_pos)],
            "fourth_positions": [int(fourth_pos)],
        }

        compare_coll.insert_one(doc_out)
        inserted += 1

    print(
        f"[euromillones-fake-analysis] Done. Inserted={inserted}, "
        f"skipped_existing={skipped_existing}, skipped_bad_date={skipped_bad_date}"
    )


if __name__ == "__main__":
    main()

