"""
Incrementally append Euromillones rows into `euromillones_feature` using the last feature row as state.

Intended workflow:
1. Run `build_euromillones_feature.py` once to backfill all history from 1999.
2. After new draws are scraped into the `euromillones` collection (via run_daily_scrape),
   run THIS script to append only the new feature rows.

The script:
- Reads the last document from `euromillones_feature` (call its source_index N).
- Reconstructs running frequency and gap state from that row.
- Finds all new draws in `euromillones` with fecha_sorteo > last feature fecha_sorteo.
- For each new draw in chronological order, computes:
    * main_number / star_number
    * main_dx / star_dx
    * frequency and gap arrays for mains and stars
    * pre_id_sorteo and dia_semana
- Upserts new rows into `euromillones_feature`.
"""

import os
import re
from datetime import datetime
from typing import List, Optional, Tuple

from pymongo import ASCENDING, MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "lottery")

SOURCE_COLLECTION = "euromillones"
FEATURE_COLLECTION = "euromillones_feature"

MAIN_MIN, MAIN_MAX = 1, 50
STAR_MIN, STAR_MAX = 1, 12


def _weekday_name(date_str: str) -> str:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%A")
    except Exception:
        return ""


def _parse_main_and_star(doc: dict) -> Tuple[List[int], List[int]]:
    """
    Preferred: `numbers` field in draw order [5 mains + 2 stars].
    Fallback: parse from combinacion(_acta) text (preserve order).
    """
    raw_nums = doc.get("numbers") or []
    if isinstance(raw_nums, list):
        nums = [int(n) for n in raw_nums if isinstance(n, int)]
        if len(nums) >= 7:
            mains = [n for n in nums[:5] if MAIN_MIN <= n <= MAIN_MAX]
            stars = [s for s in nums[5:7] if STAR_MIN <= s <= STAR_MAX]
            return mains[:5], stars[:2]

    text = (doc.get("combinacion_acta") or doc.get("combinacion") or "").strip()
    nums: List[int] = []
    if text:
        for p in re.split(r"[\s\-]+", text):
            p = p.strip()
            if p.isdigit():
                nums.append(int(p))
    mains = [n for n in nums[:5] if MAIN_MIN <= n <= MAIN_MAX]
    stars = [s for s in nums[5:7] if STAR_MIN <= s <= STAR_MAX]
    return mains[:5], stars[:2]


def _load_last_state(client: MongoClient):
    """
    Load last feature row from euromillones_feature and reconstruct
    running frequency and gap state.
    """
    db = client[MONGO_DB]
    feats = db[FEATURE_COLLECTION]

    last_doc = feats.find_one(sort=[("source_index", -1)])
    if not last_doc:
        raise RuntimeError(
            "No documents in euromillones_feature. Run build_euromillones_feature.py first."
        )

    last_index = int(last_doc.get("source_index", 0))
    last_fecha = (last_doc.get("fecha_sorteo") or "").strip()

    freq = list(last_doc.get("frequency") or [])
    if len(freq) < 50 + 12:
        raise RuntimeError("Last feature row has invalid frequency length.")
    main_freq = [int(x or 0) for x in freq[:50]]
    star_freq = [int(x or 0) for x in freq[50:62]]

    gap_arr = list(last_doc.get("gap") or [])
    if len(gap_arr) < 50 + 12:
        main_gap = [None] * 50
        star_gap = [None] * 12
    else:
        main_gap = gap_arr[:50]
        star_gap = gap_arr[50:62]

    main_last_seen = [-1] * 50
    star_last_seen = [-1] * 12

    for i in range(50):
        g = main_gap[i]
        if isinstance(g, (int, float)):
            main_last_seen[i] = last_index - int(g)

    for i in range(12):
        g = star_gap[i]
        if isinstance(g, (int, float)):
            star_last_seen[i] = last_index - int(g)

    last_id = str(last_doc.get("id_sorteo") or "").strip()

    return (
        last_index,
        last_fecha,
        last_id,
        main_freq,
        star_freq,
        main_last_seen,
        star_last_seen,
    )


def _load_new_draws(client: MongoClient, last_fecha: str):
    """
    Load new Euromillones draws with fecha_sorteo > last_fecha.
    """
    db = client[MONGO_DB]
    src = db[SOURCE_COLLECTION]

    if not last_fecha:
        cursor = src.find({}, projection={"id_sorteo": 1, "fecha_sorteo": 1, "numbers": 1, "combinacion": 1, "combinacion_acta": 1}).sort("fecha_sorteo", ASCENDING)
    else:
        cursor = src.find(
            {"fecha_sorteo": {"$gt": last_fecha}},
            projection={
                "id_sorteo": 1,
                "fecha_sorteo": 1,
                "numbers": 1,
                "combinacion": 1,
                "combinacion_acta": 1,
            },
        ).sort("fecha_sorteo", ASCENDING)

    return list(cursor)


def main() -> None:
    client = MongoClient(MONGO_URI)
    try:
        (
            last_index,
            last_fecha,
            last_id,
            main_freq,
            star_freq,
            main_last_seen,
            star_last_seen,
        ) = _load_last_state(client)

        draws = _load_new_draws(client, last_fecha)
        if not draws:
            print("No new Euromillones draws to process.")
            return

        db = client[MONGO_DB]
        feats = db[FEATURE_COLLECTION]

        print(
            f"Appending Euromillones features from fecha_sorteo > {last_fecha} "
            f"(starting at source_index={last_index + 1})…"
        )

        prev_id = last_id
        processed = 0

        for doc in draws:
            draw_id = str(doc.get("id_sorteo") or "").strip()
            fecha_full = str(doc.get("fecha_sorteo") or "").strip()
            if not draw_id or not fecha_full:
                continue
            fecha = fecha_full.split(" ")[0]

            mains, stars = _parse_main_and_star(doc)
            if len(mains) != 5 or len(stars) != 2:
                continue

            last_index += 1

            main_dx = [0] * 50
            for n in mains:
                main_dx[n - MAIN_MIN] = 1

            star_dx = [0] * 12
            for s in stars:
                star_dx[s - STAR_MIN] = 1

            # frequency including current draw
            freq_main_current = main_freq[:]
            for n in mains:
                freq_main_current[n - MAIN_MIN] += 1

            freq_star_current = star_freq[:]
            for s in stars:
                freq_star_current[s - STAR_MIN] += 1

            frequency = freq_main_current + freq_star_current

            # gaps
            main_gap: List[Optional[int]] = []
            for i in range(50):
                last = main_last_seen[i]
                main_gap.append(None if last == -1 else last_index - last)

            star_gap: List[Optional[int]] = []
            for i in range(12):
                last = star_last_seen[i]
                star_gap.append(None if last == -1 else last_index - last)

            gap = main_gap + star_gap

            out = {
                "id_sorteo": draw_id,
                "pre_id_sorteo": prev_id,
                "fecha_sorteo": fecha,
                "dia_semana": _weekday_name(fecha),
                "main_number": mains,
                "star_number": stars,
                "main_dx": main_dx,
                "star_dx": star_dx,
                "frequency": frequency,
                "gap": gap,
                "source_index": last_index,
            }

            feats.replace_one({"id_sorteo": draw_id}, out, upsert=True)
            processed += 1
            prev_id = draw_id

            # update running state
            for n in mains:
                i = n - MAIN_MIN
                main_freq[i] += 1
                main_last_seen[i] = last_index

            for s in stars:
                j = s - STAR_MIN
                star_freq[j] += 1
                star_last_seen[j] = last_index

        print(f"Done. Appended {processed} new Euromillones feature rows.")
    finally:
        client.close()


if __name__ == "__main__":
    main()

