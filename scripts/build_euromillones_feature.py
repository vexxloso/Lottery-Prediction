"""
Build Euromillones feature rows into `euromillones_feature`.

Default behavior (test mode):
- Read first 5 draws from `euromillones` sorted by `fecha_sorteo` ascending.
- Compute feature fields with no look-ahead leakage.
- Upsert into `euromillones_feature` keyed by `id_sorteo`.
"""
import argparse
import os
import re
from datetime import datetime
from typing import List, Tuple

from pymongo import ASCENDING, MongoClient


MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "lottery")

SOURCE_COLLECTION = "euromillones"
TARGET_COLLECTION = "euromillones_feature"

MAIN_MIN, MAIN_MAX = 1, 50
STAR_MIN, STAR_MAX = 1, 12


def _weekday_name(date_str: str) -> str:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%A")
    except Exception:
        return ""


def _parse_main_and_star(doc: dict) -> Tuple[List[int], List[int]]:
    # Preferred path: `numbers` in draw order [5 mains + 2 stars]
    raw_nums = doc.get("numbers") or []
    if isinstance(raw_nums, list):
        nums = [int(n) for n in raw_nums if isinstance(n, int)]
        if len(nums) >= 7:
            mains = [n for n in nums[:5] if MAIN_MIN <= n <= MAIN_MAX]
            stars = [s for s in nums[5:7] if STAR_MIN <= s <= STAR_MAX]
            return mains[:5], stars[:2]

    # Fallback parse from combination text (preserve order, no sort)
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


def build(limit: int | None = None) -> None:
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    src = db[SOURCE_COLLECTION]
    dst = db[TARGET_COLLECTION]

    dst.create_index([("id_sorteo", ASCENDING)], unique=True)

    cursor = src.find(
        {},
        projection={
            "id_sorteo": 1,
            "fecha_sorteo": 1,
            "numbers": 1,
            "combinacion": 1,
            "combinacion_acta": 1,
        },
    ).sort("fecha_sorteo", ASCENDING)

    if limit is not None and limit > 0:
        cursor = cursor.limit(limit)

    draws = list(cursor)
    if not draws:
        print("No draws found in source collection.")
        client.close()
        return

    main_freq = [0] * 50
    star_freq = [0] * 12
    main_last_seen = [-1] * 50
    star_last_seen = [-1] * 12

    processed = 0
    added = 0

    for idx, doc in enumerate(draws):
        draw_id = str(doc.get("id_sorteo") or "").strip()
        fecha_full = str(doc.get("fecha_sorteo") or "").strip()
        if not draw_id or not fecha_full:
            continue
        fecha = fecha_full.split(" ")[0]
        mains, stars = _parse_main_and_star(doc)
        if len(mains) != 5 or len(stars) != 2:
            continue

        pre_id = None if idx == 0 else str(draws[idx - 1].get("id_sorteo") or "").strip() or None

        main_dx = [0] * 50
        for n in mains:
            main_dx[n - 1] = 1

        star_dx = [0] * 12
        for s in stars:
            star_dx[s - 1] = 1

        # frequency should include the current draw numbers as well
        freq_main_current = main_freq[:]
        for n in mains:
            freq_main_current[n - 1] += 1
        freq_star_current = star_freq[:]
        for s in stars:
            freq_star_current[s - 1] += 1
        frequency = freq_main_current + freq_star_current

        main_gap: List[int | None] = []
        for i in range(50):
            last = main_last_seen[i]
            main_gap.append(None if last == -1 else idx - last)

        star_gap: List[int | None] = []
        for i in range(12):
            last = star_last_seen[i]
            star_gap.append(None if last == -1 else idx - last)

        gap = main_gap + star_gap
        out = {
            "id_sorteo": draw_id,
            "pre_id_sorteo": pre_id,
            "fecha_sorteo": fecha,
            "dia_semana": _weekday_name(fecha),
            "main_number": mains,
            "star_number": stars,
            "main_dx": main_dx,
            "star_dx": star_dx,
            "frequency": frequency,
            "gap": gap,
            "source_index": idx,
        }

        result = dst.replace_one({"id_sorteo": draw_id}, out, upsert=True)
        processed += 1
        if result.upserted_id is not None:
            added += 1

        for n in mains:
            i = n - 1
            main_freq[i] += 1
            main_last_seen[i] = idx

        for s in stars:
            i = s - 1
            star_freq[i] += 1
            star_last_seen[i] = idx

    print(f"Done. Found={len(draws)}, processed={processed}, added_new={added}")
    client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="How many sorted draws to process (0 = no limit)",
    )
    args = parser.parse_args()
    limit_arg = args.limit if args.limit and args.limit > 0 else None
    build(limit=limit_arg)
