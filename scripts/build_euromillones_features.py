"""
Build Euromillones feature/history datasets from existing MongoDB history.

Datasets:
- Per-draw feature model (`euromillones_draw_features`):
    - id: lottery object id (`id_sorteo`)
    - main numbers (5 numbers per draw)
    - lucky stars (2 numbers per draw, if present)
    - draw date and weekday name
    - previous draw snapshot (id, date, weekday, numbers)
    - hot / cold mains and stars (based on all previous draws only)
- Per-number history (`euromillones_number_history`):
    - For each number / star, list of appearances with gaps.
- Per-combination history (`euromillones_pair_trio_history`):
    - For each main-number pair/trio, list of appearances with gaps.

All features for a given draw are computed **only from earlier draws** so there
is no look-ahead: the first draw has no history, the second uses the first,
the third uses the first two, and so on.
"""

import os
import re
from dataclasses import dataclass
from datetime import datetime
from itertools import combinations
from typing import Dict, List, Tuple

from pymongo import ASCENDING, MongoClient


MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "lottery")

# Source collection: normalized Euromillones draws from scraper/backfill
SOURCE_COLLECTION = "euromillones"

# Target collection: one feature document per draw
TARGET_COLLECTION = "euromillones_draw_features"

# Euromillones ranges
MAIN_MIN, MAIN_MAX = 1, 50
STAR_MIN, STAR_MAX = 1, 12


@dataclass
class Draw:
    draw_id: str
    fecha_sorteo: str  # "YYYY-MM-DD" (normalized)
    main_numbers: List[int]
    star_numbers: List[int]


def _parse_main_and_stars_from_doc(doc: dict) -> Tuple[List[int], List[int]]:
    """
    Parse Euromillones main numbers (1–50) and lucky stars (1–12) from a raw
    MongoDB document.

    Preference order:
      1. Use `combinacion_acta` if present, which for Euromillones is
         \"n1-n2-n3-n4-n5-n6-n7\" where the first 5 are mains and last 2 are stars.
      2. Fallback to `combinacion` if needed.
      3. As a last resort, use the normalized `numbers` field for mains only.
    """
    text = (doc.get("combinacion_acta") or doc.get("combinacion") or "").strip()
    main_numbers: List[int] = []
    star_numbers: List[int] = []

    if isinstance(text, str) and text:
        # Extract integer tokens from the string (split on hyphens or whitespace)
        parts = re.split(r"[\s\-]+", text)
        nums = []
        for p in parts:
            p = p.strip()
            if p.isdigit():
                nums.append(int(p))

        if len(nums) >= 7:
            main_numbers = nums[:5]
            star_numbers = nums[5:7]
        elif len(nums) >= 5:
            main_numbers = nums[:5]

    if not main_numbers:
        # Fallback: use normalized `numbers` field as mains
        raw_nums = doc.get("numbers") or []
        main_numbers = [int(n) for n in raw_nums if isinstance(n, int)]

    # Clamp into allowed ranges and trim extras
    main_numbers = [n for n in main_numbers if MAIN_MIN <= n <= MAIN_MAX][:5]
    star_numbers = [s for s in star_numbers if STAR_MIN <= s <= STAR_MAX][:2]

    return main_numbers, star_numbers


def _load_draws(client: MongoClient) -> List[Draw]:
    """
    Load all Euromillones draws sorted by fecha_sorteo ascending (oldest first).
    Assumes documents have fields:
      - id_sorteo
      - fecha_sorteo (string "YYYY-MM-DD ..." )
      - combinacion_acta / combinacion / numbers
    """
    db = client[MONGO_DB]
    col = db[SOURCE_COLLECTION]

    cursor = col.find(
        {},
        projection={
            "id_sorteo": 1,
            "fecha_sorteo": 1,
            "numbers": 1,
            "combinacion": 1,
            "combinacion_acta": 1,
        },
    ).sort("fecha_sorteo", ASCENDING)

    draws: List[Draw] = []
    for doc in cursor:
        draw_id = str(doc.get("id_sorteo"))
        fecha_full = (doc.get("fecha_sorteo") or "").strip()
        if not draw_id or not fecha_full:
            continue
        fecha = fecha_full.split(" ")[0]  # keep YYYY-MM-DD
        main_numbers, star_numbers = _parse_main_and_stars_from_doc(doc)
        if len(main_numbers) != 5:
            # Skip malformed rows; Euromillones should have exactly 5 main numbers
            continue
        draws.append(
            Draw(
                draw_id=draw_id,
                fecha_sorteo=fecha,
                main_numbers=main_numbers,
                star_numbers=star_numbers,
            )
        )

    return draws


def _weekday_name(date_str: str) -> str:
    """Return weekday name (e.g. 'Monday') for YYYY-MM-DD string."""
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        return d.strftime("%A")
    except Exception:
        return ""


def _build_features(draws: List[Draw]) -> None:
    """
    Build per-draw feature documents and save to TARGET_COLLECTION.

    Features for draw index i are computed using draws[0 .. i-1] only.
    """
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    target = db[TARGET_COLLECTION]

    # Unique index so the script is idempotent
    target.create_index([("draw_id", ASCENDING)], unique=True)

    # Lifetime frequency for each main number (used for hot/cold and freq arrays)
    main_freq_all: Dict[int, int] = {n: 0 for n in range(MAIN_MIN, MAIN_MAX + 1)}
    # Same for lucky stars
    star_freq_all: Dict[int, int] = {s: 0 for s in range(STAR_MIN, STAR_MAX + 1)}

    # Last draw index where each number/star appeared (for gap arrays)
    main_last_seen: Dict[int, int] = {n: -1 for n in range(MAIN_MIN, MAIN_MAX + 1)}
    star_last_seen: Dict[int, int] = {s: -1 for s in range(STAR_MIN, STAR_MAX + 1)}

    total_draws = len(draws)
    print(f"Building per-draw features from {total_draws} Euromillones draws...")

    for idx, draw in enumerate(draws):
        # Previous draw snapshot (id, numbers, date, weekday)
        if idx > 0:
            prev = draws[idx - 1]
            prev_weekday = _weekday_name(prev.fecha_sorteo)
            prev_snapshot = {
                "prev_draw_id": prev.draw_id,
                "prev_draw_date": prev.fecha_sorteo,
                "prev_weekday": prev_weekday,
                "prev_main_numbers": prev.main_numbers,
                "prev_star_numbers": prev.star_numbers,
            }
        else:
            prev_snapshot = {
                "prev_draw_id": None,
                "prev_draw_date": None,
                "prev_weekday": None,
                "prev_main_numbers": [],
                "prev_star_numbers": [],
            }

        # Frequency arrays for all numbers, based only on previous draws
        main_freq_array = [main_freq_all[n] for n in range(MAIN_MIN, MAIN_MAX + 1)]
        star_freq_array = [star_freq_all[s] for s in range(STAR_MIN, STAR_MAX + 1)]

        # Gap arrays: draws since previous appearance, based only on previous draws
        main_gap_array: List[int | None] = []
        for n in range(MAIN_MIN, MAIN_MAX + 1):
            last = main_last_seen[n]
            main_gap_array.append(None if last == -1 else idx - last)

        star_gap_array: List[int | None] = []
        for s in range(STAR_MIN, STAR_MAX + 1):
            last = star_last_seen[s]
            star_gap_array.append(None if last == -1 else idx - last)

        # Hot / cold numbers (up to 5), based only on previous draws
        if idx > 0:
            # Sort by frequency desc for hot, asc for cold
            sorted_main = sorted(
                range(MAIN_MIN, MAIN_MAX + 1),
                key=lambda n: main_freq_all[n],
                reverse=True,
            )
            sorted_main_cold = sorted(
                range(MAIN_MIN, MAIN_MAX + 1),
                key=lambda n: main_freq_all[n],
            )

            hot_main_numbers = [n for n in sorted_main if main_freq_all[n] > 0][:5]
            cold_main_numbers = [n for n in sorted_main_cold][:5]

            sorted_stars = sorted(
                range(STAR_MIN, STAR_MAX + 1),
                key=lambda s: star_freq_all[s],
                reverse=True,
            )
            sorted_stars_cold = sorted(
                range(STAR_MIN, STAR_MAX + 1),
                key=lambda s: star_freq_all[s],
            )

            hot_star_numbers = [s for s in sorted_stars if star_freq_all[s] > 0][:5]
            cold_star_numbers = [s for s in sorted_stars_cold][:5]
        else:
            hot_main_numbers: List[int] = []
            cold_main_numbers: List[int] = []
            hot_star_numbers: List[int] = []
            cold_star_numbers: List[int] = []

        # Build feature document for this draw
        weekday = _weekday_name(draw.fecha_sorteo)
        doc = {
            "draw_id": draw.draw_id,
            "draw_date": draw.fecha_sorteo,
            "weekday": weekday,
            "draw_index": idx,
            "main_numbers": draw.main_numbers,
            "star_numbers": draw.star_numbers,
            # Previous draw basic info (for sequence-style features)
            **prev_snapshot,
            # Hot / cold numbers (up to 5)
            "hot_main_numbers": hot_main_numbers,
            "cold_main_numbers": cold_main_numbers,
            "hot_star_numbers": hot_star_numbers,
            "cold_star_numbers": cold_star_numbers,
            # Frequency counts (every 50 main numbers and lucky stars, BEFORE this draw)
            "main_frequency_counts": main_freq_array,
            "star_frequency_counts": star_freq_array,
            # Gaps in draws since previous appearance, BEFORE this draw
            "main_gap_draws": main_gap_array,
            "star_gap_draws": star_gap_array,
        }

        target.update_one(
            {"draw_id": draw.draw_id},
            {"$set": doc},
            upsert=True,
        )

        # After saving the feature doc, update lifetime frequencies and last_seen with this draw
        for n in draw.main_numbers:
            if MAIN_MIN <= n <= MAIN_MAX:
                main_freq_all[n] += 1
                main_last_seen[n] = idx

        for s in draw.star_numbers:
            if STAR_MIN <= s <= STAR_MAX:
                star_freq_all[s] += 1
                star_last_seen[s] = idx

        if (idx + 1) % 50 == 0 or idx == total_draws - 1:
            print(f"  Processed {idx + 1} / {total_draws} draws")

    client.close()
    print("Done. Per-draw features are in collection:", TARGET_COLLECTION)


def _build_number_history(draws: List[Draw]) -> None:
    """
    Build per-number appearance history for mains and stars.

    Creates/updates collection `euromillones_number_history` with documents:
      { type: 'main'|'star', number: n, appearances: [{draw_index, draw_id, date}] }
    """
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    coll = db["euromillones_number_history"]

    coll.create_index([("type", ASCENDING), ("number", ASCENDING)], unique=True)

    main_history: Dict[int, List[dict]] = {n: [] for n in range(MAIN_MIN, MAIN_MAX + 1)}
    star_history: Dict[int, List[dict]] = {s: [] for s in range(STAR_MIN, STAR_MAX + 1)}

    # Track last draw_index for each number so we can compute gap between appearances
    main_last_seen: Dict[int, int] = {n: -1 for n in range(MAIN_MIN, MAIN_MAX + 1)}
    star_last_seen: Dict[int, int] = {s: -1 for s in range(STAR_MIN, STAR_MAX + 1)}

    for idx, draw in enumerate(draws):
        for n in draw.main_numbers:
            if MAIN_MIN <= n <= MAIN_MAX:
                last = main_last_seen[n]
                gap = None if last == -1 else idx - last
                main_history[n].append(
                    {
                        "draw_index": idx,
                        "draw_id": draw.draw_id,
                        "date": draw.fecha_sorteo,
                        "gap_draws_since_prev": gap,
                    }
                )
                main_last_seen[n] = idx
        for s in draw.star_numbers:
            if STAR_MIN <= s <= STAR_MAX:
                last = star_last_seen[s]
                gap = None if last == -1 else idx - last
                star_history[s].append(
                    {
                        "draw_index": idx,
                        "draw_id": draw.draw_id,
                        "date": draw.fecha_sorteo,
                        "gap_draws_since_prev": gap,
                    }
                )
                star_last_seen[s] = idx

    # Upsert one document per number/type
    for n in range(MAIN_MIN, MAIN_MAX + 1):
        coll.update_one(
            {"type": "main", "number": n},
            {
                "$set": {
                    "type": "main",
                    "number": n,
                    "appearances": main_history.get(n, []),
                }
            },
            upsert=True,
        )

    for s in range(STAR_MIN, STAR_MAX + 1):
        coll.update_one(
            {"type": "star", "number": s},
            {
                "$set": {
                    "type": "star",
                    "number": s,
                    "appearances": star_history.get(s, []),
                }
            },
            upsert=True,
        )

    client.close()
    print("Done. Per-number history is in collection: euromillones_number_history")


def _build_pair_trio_history(draws: List[Draw]) -> None:
    """
    Build per-combination appearance history for pairs/trios of main numbers.

    Creates/updates collection `euromillones_pair_trio_history` with documents:
      type: 'pair' or 'trio'
      scope: 'main'
      combo: [n1, n2] or [n1, n2, n3]
      appearances: [{ draw_index, draw_id, date, gap_draws_since_prev }, ...]
    """
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    # Rebuild collection from scratch to avoid legacy/invalid documents
    db.drop_collection("euromillones_pair_trio_history")
    coll = db["euromillones_pair_trio_history"]

    # Non-unique index: we enforce one-document-per-combo via our own upserts.
    # Using a unique index on an array field (`combo`) would conflict because
    # MongoDB indexes each array element separately.
    coll.create_index([("type", ASCENDING), ("scope", ASCENDING), ("combo", ASCENDING)])

    # Histories per combination (main numbers only)
    main_pair_history: Dict[Tuple[int, int], List[dict]] = {}
    main_trio_history: Dict[Tuple[int, int, int], List[dict]] = {}

    # Last seen draw index for gap calculation
    main_pair_last_seen: Dict[Tuple[int, int], int] = {}
    main_trio_last_seen: Dict[Tuple[int, int, int], int] = {}

    for idx, draw in enumerate(draws):
        # Main number pairs (5 choose 2)
        for a, b in combinations(draw.main_numbers, 2):
            if not (MAIN_MIN <= a <= MAIN_MAX and MAIN_MIN <= b <= MAIN_MAX):
                continue
            key = tuple(sorted((a, b)))
            last = main_pair_last_seen.get(key, -1)
            gap = None if last == -1 else idx - last
            main_pair_history.setdefault(key, []).append(
                {
                    "draw_index": idx,
                    "draw_id": draw.draw_id,
                    "date": draw.fecha_sorteo,
                    "gap_draws_since_prev": gap,
                }
            )
            main_pair_last_seen[key] = idx

        # Main number trios (5 choose 3)
        for a, b, c in combinations(draw.main_numbers, 3):
            if not (
                MAIN_MIN <= a <= MAIN_MAX
                and MAIN_MIN <= b <= MAIN_MAX
                and MAIN_MIN <= c <= MAIN_MAX
            ):
                continue
            key3 = tuple(sorted((a, b, c)))
            last = main_trio_last_seen.get(key3, -1)
            gap = None if last == -1 else idx - last
            main_trio_history.setdefault(key3, []).append(
                {
                    "draw_index": idx,
                    "draw_id": draw.draw_id,
                    "date": draw.fecha_sorteo,
                    "gap_draws_since_prev": gap,
                }
            )
            main_trio_last_seen[key3] = idx

    # Upsert all main pairs
    for key, appearances in main_pair_history.items():
        coll.update_one(
            {"type": "pair", "scope": "main", "combo": list(key)},
            {
                "$set": {
                    "type": "pair",
                    "scope": "main",
                    "combo": list(key),
                    "appearances": appearances,
                }
            },
            upsert=True,
        )

    # Upsert all main trios
    for key3, appearances in main_trio_history.items():
        coll.update_one(
            {"type": "trio", "scope": "main", "combo": list(key3)},
            {
                "$set": {
                    "type": "trio",
                    "scope": "main",
                    "combo": list(key3),
                    "appearances": appearances,
                }
            },
            upsert=True,
        )

    client.close()
    print("Done. Pair/trio history is in collection: euromillones_pair_trio_history")


if __name__ == "__main__":
    mongo_client = MongoClient(MONGO_URI)
    try:
        all_draws = _load_draws(mongo_client)
    finally:
        mongo_client.close()

    # Build per-draw features (with hot/cold + previous draw snapshot)
    _build_features(all_draws)

    # Build full per-number history from all draws
    _build_number_history(all_draws)

    # Build pair/trio history from all draws (main-number combinations only)
    _build_pair_trio_history(all_draws)

