"""
Incrementally append Euromillones feature rows using only the previous feature row.

Intended workflow:
1. Run `build_euromillones_features.py` once to backfill all history from 1999.
2. After new draws are scraped into the `euromillones` collection, run THIS script.

This script:
- Reads the last document from `euromillones_draw_features` (call it row N).
- Treats its frequency / gap arrays as the full history state up to draw N.
- For each NEW draw (N+1, N+2, ...):
    - Uses ONLY that state + the new draw's main numbers / stars
      to compute:
        * new frequency arrays
        * new gap arrays
        * new hot/cold lists
        * weekday and previous-draw snapshot
    - Saves a new document in `euromillones_draw_features`.

This means every feature row contains a complete snapshot of per-number
history (frequency, gaps, hot/cold) up to that point, and new rows can be
generated from just the last one, plus the latest draw result.
"""

import os
from datetime import datetime
from itertools import combinations
from typing import Dict, List, Tuple

from pymongo import ASCENDING, MongoClient


MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "lottery")

SOURCE_COLLECTION = "euromillones"
FEATURES_COLLECTION = "euromillones_draw_features"
NUMBER_HISTORY_COLLECTION = "euromillones_number_history"
PAIR_TRIO_HISTORY_COLLECTION = "euromillones_pair_trio_history"

MAIN_MIN, MAIN_MAX = 1, 50
STAR_MIN, STAR_MAX = 1, 12


def _weekday_name(date_str: str) -> str:
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        return d.strftime("%A")
    except Exception:
        return ""


def _split_main_and_stars(numbers: List[int]) -> Tuple[List[int], List[int]]:
    """
    For Euromillones, the raw `numbers` array contains 7 integers in draw order:
      - first 5 = main numbers
      - last 2 = star numbers
    Do not sort; preserve the original order from the source.
    """
    nums = [int(n) for n in numbers if isinstance(n, int)]
    if len(nums) >= 7:
        return nums[:5], nums[5:7]
    if len(nums) >= 5:
        return nums[:5], []
    return nums, []


def _load_last_state(client: MongoClient):
    """
    Load the last feature row and reconstruct per-number history state from it.
    Returns:
        last_doc, draw_index, main_freq_all, star_freq_all,
        main_last_seen_index, star_last_seen_index
    """
    db = client[MONGO_DB]
    feats = db[FEATURES_COLLECTION]

    last_doc = feats.find_one(sort=[("draw_index", -1)])
    if not last_doc:
        raise RuntimeError(
            "No existing feature rows. Run build_euromillones_features.py first."
        )

    draw_index = int(last_doc.get("draw_index", 0))

    main_freq_array: List[int] = last_doc.get("main_frequency_counts") or []
    star_freq_array: List[int] = last_doc.get("star_frequency_counts") or []

    if len(main_freq_array) != (MAIN_MAX - MAIN_MIN + 1):
        raise RuntimeError("main_frequency_counts length mismatch in last feature row")
    if len(star_freq_array) != (STAR_MAX - STAR_MIN + 1):
        raise RuntimeError("star_frequency_counts length mismatch in last feature row")

    # Try to read gap arrays; if missing or invalid, we will reconstruct
    raw_main_gap_array = last_doc.get("main_gap_draws")
    raw_star_gap_array = last_doc.get("star_gap_draws")

    has_valid_main_gaps = isinstance(raw_main_gap_array, list) and len(raw_main_gap_array) == len(
        main_freq_array
    )
    has_valid_star_gaps = isinstance(raw_star_gap_array, list) and len(raw_star_gap_array) == len(
        star_freq_array
    )

    db = client[MONGO_DB]

    # Reconstruct freq_all from arrays
    main_freq_all: Dict[int, int] = {}
    for offset, freq in enumerate(main_freq_array):
        n = MAIN_MIN + offset
        main_freq_all[n] = int(freq)

    star_freq_all: Dict[int, int] = {}
    for offset, freq in enumerate(star_freq_array):
        s = STAR_MIN + offset
        star_freq_all[s] = int(freq)

    # Reconstruct last_seen_index
    main_last_seen_index: Dict[int, int] = {}
    star_last_seen_index: Dict[int, int] = {}

    if has_valid_main_gaps and has_valid_star_gaps:
        # Fast path: derive last_seen_index directly from stored gaps
        main_gap_array: List[int | None] = raw_main_gap_array  # type: ignore[assignment]
        star_gap_array: List[int | None] = raw_star_gap_array  # type: ignore[assignment]

        for offset, gap in enumerate(main_gap_array):
            n = MAIN_MIN + offset
            if gap is None:
                main_last_seen_index[n] = -1
            else:
                main_last_seen_index[n] = draw_index - int(gap)

        for offset, gap in enumerate(star_gap_array):
            s = STAR_MIN + offset
            if gap is None:
                star_last_seen_index[s] = -1
            else:
                star_last_seen_index[s] = draw_index - int(gap)
    else:
        # Compatibility path for legacy rows that don't store gap arrays:
        # reconstruct last_seen_index from euromillones_number_history.
        num_hist = db[NUMBER_HISTORY_COLLECTION]

        # Initialize with -1 (never seen)
        for n in range(MAIN_MIN, MAIN_MAX + 1):
            main_last_seen_index[n] = -1
        for s in range(STAR_MIN, STAR_MAX + 1):
            star_last_seen_index[s] = -1

        # For each number, look at its last appearance draw_index <= current draw_index
        main_docs = num_hist.find({"type": "main"})
        for doc in main_docs:
            number = int(doc.get("number", 0))
            if not (MAIN_MIN <= number <= MAIN_MAX):
                continue
            appearances = doc.get("appearances") or []
            last_idx = -1
            for appo in appearances:
                idx = int(appo.get("draw_index", -1))
                if idx <= draw_index and idx > last_idx:
                    last_idx = idx
            main_last_seen_index[number] = last_idx

        star_docs = num_hist.find({"type": "star"})
        for doc in star_docs:
            number = int(doc.get("number", 0))
            if not (STAR_MIN <= number <= STAR_MAX):
                continue
            appearances = doc.get("appearances") or []
            last_idx = -1
            for appo in appearances:
                idx = int(appo.get("draw_index", -1))
                if idx <= draw_index and idx > last_idx:
                    last_idx = idx
            star_last_seen_index[number] = last_idx

    return (
        last_doc,
        draw_index,
        main_freq_all,
        star_freq_all,
        main_last_seen_index,
        star_last_seen_index,
    )


def _load_pair_trio_last_seen(db):
    """
    Load last seen draw_index for existing main-number pairs/trios
    from euromillones_pair_trio_history.
    """
    coll = db[PAIR_TRIO_HISTORY_COLLECTION]

    pair_last_seen: Dict[Tuple[int, int], int] = {}
    trio_last_seen: Dict[Tuple[int, int, int], int] = {}

    cursor = coll.find(
        {"scope": "main"},
        projection={"type": 1, "combo": 1, "appearances.draw_index": 1},
    )
    for doc in cursor:
        t = doc.get("type")
        combo = doc.get("combo") or []
        appearances = doc.get("appearances") or []
        if not appearances:
            continue
        last_app = appearances[-1]
        last_idx = int(last_app.get("draw_index", -1))
        if last_idx < 0:
            continue
        if t == "pair" and len(combo) == 2:
            key = (int(combo[0]), int(combo[1]))
            pair_last_seen[key] = last_idx
        elif t == "trio" and len(combo) == 3:
            key3 = (int(combo[0]), int(combo[1]), int(combo[2]))
            trio_last_seen[key3] = last_idx

    return pair_last_seen, trio_last_seen


def _update_from_new_draw(
    new_index: int,
    last_feat_doc: dict,
    main_freq_all: Dict[int, int],
    star_freq_all: Dict[int, int],
    main_last_seen_index: Dict[int, int],
    star_last_seen_index: Dict[int, int],
    new_draw: dict,
    db,
):
    """
    Build a new feature document for one new draw, using only:
      - previous feature document (last_feat_doc)
      - per-number state dicts (freq + last_seen)
      - the new raw draw document
    """
    feats = db[FEATURES_COLLECTION]

    draw_id = str(new_draw.get("id_sorteo"))
    fecha_full = (new_draw.get("fecha_sorteo") or "").strip()
    if not draw_id or not fecha_full:
        return
    draw_date = fecha_full.split(" ")[0]
    main_numbers, star_numbers = _split_main_and_stars(new_draw.get("numbers") or [])
    if len(main_numbers) != 5:
        return

    # State BEFORE this draw (counts up to previous index)
    main_freq_array = [main_freq_all[n] for n in range(MAIN_MIN, MAIN_MAX + 1)]
    star_freq_array = [star_freq_all[s] for s in range(STAR_MIN, STAR_MAX + 1)]

    # Gaps BEFORE this draw, derived from last_seen_index
    main_gap_array: List[int | None] = []
    for n in range(MAIN_MIN, MAIN_MAX + 1):
        last = main_last_seen_index[n]
        main_gap_array.append(None if last == -1 else new_index - last)

    star_gap_array: List[int | None] = []
    for s in range(STAR_MIN, STAR_MAX + 1):
        last = star_last_seen_index[s]
        star_gap_array.append(None if last == -1 else new_index - last)

    # Hot / cold lists from current frequencies (before applying this draw)
    if new_index > 0:
        sorted_main_hot = sorted(
            range(MAIN_MIN, MAIN_MAX + 1),
            key=lambda n: main_freq_all[n],
            reverse=True,
        )
        sorted_main_cold = sorted(
            range(MAIN_MIN, MAIN_MAX + 1),
            key=lambda n: main_freq_all[n],
        )
        hot_main_numbers = [
            n for n in sorted_main_hot if main_freq_all[n] > 0
        ][:5]
        cold_main_numbers = [n for n in sorted_main_cold][:5]

        sorted_star_hot = sorted(
            range(STAR_MIN, STAR_MAX + 1),
            key=lambda s: star_freq_all[s],
            reverse=True,
        )
        sorted_star_cold = sorted(
            range(STAR_MIN, STAR_MAX + 1),
            key=lambda s: star_freq_all[s],
        )
        hot_star_numbers = [
            s for s in sorted_star_hot if star_freq_all[s] > 0
        ][:5]
        cold_star_numbers = [s for s in sorted_star_cold][:5]
    else:
        hot_main_numbers = []
        cold_main_numbers = []
        hot_star_numbers = []
        cold_star_numbers = []

    # Previous-draw snapshot from last feature doc
    prev_snapshot = {
        "prev_draw_id": last_feat_doc.get("draw_id"),
        "prev_draw_date": last_feat_doc.get("draw_date"),
        "prev_weekday": last_feat_doc.get("weekday"),
        "prev_main_numbers": last_feat_doc.get("main_numbers") or [],
        "prev_star_numbers": last_feat_doc.get("star_numbers") or [],
    }

    weekday = _weekday_name(draw_date)
    doc = {
        "draw_id": draw_id,
        "draw_date": draw_date,
        "weekday": weekday,
        "draw_index": new_index,
        "main_numbers": main_numbers,
        "star_numbers": star_numbers,
        **prev_snapshot,
        "hot_main_numbers": hot_main_numbers,
        "cold_main_numbers": cold_main_numbers,
        "hot_star_numbers": hot_star_numbers,
        "cold_star_numbers": cold_star_numbers,
        "main_frequency_counts": main_freq_array,
        "star_frequency_counts": star_freq_array,
        "main_gap_draws": main_gap_array,
        "star_gap_draws": star_gap_array,
    }

    feats.update_one({"draw_id": draw_id}, {"$set": doc}, upsert=True)

    # Now update state with this draw for the *next* iteration
    for n in main_numbers:
        if MAIN_MIN <= n <= MAIN_MAX:
            main_freq_all[n] += 1
            main_last_seen_index[n] = new_index

    for s in star_numbers:
        if STAR_MIN <= s <= STAR_MAX:
            star_freq_all[s] += 1
            star_last_seen_index[s] = new_index

    return doc


def main():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    (
        last_feat_doc,
        last_index,
        main_freq_all,
        star_freq_all,
        main_last_seen_index,
        star_last_seen_index,
    ) = _load_last_state(client)

    # Load last seen draw_index for existing pair/trio history
    pair_last_seen, trio_last_seen = _load_pair_trio_last_seen(db)

    last_draw_date = last_feat_doc.get("draw_date")
    if not last_draw_date:
        raise RuntimeError("Last feature row missing draw_date")

    draws_col = db[SOURCE_COLLECTION]

    # Fetch new draws after the last feature row's date, ordered ascending
    new_draws_cursor = draws_col
    new_draws_cursor = new_draws_cursor.find(
        {"fecha_sorteo": {"$gt": f"{last_draw_date} 00:00:00"}},
        projection={"id_sorteo": 1, "fecha_sorteo": 1, "numbers": 1},
    ).sort("fecha_sorteo", ASCENDING)

    new_draws = list(new_draws_cursor)
    if not new_draws:
        print("No new Euromillones draws to append.")
        client.close()
        return

    print(f"Found {len(new_draws)} new draws after {last_draw_date}. Appending...")

    current_feat_doc = last_feat_doc
    current_index = last_index

    num_hist_coll = db[NUMBER_HISTORY_COLLECTION]
    pair_trio_coll = db[PAIR_TRIO_HISTORY_COLLECTION]

    for raw in new_draws:
        current_index += 1

        # Copy last-seen state before updating, so we can compute gaps
        prev_main_last_seen = main_last_seen_index.copy()
        prev_star_last_seen = star_last_seen_index.copy()
        prev_pair_last_seen = pair_last_seen.copy()
        prev_trio_last_seen = trio_last_seen.copy()

        created_doc = _update_from_new_draw(
            new_index=current_index,
            last_feat_doc=current_feat_doc,
            main_freq_all=main_freq_all,
            star_freq_all=star_freq_all,
            main_last_seen_index=main_last_seen_index,
            star_last_seen_index=star_last_seen_index,
            new_draw=raw,
            db=db,
        )
        if created_doc is None:
            continue

        current_feat_doc = created_doc

        draw_id = created_doc.get("draw_id")
        draw_date = created_doc.get("draw_date")
        main_numbers = created_doc.get("main_numbers") or []
        star_numbers = created_doc.get("star_numbers") or []

        # --- Update per-number history incrementally ---
        for n in main_numbers:
            if not (MAIN_MIN <= n <= MAIN_MAX):
                continue
            last = prev_main_last_seen.get(n, -1)
            gap = None if last == -1 else current_index - last
            num_hist_coll.update_one(
                {"type": "main", "number": n},
                {
                    "$set": {"type": "main", "number": n},
                    "$push": {
                        "appearances": {
                            "draw_index": current_index,
                            "draw_id": draw_id,
                            "date": draw_date,
                            "gap_draws_since_prev": gap,
                        }
                    },
                },
                upsert=True,
            )

        for s in star_numbers:
            if not (STAR_MIN <= s <= STAR_MAX):
                continue
            last = prev_star_last_seen.get(s, -1)
            gap = None if last == -1 else current_index - last
            num_hist_coll.update_one(
                {"type": "star", "number": s},
                {
                    "$set": {"type": "star", "number": s},
                    "$push": {
                        "appearances": {
                            "draw_index": current_index,
                            "draw_id": draw_id,
                            "date": draw_date,
                            "gap_draws_since_prev": gap,
                        }
                    },
                },
                upsert=True,
            )

        # --- Update pair/trio history incrementally (main numbers only) ---
        main_nums_sorted = sorted(
            int(n) for n in main_numbers if MAIN_MIN <= int(n) <= MAIN_MAX
        )

        # Pairs
        for a, b in combinations(main_nums_sorted, 2):
            key = (a, b)
            last = prev_pair_last_seen.get(key, -1)
            gap = None if last == -1 else current_index - last
            pair_trio_coll.update_one(
                {"type": "pair", "scope": "main", "combo": [a, b]},
                {
                    "$set": {
                        "type": "pair",
                        "scope": "main",
                        "combo": [a, b],
                    },
                    "$push": {
                        "appearances": {
                            "draw_index": current_index,
                            "draw_id": draw_id,
                            "date": draw_date,
                            "gap_draws_since_prev": gap,
                        }
                    },
                },
                upsert=True,
            )
            pair_last_seen[key] = current_index

        # Trios
        for a, b, c in combinations(main_nums_sorted, 3):
            key3 = (a, b, c)
            last = prev_trio_last_seen.get(key3, -1)
            gap = None if last == -1 else current_index - last
            pair_trio_coll.update_one(
                {"type": "trio", "scope": "main", "combo": [a, b, c]},
                {
                    "$set": {
                        "type": "trio",
                        "scope": "main",
                        "combo": [a, b, c],
                    },
                    "$push": {
                        "appearances": {
                            "draw_index": current_index,
                            "draw_id": draw_id,
                            "date": draw_date,
                            "gap_draws_since_prev": gap,
                        }
                    },
                },
                upsert=True,
            )
            trio_last_seen[key3] = current_index

    client.close()
    print("Done appending Euromillones feature rows.")


if __name__ == "__main__":
    main()

