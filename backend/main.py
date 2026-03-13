"""
Lottery Prediction API — scrape lottery draws and save to MongoDB.
Uses Selenium with a new Chrome instance (pattern from refer.py).
Three collections: la_primitiva, euromillones, el_gordo.
Stores combinacion (main), parsed numbers/C/R, and joker combinacion.
"""
import json
import logging
import os
import random
import re
import secrets
import sys
import tempfile
import threading
import time
from contextlib import asynccontextmanager
from statistics import mean
from typing import Optional, List, Dict, Iterable, Sequence, Tuple
from itertools import combinations

from dotenv import load_dotenv

load_dotenv()

from datetime import datetime as dt, timedelta

from bson import ObjectId
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# Ensure we can import helper scripts from the project-level `scripts` and `refer` folders.
_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SCRIPTS_DIR = os.path.join(_ROOT_DIR, "scripts")
if _SCRIPTS_DIR not in sys.path:
    sys.path.append(_SCRIPTS_DIR)
if _ROOT_DIR not in sys.path:
    sys.path.insert(0, _ROOT_DIR)


from train_euromillones_model import (
    prepare_euromillones_dataset,
    train_euromillones_models,
    compute_euromillones_probabilities,
)
from train_el_gordo_model import (
    prepare_el_gordo_dataset,
    train_el_gordo_models,
    compute_el_gordo_probabilities,
)

def _append_el_gordo_bought_tickets(
    tickets: list,
    draw_date: str | None = None,
    cutoff_draw_id: str | None = None,
) -> None:
    """Append tickets to bought_tickets in el_gordo_train_progress (merge, dedupe by mains+clave)."""
    if db is None or not tickets:
        return
    draw_date = (draw_date or "").strip()[:10] or None
    cutoff = (cutoff_draw_id or "").strip() or None
    coll = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
    doc = None
    if draw_date:
        doc = coll.find_one({"probs_fecha_sorteo": draw_date}, projection={"bought_tickets": 1})
    elif cutoff:
        doc = coll.find_one({"cutoff_draw_id": cutoff}, projection={"bought_tickets": 1, "cutoff_draw_id": 1})
        if not doc and cutoff.isdigit():
            doc = coll.find_one({"cutoff_draw_id": int(cutoff)}, projection={"bought_tickets": 1, "cutoff_draw_id": 1})
    if not doc and not draw_date and not cutoff:
        last = _get_last_draw_date("el-gordo")
        if last:
            doc = coll.find_one({"probs_fecha_sorteo": (last or "").strip()[:10]}, projection={"bought_tickets": 1})
    if not doc:
        return
    existing = doc.get("bought_tickets") or []
    seen = {(tuple(t.get("mains") or []), int(t.get("clave", 0))) for t in existing}
    merged = list(existing)
    for t in tickets:
        mains = t.get("mains") or []
        clave = int(t.get("clave", 0))
        key = (tuple(mains), clave)
        if key not in seen:
            seen.add(key)
            merged.append({"mains": list(mains), "clave": clave})
    query = {"_id": doc["_id"]}
    coll.update_one(
        query,
        {"$set": {"bought_tickets": merged, "bought_tickets_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}},
    )
    logger.info("el_gordo: appended %s bot tickets to bought_tickets (total %s)", len(tickets), len(merged))


# Progress state for El Gordo bot (read by GET /api/el-gordo/betting/bot-progress)
# last_tickets/draw_date/cutoff_draw_id: stored after bot run so user can confirm purchase and add to bought_tickets
_el_gordo_bot_progress: dict = {
    "status": "idle",
    "step": "",
    "started_at": None,
    "finished_at": None,
    "error": None,
    "last_tickets": None,
    "last_draw_date": None,
    "last_cutoff_draw_id": None,
}


def _run_el_gordo_real_platform_bot(
    tickets: list,
    draw_date: str | None = None,
    cutoff_draw_id: str | None = None,
) -> None:
    """Run El Gordo real-platform Selenium bot in a thread. Does NOT add to bought_tickets; user confirms via confirm-bot-bought."""
    progress = _el_gordo_bot_progress
    progress["status"] = "running"
    progress["step"] = "Iniciando..."
    progress["started_at"] = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    progress["finished_at"] = None
    progress["error"] = None
    progress["last_tickets"] = None
    progress["last_draw_date"] = None
    progress["last_cutoff_draw_id"] = None
    try:
        from el_gordo_real_platform_bot import run_el_gordo_real_platform_bot as run_bot

        def on_step(step: str) -> None:
            progress["step"] = step

        result = run_bot(tickets, progress_callback=on_step)
        progress["status"] = "success"
        progress["finished_at"] = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        bought = result.get("bought") is True
        if bought:
            _append_el_gordo_bought_tickets(tickets, draw_date=draw_date, cutoff_draw_id=cutoff_draw_id)
            progress["step"] = "Compra realizada y añadida a guardados."
            progress["last_tickets"] = None
            progress["last_draw_date"] = None
            progress["last_cutoff_draw_id"] = None
        else:
            progress["step"] = "Formulario rellenado. Si compraste en Loterías, pulsa «Añadir a guardados»."
            progress["last_tickets"] = tickets
            progress["last_draw_date"] = draw_date
            progress["last_cutoff_draw_id"] = cutoff_draw_id
    except Exception as e:
        logger.exception("el_gordo open-real-platform bot: %s", e)
        progress["status"] = "error"
        progress["step"] = ""
        progress["error"] = str(e)
        progress["finished_at"] = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        progress["last_tickets"] = None
        progress["last_draw_date"] = None
        progress["last_cutoff_draw_id"] = None


from train_la_primitiva_model import (
    prepare_la_primitiva_dataset,
    train_la_primitiva_models,
    compute_la_primitiva_probabilities,
)
from itertools import combinations
from pathlib import Path


def build_step4_pool(
    mains_probs: List[Dict],
    stars_probs: List[Dict],
    prev_main_numbers: Optional[List[int]] = None,
    prev_star_numbers: Optional[List[int]] = None,
    seed: Optional[int] = None,
) -> Dict:
    """
    Step 4 pool: build prioritized subset (≈20 mains + 4 stars), then extend to full 50 mains + 12 stars.

    - 3–5 mains + 1 star: from previous draw (prev_main_numbers, prev_star_numbers) if available,
      else random. No duplicate with the rest.
    - Additional mains: from Step 3 ranking (mains_probs sorted by p), no duplicate with the previous mains:
      from 1st–20th select 6–7, 21st–30th select 3, 31st–40th select 3, 41st–50th select 3.
      The prioritized subset is truncated to 20 mains if longer.
    - Additional stars: from Step 3 star ranking, no duplicate with the 1 above:
      from 1st–6th select 2, 7th–12th select 1.
      The prioritized subset is truncated to 4 stars if longer.

    After building this prioritized subset, we EXTEND it to a full pool:
    - Append all remaining mains (from mains_probs, sorted by probability) that are not yet included,
      so the final mains list covers all numbers in [1, 50] that appear in mains_probs.
    - Append all remaining stars (from stars_probs, sorted by probability) that are not yet included,
      so the final stars list covers all numbers in [1, 12] that appear in stars_probs.

    Returns filtered_mains (up to 50 items) and filtered_stars (up to 12 items), where:
    - The first 20 mains and first 4 stars correspond to the rule-based prioritized subset.
    """
    if seed is not None:
        random.seed(seed)
    MAIN_MIN, MAIN_MAX = 1, 50
    STAR_MIN, STAR_MAX = 1, 12
    mains_ranking = sorted(mains_probs, key=lambda x: (x.get("p") or 0), reverse=True)
    stars_ranking = sorted(stars_probs, key=lambda x: (x.get("p") or 0), reverse=True)
    rules_used: List[str] = []
    excluded = {"mains": [], "stars": []}

    # --- 4–5 mains + 1 star from previous draw or random ---
    # Mains: choose a random count in {4, 5} from previous draw mains (or random fallback).
    if prev_main_numbers and len(prev_main_numbers) >= 4 and prev_star_numbers and len(prev_star_numbers) >= 1:
        pick_mains = list(prev_main_numbers)[:5] 
        pick_stars = list(prev_star_numbers)[:2]
        prev_count = 5 if len(pick_mains) >= 5 else len(pick_mains)
        # Randomly choose 4 or 5, but not more than we actually have.
        target_prev = random.choice([4, 5])
        target_prev = min(target_prev, prev_count)
        base_mains = random.sample(pick_mains, target_prev)
        one_star = random.sample(pick_stars, 1)[0]
        rules_used.append(f"from_previous_draw_{target_prev}m_1s")
    else:
        target_prev = random.choice([4, 5])
        base_mains = random.sample(range(MAIN_MIN, MAIN_MAX + 1), target_prev)
        one_star = random.randint(STAR_MIN, STAR_MAX)
        rules_used.append(f"random_{target_prev}m_1s")

    main_set = set(base_mains)
    star_set = {one_star}

    # --- Mains from ranking bands: random counts within requested ranges (no duplicate with the previous mains) ---
    def take_random_from_ranking(
        ranking: List[Dict],
        start_1based: int,
        end_1based: int,
        count: int,
        exclude: set,
    ) -> List[Dict]:
        segment = [x for x in ranking[start_1based - 1 : end_1based] if int(x.get("number") or 0) not in exclude]
        if len(segment) <= count:
            chosen = list(segment)
        else:
            chosen = random.sample(segment, count)
        for item in chosen:
            exclude.add(int(item.get("number") or 0))
        return chosen

    # User-requested distribution:
    # - From pre-draw: 4–5 mains (handled above as base_mains)
    # - From ranks 1–20: select between 6 and 7 numbers (randomly)
    # - From ranks 21–30: select 3
    # - From ranks 31–40: select 3
    # - From ranks 41–50: select 3 
    top_count = random.choice([6, 7])
    from_1_20 = take_random_from_ranking(mains_ranking, 1, 20, top_count, main_set)
    from_21_30 = take_random_from_ranking(mains_ranking, 21, 30, 3, main_set)
    from_31_40 = take_random_from_ranking(mains_ranking, 31, 40, 3, main_set)
    from_41_50 = take_random_from_ranking(mains_ranking, 41, 50, 3, main_set)
    rules_used.append(f"mains_ranking_bands_prev_{len(base_mains)}_top_{top_count}_3_3_3")

    filtered_mains_items: List[Dict] = []
    for n in base_mains:
        p = next((x.get("p") for x in mains_probs if int(x.get("number") or 0) == n), 0.0)
        filtered_mains_items.append({"number": n, "p": p})
    for seg in (from_1_20, from_21_30, from_31_40, from_41_50):
        filtered_mains_items.extend(seg)
    filtered_mains = filtered_mains_items[:20]

    # --- 3 stars from ranking bands: random within each band (no duplicate with the 1) ---
    from_star_1_6 = take_random_from_ranking(stars_ranking, 1, 6, 2, star_set)
    from_star_7_12 = take_random_from_ranking(stars_ranking, 7, 12, 1, star_set)
    rules_used.append("stars_ranking_bands_2_1")

    filtered_stars_items: List[Dict] = []
    p_one = next((x.get("p") for x in stars_probs if int(x.get("number") or 0) == one_star), 0.0)
    filtered_stars_items.append({"number": one_star, "p": p_one})
    filtered_stars_items.extend(from_star_1_6)
    filtered_stars_items.extend(from_star_7_12)
    filtered_stars = filtered_stars_items[:4]

    # Randomly reorder prioritized subset so the top of the pool is shuffled.
    random.shuffle(filtered_mains)
    random.shuffle(filtered_stars)

    # --- Extend to full 50 mains and 12 stars using probability ranking ---
    selected_main_nums = {int(r.get("number") or 0) for r in filtered_mains}
    for item in mains_ranking:
        n = int(item.get("number") or 0)
        if n < MAIN_MIN or n > MAIN_MAX:
            continue
        if n in selected_main_nums:
            continue
        filtered_mains.append({"number": n, "p": item.get("p") or 0.0})
        selected_main_nums.add(n)

    selected_star_nums = {int(r.get("number") or 0) for r in filtered_stars}
    for item in stars_ranking:
        n = int(item.get("number") or 0)
        if n < STAR_MIN or n > STAR_MAX:
            continue
        if n in selected_star_nums:
            continue
        filtered_stars.append({"number": n, "p": item.get("p") or 0.0})
        selected_star_nums.add(n)

    def _stats(rows: List[Dict]) -> Dict:
        nums = [int(r.get("number") or 0) for r in rows]
        return {
            "count": len(nums),
            "sum": sum(nums),
            "even": sum(1 for n in nums if n % 2 == 0),
            "odd": sum(1 for n in nums if n % 2 != 0),
        }

    stats = {"mains": _stats(filtered_mains), "stars": _stats(filtered_stars)}
    return {
        "filtered_mains": filtered_mains,
        "filtered_stars": filtered_stars,
        "rules_used": rules_used,
        "excluded": excluded,
        "stats": stats,
        "snapshot_mains": base_mains,
        "snapshot_stars": [one_star],
    }


def _iter_euromillones_tickets_from_pool(
    mains_pool: Iterable[int],
    stars_pool: Iterable[int],
) -> Iterable[tuple[List[int], List[int]]]:
    """
    Iterate all Euromillones tickets for the given ordered pools, with
    mixed traversal over mains and stars to avoid long runs of identical
    mains or identical stars.

    - mains_pool: ordered mains numbers (length 50 expected)
    - stars_pool: ordered star numbers (length 12 expected)

    We precompute:
    - all mains combinations indices
    - all star pairs indices

    Then we traverse the (mains_combo_index, star_pair_index) grid in a
    "diagonal" pattern:

        i = k % nm
        t = k // nm
        j = (t + i) % ns

    where:
        nm = number of mains combinations
        ns = number of star pairs

    This visits every (i, j) exactly once but interleaves mains and stars
    more richly than the naive nested loops.
    """
    mains_list = list(dict.fromkeys(int(x) for x in mains_pool))
    stars_list = list(dict.fromkeys(int(x) for x in stars_pool))
    if len(mains_list) < 5:
        raise ValueError(f"Need at least 5 mains, got {len(mains_list)}")
    if len(stars_list) < 2:
        raise ValueError(f"Need at least 2 stars, got {len(stars_list)}")

    main_idx_combos = list(combinations(range(len(mains_list)), 5))
    star_idx_pairs = list(combinations(range(len(stars_list)), 2))

    # Deterministically shuffle mains and star pairs based on the pool,
    # so that the overall structure is reproducible but not grouped in
    # lexicographic order (which tends to cluster similar tickets).
    seed = (hash((tuple(mains_list), tuple(stars_list))) & 0xFFFFFFFF)
    rng = random.Random(seed)
    rng.shuffle(main_idx_combos)
    rng.shuffle(star_idx_pairs)
    nm = len(main_idx_combos)
    ns = len(star_idx_pairs)

    total = nm * ns
    for k in range(total):
        i = k % nm
        t = k // nm
        j = (t + i) % ns
        mains = [mains_list[idx] for idx in main_idx_combos[i]]
        stars = [stars_list[idx] for idx in star_idx_pairs[j]]
        yield mains, stars


def _is_bad_euromillones_ticket(mains: Sequence[int]) -> bool:
    """Return True if the ticket violates any structural rule (bad ticket)."""
    if len(mains) != 5:
        return False

    # Rule A: long consecutive run (>= 4 consecutive numbers)
    sorted_mains = sorted(mains)
    longest_run = 1
    current_run = 1
    for i in range(1, len(sorted_mains)):
        if sorted_mains[i] == sorted_mains[i - 1] + 1:
            current_run += 1
            longest_run = max(longest_run, current_run)
        else:
            current_run = 1
    if longest_run >= 4:
        return True

    # Rule B: all same last digit or same decade
    last_digits = {n % 10 for n in mains}
    decades = {n // 10 for n in mains}
    if len(last_digits) == 1 or len(decades) == 1:
        return True

    # Rule C: all odd or all even
    all_even = all(n % 2 == 0 for n in mains)
    all_odd = all(n % 2 == 1 for n in mains)
    if all_even or all_odd:
        return True

    return False


def _euromillones_ticket_tier(mains: Sequence[int]) -> int:
    """
    Classify a Euromillones ticket into quality tiers:
    - 0: very good (best patterns)
    - 1: normal
    - 2: weak
    - 3: bad (structural anti-patterns; delegated to _is_bad_euromillones_ticket)
    """
    if _is_bad_euromillones_ticket(mains):
        return 3

    if len(mains) != 5:
        return 2

    nums = list(mains)
    total = sum(nums)
    evens = sum(1 for n in nums if n % 2 == 0)
    odds = 5 - evens
    decades = {n // 10 for n in nums}

    score = 0

    # Even/odd balance: 2–3 or 3–2 is ideal, 1–4 or 4–1 is weaker.
    if evens in (2, 3):
        score += 2
    elif evens in (1, 4):
        score += 1

    # Spread across decades: more distinct decades is better.
    if len(decades) >= 4:
        score += 2
    elif len(decades) == 3:
        score += 1

    # Sum band: favour mid-range totals.
    if 100 <= total <= 180:
        score += 2
    elif 80 <= total <= 200:
        score += 1

    # Short consecutive runs (2–3) are acceptable; long runs already excluded.
    sorted_mains = sorted(nums)
    longest_run = 1
    current_run = 1
    for i in range(1, len(sorted_mains)):
        if sorted_mains[i] == sorted_mains[i - 1] + 1:
            current_run += 1
            longest_run = max(longest_run, current_run)
        else:
            current_run = 1
    if longest_run == 3:
        score += 1

    if score >= 5:
        return 0
    if score >= 3:
        return 1
    return 2


def _generate_full_wheel_file_from_pool(
    mains_pool: Iterable[int],
    stars_pool: Iterable[int],
    draw_date: str,
) -> dict:
    """
    Generate full Euromillones wheel from the given pools and save to TXT.

    - Uses structural rules to classify tickets into quality tiers:
        * Tier 0: very good patterns
        * Tier 1: normal
        * Tier 2: weak
        * Tier 3: bad (consecutive, same modulo/decade, all odd/even)
      Tickets are written in tier order (0 → 1 → 2 → 3) so the worst
      patterns move to the end of the list.
    - Within each tier, tickets are buffered in small batches and shuffled
      with a deterministic seed to avoid long runs of identical mains.
    - Positions are 1-based and contiguous across all tiers.
    """
    root = Path(_ROOT_DIR)
    out_dir = root / "euromillones_pools"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"euromillones_{draw_date}.txt"

    # Positions are global across tiers.
    position = 0
    total_by_tier = [0, 0, 0, 0]

    # Write tiers 0..3 in order.
    with out_path.open("w", encoding="utf-8", buffering=1024 * 1024) as f:
        for tier in range(4):
            # Deterministic shuffling per tier (no cancel support).
            seed = (hash((draw_date, tier)) & 0xFFFFFFFF)
            rng = random.Random(seed)
            buffer: List[tuple[List[int], List[int]]] = []
            buffer_size = 1000

            for mains, stars in _iter_euromillones_tickets_from_pool(mains_pool, stars_pool):
                t = _euromillones_ticket_tier(mains)
                if t != tier:
                    continue
                buffer.append((list(mains), list(stars)))
                if len(buffer) >= buffer_size:
                    rng.shuffle(buffer)
                    for m, s in buffer:
                        position += 1
                        mains_str = ",".join(str(n) for n in m)
                        stars_str = ",".join(str(n) for n in s)
                        f.write(f"{position};{mains_str};{stars_str}\n")
                        total_by_tier[tier] += 1
                    buffer.clear()

            if buffer:
                rng.shuffle(buffer)
                for m, s in buffer:
                    position += 1
                    mains_str = ",".join(str(n) for n in m)
                    stars_str = ",".join(str(n) for n in s)
                    f.write(f"{position};{mains_str};{stars_str}\n")
                    total_by_tier[tier] += 1

    good_count = total_by_tier[0] + total_by_tier[1] + total_by_tier[2]
    bad_count = total_by_tier[3]
    total = position
    return {
        "file_path": str(out_path),
        "draw_date": draw_date,
        "good_tickets": good_count,
        "bad_tickets": bad_count,
        "total_tickets": total,
    }


def _iter_la_primitiva_tickets_from_pool(
    mains_pool: Iterable[int],
) -> Iterable[List[int]]:
    """
    Iterate all La Primitiva tickets (6 mains) for the given ordered pool.

    - mains_pool: ordered mains numbers (expected length 49, already extended
      and prioritized by Step 4).

    We:
    - Deduplicate while preserving order.
    - Build all index combinations of length 6.
    - Deterministically shuffle the combinations using a seed derived from
      the mains list, so the traversal is "manifold" (not lexicographic) but
      reproducible.
    """
    mains_list = list(dict.fromkeys(int(x) for x in mains_pool))
    if len(mains_list) < 6:
        raise ValueError(f"Need at least 6 mains, got {len(mains_list)}")

    main_idx_combos = list(combinations(range(len(mains_list)), 6))
    seed = (hash(tuple(mains_list)) & 0xFFFFFFFF)
    rng = random.Random(seed)
    rng.shuffle(main_idx_combos)

    for idxs in main_idx_combos:
        yield [mains_list[i] for i in idxs]


def _la_primitiva_ticket_tier(mains: Sequence[int]) -> int:
    """
    Classify a La Primitiva ticket into quality tiers (0 best .. 3 worst),
    reusing the same structural ideas as El Gordo/Euromillones.
    """
    return _el_gordo_ticket_tier(mains)


def _generate_la_primitiva_full_wheel_file(
    mains_pool: Iterable[int],
    draw_date: str,
) -> dict:
    """
    Generate full La Primitiva wheel from the given mains pool and save to TXT.

    - Uses structural rules (via _la_primitiva_ticket_tier) to group tickets
      into tiers 0..3.
    - Tickets are written tier by tier (0 → 1 → 2 → 3) so structurally
      weaker patterns are pushed toward the end of the file.
    - Each line: "position;m1,m2,m3,m4,m5,m6"
    """
    root = Path(_ROOT_DIR)
    out_dir = root / "la_primitiva_pools"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"la_primitiva_{draw_date}.txt"
    print(
        f"[la-prim-fullwheel] start generate TXT draw_date={draw_date!r} mains_pool_len={len(list(dict.fromkeys(int(x) for x in mains_pool)))} path={out_path}",
        flush=True,
    )

    position = 0
    total_by_tier = [0, 0, 0, 0]

    with out_path.open("w", encoding="utf-8", buffering=1024 * 1024) as f:
        for tier in range(4):
            seed = (hash((draw_date, "la-primitiva", tier)) & 0xFFFFFFFF)
            rng = random.Random(seed)
            buffer: List[List[int]] = []
            buffer_size = 1000

            for mains in _iter_la_primitiva_tickets_from_pool(mains_pool):
                t = _la_primitiva_ticket_tier(mains)
                if t != tier:
                    continue
                buffer.append(list(mains))
                if len(buffer) >= buffer_size:
                    rng.shuffle(buffer)
                    for m in buffer:
                        position += 1
                        mains_str = ",".join(str(n) for n in m)
                        f.write(f"{position};{mains_str}\n")
                        total_by_tier[tier] += 1
                    buffer.clear()

            if buffer:
                rng.shuffle(buffer)
                for m in buffer:
                    position += 1
                    mains_str = ",".join(str(n) for n in m)
                    f.write(f"{position};{mains_str}\n")
                    total_by_tier[tier] += 1

    good_count = total_by_tier[0] + total_by_tier[1] + total_by_tier[2]
    bad_count = total_by_tier[3]
    total = position
    print(
        f"[la-prim-fullwheel] finished TXT draw_date={draw_date!r} total={total} good={good_count} bad={bad_count}",
        flush=True,
    )
    return {
        "file_path": str(out_path),
        "draw_date": draw_date,
        "good_tickets": good_count,
        "bad_tickets": bad_count,
        "total_tickets": total,
    }


def _el_gordo_ticket_tier(mains: Sequence[int]) -> int:
    """
    Classify an El Gordo ticket into quality tiers, reusing the same structural
    ideas as Euromillones:

    - Penalize long consecutive runs.
    - Penalize all-in-one decade or all same last digit.
    - Penalize all odd or all even.

    Returns 0 (best) .. 3 (worst).
    """
    nums = sorted(int(n) for n in mains)
    score = 0

    # Consecutive runs
    longest_run = 1
    current_run = 1
    for i in range(1, len(nums)):
        if nums[i] == nums[i - 1] + 1:
            current_run += 1
            longest_run = max(longest_run, current_run)
        else:
            current_run = 1
    if longest_run >= 4:
        score += 3
    elif longest_run == 3:
        score += 1

    # Same decade (10s) or same last digit
    decades = {n // 10 for n in nums}
    last_digits = {n % 10 for n in nums}
    if len(decades) == 1:
        score += 2
    if len(last_digits) == 1:
        score += 2

    # All odd or all even
    odds = sum(1 for n in nums if n % 2 == 1)
    evens = len(nums) - odds
    if odds == len(nums) or evens == len(nums):
        score += 2

    if score >= 5:
        return 3
    if score >= 3:
        return 2
    if score >= 1:
        return 1
    return 0


def _iter_el_gordo_tickets_from_pool(
    mains_pool: Iterable[int],
    clave_pool: Iterable[int],
) -> Iterable[Tuple[Sequence[int], int]]:
    """
    Yield all El Gordo tickets in manifold order (mirror Euromillones full wheel).

    Traverses the (main_combo_index × clave_index) grid in a diagonal pattern so we
    avoid long runs of identical mains or identical clave. Each (mains[5], clave)
    is yielded exactly once.

    - mains_pool: ordered mains (length 54 expected after Step 4 extend)
    - clave_pool: ordered claves (length 10 expected)
    """
    mains_list = list(dict.fromkeys(int(x) for x in mains_pool))
    clave_list = list(dict.fromkeys(int(x) for x in clave_pool))
    if len(mains_list) < 5:
        raise ValueError(f"Need at least 5 mains, got {len(mains_list)}")
    if not clave_list:
        raise ValueError("Need at least 1 clave")

    main_idx_combos = list(combinations(range(len(mains_list)), 5))
    clave_indices = list(range(len(clave_list)))

    seed = (hash((tuple(mains_list), tuple(clave_list))) & 0xFFFFFFFF)
    rng = random.Random(seed)
    rng.shuffle(main_idx_combos)
    rng.shuffle(clave_indices)

    nm = len(main_idx_combos)
    nc = len(clave_list)
    total = nm * nc
    for k in range(total):
        i = k % nm
        t = k // nm
        j = (t + i) % nc
        mains = [mains_list[idx] for idx in main_idx_combos[i]]
        clave = clave_list[clave_indices[j]]
        yield mains, int(clave)


def _generate_el_gordo_full_wheel_file(
    mains_pool: Iterable[int],
    clave_pool: Iterable[int],
    draw_date: str,
) -> dict:
    """
    Generate full El Gordo wheel TXT from the given pools (mirror Euromillones).

    - Tickets are generated in manifold order: diagonal traversal over
      (main combos × claves) with deterministic shuffle to avoid long runs
      of identical mains or clave.
    - Tier is applied to main numbers only: _el_gordo_ticket_tier(mains) → 0..3.
    - Write tier by tier (0 → 1 → 2 → 3); within each tier, buffer and shuffle
      with a deterministic seed so the order is reproducible.
    """
    # Debug: show the exact pools used by the wheeling system (after Step 4)
    mains_list_dbg = list(mains_pool)
    clave_list_dbg = list(clave_pool)
    print(
        f"[el-gordo-fullwheel] START draw_date={draw_date} mains_pool={mains_list_dbg} clave_pool={clave_list_dbg}",
        flush=True,
    )

    # Use local lists for iteration below so we don't re-iterate the debug copies.
    mains_pool = mains_list_dbg
    clave_pool = clave_list_dbg

    root = Path(_ROOT_DIR)
    out_dir = root / "el_gordo_pools"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"el_gordo_{draw_date}.txt"

    position = 0
    total_by_tier = [0, 0, 0, 0]
    debug_first_tickets: List[Tuple[List[int], int]] = []

    with out_path.open("w", encoding="utf-8", buffering=1024 * 1024) as f:
        for tier in range(4):
            seed = (hash(("el_gordo_full_wheel", draw_date, tier)) & 0xFFFFFFFF)
            rng = random.Random(seed)
            buffer: List[Tuple[List[int], int]] = []
            buffer_size = 1000

            for mains, clave in _iter_el_gordo_tickets_from_pool(mains_pool, clave_pool):
                t = _el_gordo_ticket_tier(mains)
                if t != tier:
                    continue
                buffer.append((list(mains), int(clave)))
                if len(buffer) >= buffer_size:
                    rng.shuffle(buffer)
                    for m, c in buffer:
                        position += 1
                        mains_str = ",".join(str(n) for n in m)
                        f.write(f"{position};{mains_str};{c}\n")
                        total_by_tier[tier] += 1
                    buffer.clear()

            if buffer:
                rng.shuffle(buffer)
                for m, c in buffer:
                    position += 1
                    mains_str = ",".join(str(n) for n in m)
                    f.write(f"{position};{mains_str};{c}\n")
                    total_by_tier[tier] += 1

    # Debug: collect first 30 tickets from the generated file
    try:
        with out_path.open("r", encoding="utf-8") as f_debug:
            for _ in range(30):
                line = f_debug.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    pos_str, mains_str, clave_str = line.split(";")
                    pos = int(pos_str)
                    mains = [int(x) for x in mains_str.split(",") if x]
                    clave = int(clave_str)
                    debug_first_tickets.append((mains, clave))
                except Exception:
                    continue
        print(
            f"[el-gordo-fullwheel] first_tickets (up to 30): {debug_first_tickets}",
            flush=True,
        )
    except Exception as e:
        print(f"[el-gordo-fullwheel] debug read failed: {e}", flush=True)

    total = position
    return {
        "file_path": str(out_path),
        "draw_date": draw_date,
        "good_tickets": total_by_tier[0] + total_by_tier[1] + total_by_tier[2],
        "bad_tickets": total_by_tier[3],
        "total_tickets": total,
    }


def build_el_gordo_step4_pool(
    mains_probs: List[Dict],
    clave_probs: List[Dict],
    prev_main_numbers: Optional[List[int]] = None,
    prev_clave: Optional[int] = None,
    seed: Optional[int] = None,
) -> Dict:
    """
    Step 4 pool for El Gordo: mains + clave candidates.

    - 4–5 mains + 1 clave from previous draw (prev_main_numbers, prev_clave) if available,
      else random. No duplicate with the rest.
    - Additional mains from Step 3 ranking bands (mains_probs sorted by p), no duplicate:
        * From 1st–20th select 6–7 (random)
        * From 21st–30th select 3
        * From 31st–40th select 3
        * From 41st–54th select 3
      Final mains list is truncated to 20 if longer.
    - Clave pool:
        * 1 clave from previous draw (snapshot) if available, else random
        * Up to 3 additional claves from ranking (clave_probs sorted by p), no duplicate
      Final clave list is truncated to 4.
    """
    if seed is not None:
        random.seed(seed)
    MAIN_MIN, MAIN_MAX = 1, 54
    CLAVE_MIN, CLAVE_MAX = 0, 9
    mains_ranking = sorted(mains_probs, key=lambda x: (x.get("p") or 0), reverse=True)
    clave_ranking = sorted(clave_probs, key=lambda x: (x.get("p") or 0), reverse=True)
    rules_used: List[str] = []
    excluded = {"mains": [], "clave": []}

    # --- 4–5 mains + 1 clave from previous draw or random ---
    if prev_main_numbers and len(prev_main_numbers) >= 4 and isinstance(prev_clave, int):
        pick_mains = list(prev_main_numbers)[:5]
        prev_count = 5 if len(pick_mains) >= 5 else len(pick_mains)
        target_prev = random.choice([4, 5])
        target_prev = min(target_prev, prev_count)
        base_mains = random.sample(pick_mains, target_prev)
        base_clave = int(prev_clave)
        rules_used.append(f"from_previous_draw_{target_prev}m_1c")
    else:
        target_prev = random.choice([4, 5])
        base_mains = random.sample(range(MAIN_MIN, MAIN_MAX + 1), target_prev)
        base_clave = random.randint(CLAVE_MIN, CLAVE_MAX)
        rules_used.append(f"random_{target_prev}m_1c")

    main_set = set(base_mains)
    clave_set = {base_clave}

    # --- Mains from ranking bands (avoid duplicates with snapshot mains) ---
    def take_random_from_ranking(
        ranking: List[Dict],
        start_1based: int,
        end_1based: int,
        count: int,
        exclude: set,
    ) -> List[Dict]:
        segment = [
            x
            for x in ranking[start_1based - 1 : end_1based]
            if int(x.get("number") or 0) not in exclude
        ]
        if len(segment) <= count:
            chosen = list(segment)
        else:
            chosen = random.sample(segment, count)
        for item in chosen:
            exclude.add(int(item.get("number") or 0))
        return chosen

    top_count = random.choice([6, 7])
    from_1_20 = take_random_from_ranking(mains_ranking, 1, 20, top_count, main_set)
    from_21_30 = take_random_from_ranking(mains_ranking, 21, 30, 3, main_set)
    from_31_40 = take_random_from_ranking(mains_ranking, 31, 40, 3, main_set)
    from_41_54 = take_random_from_ranking(mains_ranking, 41, 54, 3, main_set)
    rules_used.append(f"el_gordo_mains_bands_prev_{len(base_mains)}_top_{top_count}_3_3_3")

    filtered_mains_items: List[Dict] = []
    for n in base_mains:
        p = next((x.get("p") for x in mains_probs if int(x.get("number") or 0) == n), 0.0)
        filtered_mains_items.append({"number": n, "p": p})
    for seg in (from_1_20, from_21_30, from_31_40, from_41_54):
        filtered_mains_items.extend(seg)
    filtered_mains = filtered_mains_items[:20]

    # --- Clave pool: snapshot + top ranking claves (avoid duplicates) ---
    filtered_clave_items: List[Dict] = []
    p_clave = next(
        (x.get("p") for x in clave_probs if int(x.get("number") or 0) == base_clave), 0.0
    )
    filtered_clave_items.append({"number": base_clave, "p": p_clave})

    extras = [x for x in clave_ranking if int(x.get("number") or 0) not in clave_set]
    max_extras = max(0, 4 - len(filtered_clave_items))
    filtered_clave_items.extend(extras[:max_extras])
    filtered_clave = filtered_clave_items[:4]

    # Randomly reorder prioritized subset so Step 5 receives shuffled pools.
    random.shuffle(filtered_mains)
    random.shuffle(filtered_clave)

    # --- Extend to full 54 mains and 10 claves (mirror Euromillones full wheel pool) ---
    selected_main_nums = {int(r.get("number") or 0) for r in filtered_mains}
    for item in mains_ranking:
        n = int(item.get("number") or 0)
        if n < MAIN_MIN or n > MAIN_MAX:
            continue
        if n in selected_main_nums:
            continue
        filtered_mains.append({"number": n, "p": item.get("p") or 0.0})
        selected_main_nums.add(n)

    selected_clave_nums = {int(r.get("number") or 0) for r in filtered_clave}
    for item in clave_ranking:
        n = int(item.get("number") or 0)
        if n < CLAVE_MIN or n > CLAVE_MAX:
            continue
        if n in selected_clave_nums:
            continue
        filtered_clave.append({"number": n, "p": item.get("p") or 0.0})
        selected_clave_nums.add(n)

    def _stats(rows: List[Dict]) -> Dict:
        nums = [int(r.get("number") or 0) for r in rows]
        return {
            "count": len(nums),
            "sum": sum(nums),
            "even": sum(1 for n in nums if n % 2 == 0),
            "odd": sum(1 for n in nums if n % 2 != 0),
        }

    stats = {"mains": _stats(filtered_mains), "clave": _stats(filtered_clave)}
    return {
        "filtered_mains": filtered_mains,
        "filtered_clave": filtered_clave,
        "rules_used": rules_used,
        "excluded": excluded,
        "stats": stats,
        "snapshot_mains": base_mains,
        "snapshot_clave": [base_clave],
    }


def build_la_primitiva_step4_pool(
    mains_probs: List[Dict],
    reintegro_probs: List[Dict],
    prev_main_numbers: Optional[List[int]] = None,
    prev_reintegro: Optional[int] = None,
    seed: Optional[int] = None,
) -> Dict:
    """
    Step 4 pool for La Primitiva: mains + reintegro candidates.

    - 4–5 mains + 1 reintegro from previous draw (prev_main_numbers, prev_reintegro) if available,
      else random. No duplicate with the rest.
    - Additional mains from Step 3 ranking bands (mains_probs sorted by p), no duplicate:
        * From 1st–20th select 6–8 (random)
        * From 21st–35th select 4
        * From 36th–49th select 4
      These form the *prioritized* mains set (up to 20 numbers).
    - Reintegro pool:
        * 1 reintegro from previous draw (snapshot) if available, else random
        * Up to 3 additional reintegros from ranking (reintegro_probs sorted by p), no duplicate
      These form the *prioritized* reintegro set (up to 4 numbers).

    After building the prioritized sets, we EXTEND them so that:
      - filtered_mains contains ALL 49 mains (1..49), ordered with prioritized mains first,
        then remaining numbers in probability order.
      - filtered_reintegro contains ALL 10 reintegros (0..9), ordered with prioritized
        reintegros first, then remaining numbers in probability order.
    """
    if seed is not None:
        random.seed(seed)
    MAIN_MIN, MAIN_MAX = 1, 49
    REIN_MIN, REIN_MAX = 0, 9
    mains_ranking = sorted(mains_probs, key=lambda x: (x.get("p") or 0), reverse=True)
    rein_ranking = sorted(reintegro_probs, key=lambda x: (x.get("p") or 0), reverse=True)
    rules_used: List[str] = []
    excluded = {"mains": [], "reintegro": []}

    # --- 4–5 mains + 1 reintegro from previous draw or random ---
    if prev_main_numbers and len(prev_main_numbers) >= 4 and isinstance(prev_reintegro, int):
        pick_mains = list(prev_main_numbers)[:6]
        prev_count = len(pick_mains)
        target_prev = random.choice([4, 5])
        target_prev = min(target_prev, prev_count)
        base_mains = random.sample(pick_mains, target_prev)
        base_rein = int(prev_reintegro)
        rules_used.append(f"from_previous_draw_{target_prev}m_1r")
    else:
        target_prev = random.choice([4, 5])
        base_mains = random.sample(range(MAIN_MIN, MAIN_MAX + 1), target_prev)
        base_rein = random.randint(REIN_MIN, REIN_MAX)
        rules_used.append(f"random_{target_prev}m_1r")

    main_set = set(base_mains)
    rein_set = {base_rein}

    # --- Mains from ranking bands (avoid duplicates with snapshot mains) ---
    def take_random_from_ranking(
        ranking: List[Dict],
        start_1based: int,
        end_1based: int,
        count: int,
        exclude: set,
    ) -> List[Dict]:
        segment = [
            x
            for x in ranking[start_1based - 1 : end_1based]
            if int(x.get("number") or 0) not in exclude
        ]
        if len(segment) <= count:
            chosen = list(segment)
        else:
            chosen = random.sample(segment, count)
        for item in chosen:
            exclude.add(int(item.get("number") or 0))
        return chosen

    # Prioritized main pool target: 4–5 prev + (6–8) + 4 + 4 from bands (<= 20)
    MAIN_POOL_MAX = 20
    top_count = random.choice([6, 7, 8])
    from_1_20 = take_random_from_ranking(mains_ranking, 1, 20, top_count, main_set)
    from_21_35 = take_random_from_ranking(mains_ranking, 21, 35, 4, main_set)
    from_36_49 = take_random_from_ranking(mains_ranking, 36, 49, 4, main_set)
    rules_used.append(
        f"la_primitiva_mains_bands_prev_{len(base_mains)}_top_{top_count}_4_4"
    )

    filtered_mains_items: List[Dict] = []
    for n in base_mains:
        p = next((x.get("p") for x in mains_probs if int(x.get("number") or 0) == n), 0.0)
        filtered_mains_items.append({"number": n, "p": p})
    for seg in (from_1_20, from_21_35, from_36_49):
        filtered_mains_items.extend(seg)
    prioritized_mains = filtered_mains_items[:MAIN_POOL_MAX]

    # --- Reintegro prioritized pool (cap 4): snapshot + top ranking reintegros (avoid duplicates) ---
    REIN_POOL_MAX = 4
    filtered_rein_items: List[Dict] = []
    p_rein = next(
        (x.get("p") for x in reintegro_probs if int(x.get("number") or 0) == base_rein),
        0.0,
    )
    filtered_rein_items.append({"number": base_rein, "p": p_rein})

    extras = [x for x in rein_ranking if int(x.get("number") or 0) not in rein_set]
    max_extras = max(0, REIN_POOL_MAX - len(filtered_rein_items))
    filtered_rein_items.extend(extras[:max_extras])
    prioritized_rein = filtered_rein_items[:REIN_POOL_MAX]

    # --- Extend to full 49 mains and 10 reintegros (prioritized first, then remaining by prob) ---
    prioritized_main_numbers = {int(x["number"]) for x in prioritized_mains}
    extended_mains: List[Dict] = list(prioritized_mains)
    for item in mains_ranking:
        n = int(item.get("number") or 0)
        if n < MAIN_MIN or n > MAIN_MAX or n in prioritized_main_numbers:
            continue
        extended_mains.append({"number": n, "p": item.get("p") or 0.0})
        prioritized_main_numbers.add(n)
        if len(prioritized_main_numbers) >= (MAIN_MAX - MAIN_MIN + 1):
            break
    # Ensure all 1..49 present (fallback in case probs missing some numbers)
    for n in range(MAIN_MIN, MAIN_MAX + 1):
        if n not in prioritized_main_numbers:
            extended_mains.append({"number": n, "p": 0.0})
            prioritized_main_numbers.add(n)

    prioritized_rein_numbers = {int(x["number"]) for x in prioritized_rein}
    extended_rein: List[Dict] = list(prioritized_rein)
    for item in rein_ranking:
        n = int(item.get("number") or 0)
        if n < REIN_MIN or n > REIN_MAX or n in prioritized_rein_numbers:
            continue
        extended_rein.append({"number": n, "p": item.get("p") or 0.0})
        prioritized_rein_numbers.add(n)
        if len(prioritized_rein_numbers) >= (REIN_MAX - REIN_MIN + 1):
            break
    for n in range(REIN_MIN, REIN_MAX + 1):
        if n not in prioritized_rein_numbers:
            extended_rein.append({"number": n, "p": 0.0})
            prioritized_rein_numbers.add(n)

    filtered_mains = extended_mains
    filtered_rein = extended_rein

    def _stats(rows: List[Dict]) -> Dict:
        nums = [int(r.get("number") or 0) for r in rows]
        return {
            "count": len(nums),
            "sum": sum(nums),
            "even": sum(1 for n in nums if n % 2 == 0),
            "odd": sum(1 for n in nums if n % 2 != 0),
        }

    stats = {"mains": _stats(filtered_mains), "reintegro": _stats(filtered_rein)}
    return {
        "filtered_mains": filtered_mains,
        "filtered_reintegro": filtered_rein,
        "rules_used": rules_used,
        "excluded": excluded,
        "stats": stats,
        "snapshot_mains": base_mains,
        "snapshot_reintegro": [base_rein],
    }

try:
    from simulation.euromillones.frequency_model import (
        predict_next_frequency_scores,
        save_frequency_simulation_result,
        train_all_frequency_models,
    )
    from simulation.euromillones.gap_model import (
        predict_next_gap_scores,
        save_gap_simulation_result,
        train_all_gap_models,
    )
    from simulation.euromillones.hot_model import (
        predict_next_hot_scores,
        save_hot_simulation_result,
        train_all_hot_models,
    )
    from simulation.euromillones.prediction_compare import compare_prediction_with_result
    from simulation.euromillones.candidate_pool import build_candidate_pool
    from simulation.el_gordo.candidate_pool import build_el_gordo_candidate_pool
    from simulation.el_gordo.frequency_model import (
        predict_next_el_gordo_frequency_scores,
        save_el_gordo_frequency_simulation_result,
        train_all_el_gordo_frequency_models,
    )
    from simulation.el_gordo.gap_model import (
        predict_next_el_gordo_gap_scores,
        save_el_gordo_gap_simulation_result,
        train_all_el_gordo_gap_models,
    )
    from simulation.el_gordo.hot_model import (
        predict_next_el_gordo_hot_scores,
        save_el_gordo_hot_simulation_result,
        train_all_el_gordo_hot_models,
    )
    from simulation.el_gordo.simple_simulation import run_el_gordo_simple_simulation
    SIMULATION_AVAILABLE = True
except ModuleNotFoundError:
    # Older environments or stripped-down deployments may not include the
    # optional `simulation` package. In that case we still want the API to
    # start; the specific simulation endpoints will fail at call time.
    SIMULATION_AVAILABLE = False

    def _missing_simulation(*_args, **_kwargs):
        raise RuntimeError("Simulation models are not available in this environment.")

    predict_next_frequency_scores = _missing_simulation  # type: ignore[assignment]
    save_frequency_simulation_result = _missing_simulation  # type: ignore[assignment]
    train_all_frequency_models = _missing_simulation  # type: ignore[assignment]

    predict_next_gap_scores = _missing_simulation  # type: ignore[assignment]
    save_gap_simulation_result = _missing_simulation  # type: ignore[assignment]
    train_all_gap_models = _missing_simulation  # type: ignore[assignment]

    predict_next_hot_scores = _missing_simulation  # type: ignore[assignment]
    save_hot_simulation_result = _missing_simulation  # type: ignore[assignment]
    train_all_hot_models = _missing_simulation  # type: ignore[assignment]

    compare_prediction_with_result = _missing_simulation  # type: ignore[assignment]
    build_candidate_pool = _missing_simulation  # type: ignore[assignment]

    build_el_gordo_candidate_pool = _missing_simulation  # type: ignore[assignment]
    predict_next_el_gordo_frequency_scores = _missing_simulation  # type: ignore[assignment]
    save_el_gordo_frequency_simulation_result = _missing_simulation  # type: ignore[assignment]
    train_all_el_gordo_frequency_models = _missing_simulation  # type: ignore[assignment]
    predict_next_el_gordo_gap_scores = _missing_simulation  # type: ignore[assignment]
    save_el_gordo_gap_simulation_result = _missing_simulation  # type: ignore[assignment]
    train_all_el_gordo_gap_models = _missing_simulation  # type: ignore[assignment]
    predict_next_el_gordo_hot_scores = _missing_simulation  # type: ignore[assignment]
    save_el_gordo_hot_simulation_result = _missing_simulation  # type: ignore[assignment]
    train_all_el_gordo_hot_models = _missing_simulation  # type: ignore[assignment]
    run_el_gordo_simple_simulation = _missing_simulation  # type: ignore[assignment]

# Lottery slug -> game_id for loteriasyapuestas.es API (El Gordo = ELGR per site)
GAME_IDS = {
    "la-primitiva": "LAPR",
    "euromillones": "EMIL",
    "el-gordo": "ELGR",
}

# Results page path per lottery (load this first so real Chrome has correct referer/cookies)
RESULTS_PATHS = {
    "la-primitiva": "/es/resultados/la-primitiva",
    "euromillones": "/es/resultados/euromillones",
    "el-gordo": "/es/resultados/gordo-primitiva",
}

# Daily scrape at 00:02 and Update button: process in this order
LOTTERY_DAILY_ORDER = ["euromillones", "la-primitiva", "el-gordo"]

BASE_URL = "https://www.loteriasyapuestas.es/servicios/buscadorSorteos"
SITE_ORIGIN = "https://www.loteriasyapuestas.es"

# One collection per lottery (El Gordo = ELGR)
COLLECTIONS = {"LAPR": "la_primitiva", "EMIL": "euromillones", "ELGR": "el_gordo"}
METADATA_COLLECTION = "scraper_metadata"
EUROMILLONES_TRAIN_PROGRESS_COLLECTION = "euromillones_train_progress"
EUROMILLONES_COMPARE_RESULTS_COLLECTION = "euromillones_compare_results"
EL_GORDO_TRAIN_PROGRESS_COLLECTION = "el_gordo_train_progress"
EL_GORDO_COMPARE_RESULTS_COLLECTION = "el_gordo_compare_results"
LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION = "la_primitiva_train_progress"
LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION = "la_primitiva_compare_results"
# Cost per Euromillones ticket (€) for compare total cost
EUROMILLONES_TICKET_COST_EUR = 2.50
# Locks so only one full-wheel reorder per lottery runs at a time (avoids concurrent file write / corrupt state)
_EUROMILLONES_REORDER_LOCK = threading.Lock()
_EL_GORDO_REORDER_LOCK = threading.Lock()
_LA_PRIMITIVA_REORDER_LOCK = threading.Lock()
EL_GORDO_BUY_QUEUE_COLLECTION = "el_gordo_buy_queue"
LA_PRIMITIVA_BUY_QUEUE_COLLECTION = "la_primitiva_buy_queue"
EUROMILLONES_BUY_QUEUE_COLLECTION = "euromillones_buy_queue"
BOT_CREDENTIALS_COLLECTION = "bot_credentials"

logger = logging.getLogger("lottery")

# MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "lottery")

client: MongoClient | None = None
db = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global client, db
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    for coll_name in COLLECTIONS.values():
        db[coll_name].create_index("id_sorteo", unique=True)
    db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION].create_index("cutoff_draw_id", unique=True)
    db[EUROMILLONES_COMPARE_RESULTS_COLLECTION].create_index(
        [("current_id", 1), ("pre_id", 1)], unique=True
    )
    db[EL_GORDO_TRAIN_PROGRESS_COLLECTION].create_index("cutoff_draw_id", unique=True)
    db[EL_GORDO_COMPARE_RESULTS_COLLECTION].create_index(
        [("current_id", 1), ("pre_id", 1)], unique=True
    )
    db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION].create_index("cutoff_draw_id", unique=True)
    db[LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION].create_index(
        [("current_id", 1), ("pre_id", 1)], unique=True
    )
    db[LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION].create_index(
        [("current_id", 1), ("pre_id", 1)], unique=True
    )
    db[EL_GORDO_BUY_QUEUE_COLLECTION].create_index([("status", 1), ("created_at", 1)])
    # bot_credentials: no index on "order" at startup (optional; avoids conflict if one already exists)
    yield
    if client:
        client.close()


def parse_combinacion(combinacion: str) -> dict:
    """
    Parse "04 - 12 - 16 - 37 - 39 - 45 C(44) R(9)" into numbers array, C, R.
    """
    numbers = []
    complementario = None
    reintegro = None
    if not combinacion or not isinstance(combinacion, str):
        return {"numbers": numbers, "complementario": complementario, "reintegro": reintegro}
    match_c = re.search(r"C\((\d+)\)", combinacion)
    match_r = re.search(r"R\((\d+)\)", combinacion)
    if match_c:
        complementario = int(match_c.group(1))
    if match_r:
        reintegro = int(match_r.group(1))
    main_part = re.split(r"\s+C\(|\s+R\(", combinacion)[0].strip()
    for part in main_part.split("-"):
        part = part.strip()
        if part.isdigit():
            numbers.append(int(part))
    return {"numbers": numbers, "complementario": complementario, "reintegro": reintegro}


def normalize_draw(draw: dict) -> dict:
    """Add parsed numbers, C, R, and joker_combinacion; keep all original fields."""
    out = dict(draw)
    combinacion = draw.get("combinacion") or ""
    parsed = parse_combinacion(combinacion)
    out["numbers"] = parsed["numbers"]
    out["complementario"] = parsed["complementario"]
    out["reintegro"] = parsed["reintegro"]
    joker = draw.get("joker") or {}
    millon = draw.get("millon") or {}
    out["joker_combinacion"] = (
        (joker.get("combinacion") if isinstance(joker, dict) else None)
        or (millon.get("combinacion") if isinstance(millon, dict) else None)
    )
    return out


app = FastAPI(title="Lottery Prediction API", lifespan=lifespan)

# Allow frontend on common dev ports (5173 = default Vite, 5174+ when 5173 is in use)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://151.241.216.178:5173"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


from starlette.middleware.base import BaseHTTPMiddleware


class AuthTokenMiddleware(BaseHTTPMiddleware):
    """Require Authorization Bearer token for /api/* except public paths."""

    async def dispatch(self, request, call_next):
        path = request.url.path
        if request.method == "OPTIONS":
            return await call_next(request)
        if path in _PUBLIC_API_PATHS:
            return await call_next(request)
        if not path.startswith("/api/"):
            return await call_next(request)
        auth = request.headers.get("Authorization") or ""
        if not auth.startswith("Bearer "):
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing or invalid authorization"},
                headers={"WWW-Authenticate": "Bearer"},
            )
        token = auth[7:].strip()
        now = time.time()
        if token not in _valid_tokens:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or expired token"},
                headers={"WWW-Authenticate": "Bearer"},
            )
        if now > _valid_tokens[token]:
            del _valid_tokens[token]
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or expired token"},
                headers={"WWW-Authenticate": "Bearer"},
            )
        return await call_next(request)


app.add_middleware(AuthTokenMiddleware)


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.post("/api/auth/verify")
async def api_auth_verify(request: Request):
    """
    Verify platform password. Body: { "password": "..." }.
    Returns { "ok": true, "token": "..." } if ADMIN_PASSWORD is set and matches.
    If ADMIN_PASSWORD is not set in .env, any password is accepted (gate disabled).
    """
    try:
        body = await request.json() or {}
    except Exception:
        body = {}
    submitted = (body.get("password") or "").strip()
    if not ADMIN_PASSWORD:
        token = secrets.token_urlsafe(32)
        _valid_tokens[token] = time.time() + TOKEN_TTL_SEC
        return JSONResponse(
            content={"ok": True, "token": token},
            headers={"Cache-Control": "no-store"},
        )
    if submitted != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")
    token = secrets.token_urlsafe(32)
    _valid_tokens[token] = time.time() + TOKEN_TTL_SEC
    return JSONResponse(
        content={"ok": True, "token": token},
        headers={"Cache-Control": "no-store"},
    )


def create_driver() -> webdriver.Chrome:
    """
    Create a new Chrome browser via Selenium (same pattern as refer.py).
    Headless on all platforms for API use; Linux gets extra stability flags.
    """
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280,800")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    if sys.platform.startswith("linux"):
        options.add_argument("--disable-setuid-sandbox")
        try:
            import shutil
            for path in ("/usr/bin/google-chrome", "/usr/bin/google-chrome-stable"):
                if shutil.which(path):
                    options.binary_location = path
                    break
        except Exception:
            pass
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(30)
    driver.set_script_timeout(30)
    return driver


def _parse_proximo_bote_from_page(driver) -> Optional[float]:
    """
    Parse Próx. bote (next jackpot) from the loaded results page.
    Looks for span.c-elemento-destacado_cantidad-millones (e.g. "193") and treats as millions.
    Returns value in euros (e.g. 193 -> 193_000_000.0), or None if not found.
    """
    try:
        # Pattern from loteriasyapuestas.es: number in span with class cantidad-millones
        el = driver.find_element(By.CSS_SELECTOR, ".c-elemento-destacado_cantidad-millones")
        text = (el.text or "").strip().replace(",", ".").replace("\u00a0", "").replace(" ", "")
        if not text:
            return None
        num = float(text)
        # Class name indicates "millones"; 193 -> 193 million euros
        return num * 1_000_000.0
    except Exception:
        try:
            # Fallback: any element with cantidad-millones in class
            el = driver.find_element(By.CSS_SELECTOR, "[class*='cantidad-millones']")
            text = (el.text or "").strip().replace(",", ".").replace("\u00a0", "").replace(" ", "")
            if not text:
                return None
            num = float(text)
            return num * 1_000_000.0
        except Exception:
            return None


def _scrape_with_selenium(api_url: str, results_page_url: str) -> tuple:
    """
    Launch Chrome, load results page, parse Próx. bote from HTML, fetch API, return (draws, proximo_bote_eur).
    proximo_bote_eur is None if parsing failed.
    """
    driver = None
    proximo_bote_eur: Optional[float] = None
    try:
        driver = create_driver()
        driver.get(results_page_url)
        proximo_bote_eur = _parse_proximo_bote_from_page(driver)
        data = driver.execute_async_script(
            """
            var url = arguments[0];
            var callback = arguments[arguments.length - 1];
            fetch(url)
                .then(function(r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
                .then(callback)
                .catch(function(e) { callback({__error: e.message}); });
            """,
            api_url,
        )
        if isinstance(data, dict) and data.get("__error"):
            raise RuntimeError(data["__error"])
        return (data, proximo_bote_eur)
    except Exception:
        raise
    finally:
        if driver:
            try:
                driver.quit()
            except Exception as e:
                logger.warning("Driver quit failed (ignored): %s", e)


@app.get("/api/scrape")
def scrape(
    start_date: str = Query(..., description="YYYYMMDD"),
    end_date: str = Query(..., description="YYYYMMDD"),
    lottery: str = Query(..., description="la-primitiva | euromillones | el-gordo"),
):
    """Fetch draws from loteriasyapuestas.es using Selenium Chrome, save to MongoDB."""
    game_id = GAME_IDS.get(lottery)
    if not game_id:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}")

    api_url = (
        f"{BASE_URL}"
        f"?game_id={game_id}"
        "&celebrados=true"
        f"&fechaInicioInclusiva={start_date}"
        f"&fechaFinInclusiva={end_date}"
    )
    results_path = RESULTS_PATHS.get(lottery, RESULTS_PATHS["la-primitiva"])
    results_page_url = f"{SITE_ORIGIN}{results_path}"

    try:
        data, proximo_bote_eur = _scrape_with_selenium(api_url, results_page_url)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Selenium scrape error")
        raise HTTPException(502, detail=f"Chrome scrape failed: {e!s}")

    if not isinstance(data, list):
        got = type(data).__name__
        if isinstance(data, dict):
            got = f"dict keys: {list(data.keys())[:10]}"
        raise HTTPException(
            502,
            detail=f"API did not return a list of draws (got {got})",
        )

    saved, errors = _save_draws_to_db(data)
    max_date = _max_date_from_draws(data)
    if not max_date:
        max_date = _get_max_draw_date_from_db(game_id)
    if max_date:
        _set_last_draw_date(lottery, max_date)
    _update_next_funds_metadata(lottery, scraped_bote=proximo_bote_eur)

    return {
        "saved": saved,
        "total": len(data),
        "lottery": lottery,
        "game_id": game_id,
        "start_date": start_date,
        "end_date": end_date,
        "proximo_bote_eur": proximo_bote_eur,
        "message": f"Saved {saved} draws to MongoDB.",
        "errors": errors[:5] if errors else None,
    }


def _get_last_draw_date(lottery: str) -> str | None:
    """Get last_draw_date for a lottery from scraper_metadata."""
    if db is None:
        return None
    doc = db[METADATA_COLLECTION].find_one({"lottery": lottery}, projection=["last_draw_date"])
    return (doc.get("last_draw_date") or "").strip() or None


@app.get("/api/metadata/next-draws")
def get_next_draws_metadata():
    """
    Return last_draw_date and next_draw_date for each lottery from scraper_metadata.

    Response example:
      {
        "items": [
          { "lottery": "euromillones", "last_draw_date": "2026-02-27", "next_draw_date": "2026-03-03" },
          { "lottery": "la-primitiva", "last_draw_date": "...", "next_draw_date": "..." },
          { "lottery": "el-gordo", "last_draw_date": "...", "next_draw_date": "..." }
        ]
      }
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    cursor = db[METADATA_COLLECTION].find(
        {},
        projection={
            "_id": 1,
            "lottery": 1,
            "last_draw_date": 1,
            "next_draw_date": 1,
            "next_bote": 1,
            "next_premios": 1,
            "next_funds_updated_at": 1,
        },
    )
    items = []
    for doc in cursor:
        d = _item_to_json(doc)
        next_bote = doc.get("next_bote")
        next_premios = doc.get("next_premios")
        if next_bote is None and doc.get("next_funds_prediction"):
            old = doc["next_funds_prediction"] or {}
            next_bote = (old.get("bote_stats") or {}).get("median")
            next_premios = next_premios or (old.get("premios_stats") or {}).get("median")
        if isinstance(next_bote, (int, float)):
            next_bote = float(next_bote)
        else:
            next_bote = None
        if isinstance(next_premios, (int, float)):
            next_premios = float(next_premios)
        else:
            next_premios = None
        d["next_funds_prediction"] = {
            "bote_stats": {"median": next_bote} if next_bote is not None else {},
            "premios_stats": {"median": next_premios} if next_premios is not None else {},
        }
        items.append(d)
    return JSONResponse(content={"items": items})


# --- Bot credentials (DB-stored username/password for lottery site; bot fetches active one) ---
BOT_CREDENTIALS_SECRET = (os.getenv("BOT_CREDENTIALS_SECRET") or "").strip() or None

# --- Platform auth: token issued on login; required for all API except health, auth, and bot endpoints ---
ADMIN_PASSWORD = (os.getenv("ADMIN_PASSWORD") or "").strip() or None
_valid_tokens: dict[str, float] = {}  # token -> expiry timestamp
TOKEN_TTL_SEC = 24 * 3600  # 24 hours

# Paths that do not require Authorization Bearer token (health, login, bot claim/complete, bot active-credentials, dev test helpers)
_PUBLIC_API_PATHS = {
    "/api/health",
    "/api/auth/verify",
    "/api/bot/active-credentials",
    "/api/el-gordo/betting/bot/claim",
    "/api/el-gordo/betting/bot/complete",
    "/api/euromillones/betting/bot/claim",
    "/api/euromillones/betting/bot/complete",
    "/api/la-primitiva/betting/bot/claim",
    "/api/la-primitiva/betting/bot/complete",
    # Dev-only helper to seed test bought tickets for Euromillones. Do NOT expose in production.
    "/api/dev/euromillones/betting/seed-test-bought",
    # Dev-only helper to seed test bought tickets for El Gordo. Do NOT expose in production.
    "/api/dev/el-gordo/betting/seed-test-bought",
    # Dev-only helper to seed test bought tickets for La Primitiva. Do NOT expose in production.
    "/api/dev/la-primitiva/betting/seed-test-bought",
}


@app.get("/api/bot/credentials")
def api_bot_credentials_list():
    """List all bot credentials (username, is_active, order); no password in response."""
    if db is None:
        raise HTTPException(503, detail="Database not connected")
    coll = db[BOT_CREDENTIALS_COLLECTION]
    cursor = coll.find({}, projection={"username": 1, "is_active": 1, "order": 1, "created_at": 1}).sort("order", 1)
    items = []
    for doc in cursor:
        items.append({
            "id": str(doc["_id"]),
            "username": doc.get("username") or "",
            "is_active": doc.get("is_active") is True,
            "order": doc.get("order", 0),
            "created_at": doc.get("created_at"),
        })
    return JSONResponse(content={"items": items}, headers={"Cache-Control": "no-store"})


@app.post("/api/bot/credentials")
async def api_bot_credentials_add(request: Request):
    """Add a bot credential. First one is set active by default."""
    if db is None:
        raise HTTPException(503, detail="Database not connected")
    try:
        body = await request.json() or {}
    except Exception:
        body = {}
    username = (body.get("username") or "").strip()
    password = (body.get("password") or "").strip()
    if not username or not password:
        raise HTTPException(400, detail="username and password required")
    coll = db[BOT_CREDENTIALS_COLLECTION]
    count = coll.count_documents({})
    is_active = count == 0
    order = count
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    doc = {
        "username": username,
        "password": password,
        "is_active": is_active,
        "order": order,
        "created_at": now,
    }
    ins = coll.insert_one(doc)
    return JSONResponse(
        content={"id": str(ins.inserted_id), "is_active": is_active, "order": order},
        status_code=201,
        headers={"Cache-Control": "no-store"},
    )


@app.patch("/api/bot/credentials/{credential_id}")
def api_bot_credentials_set_active(credential_id: str):
    """Set this credential as active; all others become inactive."""
    if db is None:
        raise HTTPException(503, detail="Database not connected")
    try:
        oid = ObjectId(credential_id)
    except Exception:
        raise HTTPException(400, detail="Invalid id")
    coll = db[BOT_CREDENTIALS_COLLECTION]
    if coll.find_one({"_id": oid}) is None:
        raise HTTPException(404, detail="Credential not found")
    coll.update_many({}, {"$set": {"is_active": False}})
    coll.update_one({"_id": oid}, {"$set": {"is_active": True}})
    return JSONResponse(content={"status": "ok"}, headers={"Cache-Control": "no-store"})


@app.delete("/api/bot/credentials/{credential_id}")
def api_bot_credentials_delete(credential_id: str):
    """Delete a credential. If it was active, the first remaining becomes active."""
    if db is None:
        raise HTTPException(503, detail="Database not connected")
    try:
        oid = ObjectId(credential_id)
    except Exception:
        raise HTTPException(400, detail="Invalid id")
    coll = db[BOT_CREDENTIALS_COLLECTION]
    doc = coll.find_one({"_id": oid})
    if doc is None:
        raise HTTPException(404, detail="Credential not found")
    was_active = doc.get("is_active") is True
    coll.delete_one({"_id": oid})
    if was_active:
        first = coll.find_one({}, sort=[("order", 1)])
        if first:
            coll.update_one({"_id": first["_id"]}, {"$set": {"is_active": True}})
    return JSONResponse(content={"status": "ok"}, headers={"Cache-Control": "no-store"})


@app.get("/api/bot/active-credentials")
def api_bot_active_credentials(request: Request):
    """
    Return active bot credential (username, password) from DB (bot_credentials collection).
    Finds document with is_active: true and returns it.
    When BOT_CREDENTIALS_SECRET is set in backend env, requires header X-Bot-Secret to match.
    When BOT_CREDENTIALS_SECRET is not set, returns active credential without auth (for local/same-machine bot).
    """
    if db is None:
        raise HTTPException(503, detail="Database not connected")
    if BOT_CREDENTIALS_SECRET:
        secret = (request.headers.get("X-Bot-Secret") or "").strip()
        if secret != BOT_CREDENTIALS_SECRET:
            raise HTTPException(401, detail="Invalid or missing X-Bot-Secret")
    coll = db[BOT_CREDENTIALS_COLLECTION]
    doc = coll.find_one({"is_active": True}, projection={"username": 1, "password": 1})
    if not doc:
        raise HTTPException(404, detail="No active bot credential")
    return JSONResponse(
        content={"username": doc.get("username") or "", "password": doc.get("password") or ""},
        headers={"Cache-Control": "no-store"},
    )


@app.get("/api/dashboard/sample-tickets")
def api_dashboard_sample_tickets(
    count: int = Query(10, ge=1, le=20, description="Number of random tickets per lottery"),
):
    """
    For homepage alert: get last_draw_date from scraper_metadata per lottery,
    find *train_progress by probs_fecha_sorteo === last_draw_date,
    return random `count` tickets from candidate_pool for each lottery.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    result = {}
    for lottery_slug, coll_name in [
        ("euromillones", EUROMILLONES_TRAIN_PROGRESS_COLLECTION),
        ("la-primitiva", LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION),
        ("el-gordo", EL_GORDO_TRAIN_PROGRESS_COLLECTION),
    ]:
        last_draw_date = _get_last_draw_date(lottery_slug)
        date_str = (last_draw_date or "").strip()[:10] if last_draw_date else None
        tickets = []
        if date_str:
            coll = db[coll_name]
            doc = coll.find_one({"probs_fecha_sorteo": date_str}, projection=["candidate_pool"])
            pool = doc.get("candidate_pool") or [] if doc else []
            if pool:
                n = min(count, len(pool))
                tickets = list(random.sample(pool, n))
        result[lottery_slug] = {
            "last_draw_date": date_str,
            "tickets": tickets,
        }
    return JSONResponse(
        content=result,
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


def _compute_next_draw_date(lottery_slug: str, last_date_str: str) -> str | None:
    """
    Given the date of the last draw, compute the next draw date for that lottery.

    This approximates the "Próximo sorteo" / closing-of-sales date using only
    the draw dates we already have in the database.
    """
    try:
        d = dt.strptime(last_date_str, "%Y-%m-%d").date()
    except ValueError:
        return None

    wd = d.weekday()  # Monday=0 .. Sunday=6

    if lottery_slug == "euromillones":
        # Euromillones draws Tuesday (1) and Friday (4)
        if wd == 1:  # Tuesday -> next Friday
            delta = 3
        elif wd == 4:  # Friday -> next Tuesday
            delta = 4
        else:
            delta = 1
            while True:
                cand = d + timedelta(days=delta)
                if cand.weekday() in (1, 4):
                    break
                delta += 1
    elif lottery_slug == "la-primitiva":
        # La Primitiva draws Monday (0), Thursday (3) and Saturday (5)
        valid_days = (0, 3, 5)
        if wd in valid_days:
            order = [0, 3, 5]
            idx = order.index(wd)
            next_wd = order[(idx + 1) % len(order)]
            delta = (next_wd - wd) % 7 or 7
        else:
            delta = 1
            while True:
                cand = d + timedelta(days=delta)
                if cand.weekday() in valid_days:
                    break
                delta += 1
    elif lottery_slug == "el-gordo":
        # El Gordo weekly on Sunday (6)
        delta = (6 - wd) % 7 or 7
    else:
        return None

    return (d + timedelta(days=delta)).strftime("%Y-%m-%d")


def _set_last_draw_date(lottery: str, date_str: str) -> None:
    """Set last_draw_date (and next_draw_date) for a lottery in scraper_metadata."""
    if db is None:
        return

    update_doc: dict = {"last_draw_date": date_str}
    next_draw = _compute_next_draw_date(lottery, date_str)
    if next_draw:
        update_doc["next_draw_date"] = next_draw

    db[METADATA_COLLECTION].update_one(
        {"lottery": lottery},
        {"$set": update_doc},
        upsert=True,
    )


def _max_date_from_draws(data: list) -> str | None:
    """Return max fecha_sorteo date (YYYY-MM-DD) from a list of draws."""
    out = None
    for draw in data:
        if not isinstance(draw, dict):
            continue
        f = (draw.get("fecha_sorteo") or "").strip()
        if f:
            d = f.split(" ")[0]
            if d and (out is None or d > out):
                out = d
    return out


def _get_max_draw_date_from_db(game_id: str) -> str | None:
    """Return latest fecha_sorteo date (YYYY-MM-DD) from the DB for a given game_id."""
    if db is None:
        return None
    coll_name = COLLECTIONS.get(game_id)
    if not coll_name:
        return None
    doc = db[coll_name].find_one(sort=[("fecha_sorteo", -1)], projection={"fecha_sorteo": 1})
    if not doc:
        return None
    f = (doc.get("fecha_sorteo") or "").strip()
    if not f:
        return None
    return f.split(" ")[0]


def _save_draws_to_db(data: list) -> tuple[int, list]:
    """Upsert draws into the correct collection (la_primitiva / euromillones / el_gordo)."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    saved = 0
    errors = []
    for draw in data:
        if not isinstance(draw, dict):
            continue
        draw_id = draw.get("id_sorteo")
        game_id = draw.get("game_id")
        if not draw_id or not game_id:
            continue
        coll_name = COLLECTIONS.get(game_id)
        if not coll_name:
            continue
        try:
            doc = normalize_draw(draw)
            db[coll_name].replace_one(
                {"id_sorteo": draw_id},
                doc,
                upsert=True,
            )
            saved += 1
        except PyMongoError as e:
            errors.append(str(e))
    return saved, errors


# game_id -> display name for API responses
GAME_ID_TO_NAME = {"LAPR": "La Primitiva", "EMIL": "Euromillones", "ELGR": "El Gordo"}


def _doc_to_json(doc: dict) -> dict:
    """Convert a MongoDB document to a JSON-serializable dict (all keys from DB)."""
    out = {}
    for k, v in doc.items():
        if k == "_id":
            out[k] = str(v) if isinstance(v, ObjectId) else v
        elif isinstance(v, ObjectId):
            out[k] = str(v)
        elif isinstance(v, dt):
            out[k] = v.isoformat() if hasattr(v, "isoformat") else str(v)
        elif isinstance(v, list):
            out[k] = [_item_to_json(x) for x in v]
        elif isinstance(v, dict):
            out[k] = _doc_to_json(v)
        else:
            out[k] = v
    return out


def _item_to_json(x):
    if isinstance(x, ObjectId):
        return str(x)
    if isinstance(x, dt):
        return x.isoformat() if hasattr(x, "isoformat") else str(x)
    if isinstance(x, dict):
        return _doc_to_json(x)
    if isinstance(x, list):
        return [_item_to_json(i) for i in x]
    return x


# Keys we send to frontend; we always set these from the raw doc so combinacion_acta and escrutinio are never missing
DRAW_KEYS = (
    "id_sorteo",
    "fecha_sorteo",
    "game_id",
    "combinacion",
    "combinacion_acta",
    "numbers",
    "complementario",
    "reintegro",
    "joker_combinacion",
    "premio_bote",
    "escrutinio",
    # Euromillones extra stats (if present in DB)
    "apuestas",  # bets received
    "aquestas",  # fallback name if mis-typed in DB
    "recaudacion",
    "recaudacion_europea",
    "premios",
    "escrutinio_millon",
)


def _build_draw(doc: dict, game_id: str) -> dict:
    """Build one draw for API: always include combinacion_acta and escrutinio from doc."""
    draw = {}
    for k in DRAW_KEYS:
        v = doc.get(k)
        draw[k] = _item_to_json(v) if v is not None else None
    draw["game_id"] = game_id
    draw["game_name"] = GAME_ID_TO_NAME.get(game_id, game_id)
    if draw.get("joker_combinacion") is None:
        millon = doc.get("millon") or {}
        draw["joker_combinacion"] = millon.get("combinacion") if isinstance(millon, dict) else None
    return draw


@app.get("/api/draws")
def get_draws(
    lottery: str = Query(None, description="la-primitiva | euromillones | el-gordo"),
    from_date: str = Query(None, description="YYYY-MM-DD"),
    to_date: str = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
):
    """Fetch draws: use find() then build each draw in Python so combinacion_acta and escrutinio are always returned."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    query = {}
    from_ = (from_date or "").strip()
    to_ = (to_date or "").strip()
    if from_ or to_:
        query["fecha_sorteo"] = {}
        if from_:
            query["fecha_sorteo"]["$gte"] = from_ + " 00:00:00"
        if to_:
            query["fecha_sorteo"]["$lte"] = to_ + " 23:59:59"

    if lottery and lottery in GAME_IDS:
        coll_name = COLLECTIONS.get(GAME_IDS[lottery])
        collections_to_query = [(coll_name, GAME_IDS[lottery])] if coll_name else []
    else:
        collections_to_query = [(name, gid) for gid, name in COLLECTIONS.items()]

    all_draws = []
    total = 0
    one_lottery = len(collections_to_query) == 1

    for coll_name, game_id in collections_to_query:
        cursor = db[coll_name].find(query).sort("fecha_sorteo", -1)
        if one_lottery:
            total = db[coll_name].count_documents(query)
            for doc in cursor.skip(skip).limit(limit):
                all_draws.append(_build_draw(doc, game_id))
        else:
            for doc in cursor:
                total += 1
                all_draws.append(_build_draw(doc, game_id))

    if not one_lottery:
        all_draws.sort(key=lambda d: d.get("fecha_sorteo") or "", reverse=True)
        total = len(all_draws)
        all_draws = all_draws[skip : skip + limit]

    return JSONResponse(content={"draws": all_draws, "total": total})


@app.get("/api/euromillones/draw")
def get_euromillones_draw_by_id(
    draw_id: str | None = Query(None, description="id_sorteo of the draw."),
):
    """Return one Euromillones draw by id_sorteo (escrutinio, combinacion_acta, numbers, main/star)."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if not draw_id or not draw_id.strip():
        raise HTTPException(400, detail="draw_id is required")
    cid = draw_id.strip()
    doc = db["euromillones"].find_one({"id_sorteo": cid})
    if doc is None and cid.isdigit():
        doc = db["euromillones"].find_one({"id_sorteo": int(cid)})
    if not doc:
        raise HTTPException(404, detail="Draw not found")
    draw = _build_draw(doc, "EMIL")
    return JSONResponse(content=draw)


@app.get("/api/el-gordo/draw")
def get_el_gordo_draw_by_id(
    draw_id: str | None = Query(None, description="id_sorteo of the El Gordo draw."),
):
    """Return one El Gordo draw by id_sorteo (escrutinio, combinacion_acta, numbers, main/clave)."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if not draw_id or not draw_id.strip():
        raise HTTPException(400, detail="draw_id is required")
    cid = draw_id.strip()
    coll = db["el_gordo"]
    doc = coll.find_one({"id_sorteo": cid})
    if doc is None and cid.isdigit():
        doc = coll.find_one({"id_sorteo": int(cid)})
    if not doc:
        raise HTTPException(404, detail="Draw not found")
    draw = _build_draw(doc, "ELGR")
    return JSONResponse(content=draw)


@app.get("/api/la-primitiva/draw")
def get_la_primitiva_draw_by_id(
    draw_id: str | None = Query(None, description="id_sorteo of the La Primitiva draw."),
):
    """Return one La Primitiva draw by id_sorteo (escrutinio, combinacion_acta, numbers, complementario, reintegro)."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if not draw_id or not draw_id.strip():
        raise HTTPException(400, detail="draw_id is required")
    cid = draw_id.strip()
    coll = db["la_primitiva"]
    doc = coll.find_one({"id_sorteo": cid})
    if doc is None and cid.isdigit():
        doc = coll.find_one({"id_sorteo": int(cid)})
    if not doc:
        raise HTTPException(404, detail="Draw not found")
    draw = _build_draw(doc, "LAPR")
    return JSONResponse(content=draw)


@app.get("/api/debug/euromillones-one")
def debug_euromillones_one():
    """Return one raw document from euromillones to verify combinacion_acta and escrutinio."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    doc = db["euromillones"].find_one(sort=[("fecha_sorteo", -1)])
    if not doc:
        return JSONResponse(content={"error": "No document in euromillones"})
    return JSONResponse(content=_doc_to_json(doc))


@app.get("/api/euromillones/features")
def get_euromillones_features(
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
    draw_id: str | None = Query(
        None,
        description="Optional: filter by draw_id to get a specific feature row.",
    ),
):
    """
    Return per-draw Euromillones feature rows from `euromillones_draw_features`.

    Each document contains:
      - main_numbers, star_numbers
      - draw_date, weekday
      - hot/cold numbers
      - frequency and gap arrays
      - previous-draw snapshot fields
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["euromillones_draw_features"]

    if draw_id:
        cursor = coll.find({"draw_id": draw_id})
        docs = [_doc_to_json(doc) for doc in cursor]
        total = len(docs)
        return JSONResponse(content={"features": docs, "total": total})

    total = coll.count_documents({})
    cursor = coll.find().sort("draw_date", -1).skip(skip).limit(limit)
    docs = [_doc_to_json(doc) for doc in cursor]

    return JSONResponse(content={"features": docs, "total": total})


@app.get("/api/euromillones/feature-model")
def get_euromillones_feature_model(
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
    draw_id: str | None = Query(
        None,
        description="Optional: filter by id_sorteo to get one feature row.",
    ),
):
    """
    Return rows from `euromillones_feature` (new feature model).
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["euromillones_feature"]

    if draw_id:
        cursor = coll.find({"id_sorteo": draw_id})
        docs = [_doc_to_json(doc) for doc in cursor]
        return JSONResponse(content={"features": docs, "total": len(docs)})

    total = coll.count_documents({})
    cursor = coll.find().sort("fecha_sorteo", -1).skip(skip).limit(limit)
    docs = [_doc_to_json(doc) for doc in cursor]
    return JSONResponse(content={"features": docs, "total": total})


@app.get("/api/euromillones/train/progress")
def api_euromillones_train_progress(
    cutoff_draw_id: str | None = Query(None, description="id_sorteo for this training run."),
):
    """Return training progress for the given cutoff_draw_id (dataset prepared, models trained)."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if not cutoff_draw_id:
        return JSONResponse(
            content={"progress": None},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    coll = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
    doc = coll.find_one({"cutoff_draw_id": cutoff_draw_id.strip()})
    if not doc:
        return JSONResponse(
            content={"progress": None},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    # Compute full wheel elapsed seconds (server-side)
    full_status = doc.get("full_wheel_status")
    started_at = doc.get("full_wheel_started_at")
    generated_at = doc.get("full_wheel_generated_at")
    full_elapsed: Optional[int] = None
    if started_at:
        try:
            start_dt = dt.fromisoformat(str(started_at).replace("Z", "+00:00"))
            if full_status == "done" and generated_at:
                end_dt = dt.fromisoformat(str(generated_at).replace("Z", "+00:00"))
            else:
                end_dt = dt.utcnow()
            diff = end_dt - start_dt
            full_elapsed = max(0, int(diff.total_seconds()))
        except Exception:
            full_elapsed = None

    progress = {
        "cutoff_draw_id": doc.get("cutoff_draw_id"),
        "dataset_prepared": bool(doc.get("dataset_prepared")),
        "dataset_prepared_at": doc.get("dataset_prepared_at"),
        "main_rows": doc.get("main_rows"),
        "star_rows": doc.get("star_rows"),
        "models_trained": bool(doc.get("models_trained")),
        "trained_at": doc.get("trained_at"),
        "main_accuracy": doc.get("main_accuracy"),
        "star_accuracy": doc.get("star_accuracy"),
        "probs_computed": bool(doc.get("probs_computed")),
        "probs_computed_at": doc.get("probs_computed_at"),
        "mains_probs": doc.get("mains_probs"),
        "stars_probs": doc.get("stars_probs"),
        "probs_draw_id": doc.get("probs_draw_id"),
        "probs_fecha_sorteo": doc.get("probs_fecha_sorteo"),
        "rules_applied": bool(doc.get("rules_applied")),
        "rules_applied_at": doc.get("rules_applied_at"),
        "filtered_mains_probs": doc.get("filtered_mains_probs"),
        "filtered_stars_probs": doc.get("filtered_stars_probs"),
        "rule_flags": doc.get("rule_flags"),
        "generated_30_mains": doc.get("generated_30_mains"),
        "generated_30_mains_at": doc.get("generated_30_mains_at"),
        "candidate_pool": doc.get("candidate_pool"),
        "candidate_pool_at": doc.get("candidate_pool_at"),
        "candidate_pool_count": doc.get("candidate_pool_count"),
        "bought_tickets": doc.get("bought_tickets"),
        "full_wheel_draw_date": doc.get("full_wheel_draw_date"),
        "full_wheel_file_path": doc.get("full_wheel_file_path"),
        "full_wheel_total_tickets": doc.get("full_wheel_total_tickets"),
        "full_wheel_good_tickets": doc.get("full_wheel_good_tickets"),
        "full_wheel_bad_tickets": doc.get("full_wheel_bad_tickets"),
        "full_wheel_generated_at": doc.get("full_wheel_generated_at"),
        "full_wheel_started_at": doc.get("full_wheel_started_at"),
        "full_wheel_status": full_status,
        "full_wheel_error": doc.get("full_wheel_error"),
        "full_wheel_elapsed_seconds": full_elapsed,
        "pipeline_status": doc.get("pipeline_status"),
        "pipeline_error": doc.get("pipeline_error"),
        "pipeline_started_at": doc.get("pipeline_started_at"),
    }
    return JSONResponse(
        content={"progress": progress},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.post("/api/euromillones/train/prepare-dataset")
def api_euromillones_prepare_dataset(
    cutoff_draw_id: str | None = Query(
        None,
        description="Optional id_sorteo: only draws up to this one (inclusive) are used to build the dataset.",
    ),
):
    """
    Build / refresh the Euromillones per-number training datasets from
    `euromillones_feature`.

    This calls `scripts/train_euromillones_model.prepare_euromillones_dataset`
    inside the backend process and returns basic metadata so the UI can show
    where the CSV files were written. Saves progress to euromillones_train_progress.
    """
    try:
        info = prepare_euromillones_dataset(cutoff_draw_id=cutoff_draw_id, out_dir=None)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error while preparing Euromillones dataset: {e!s}",
        )
    if db is not None and cutoff_draw_id:
        coll = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
        now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        coll.update_one(
            {"cutoff_draw_id": cutoff_draw_id.strip()},
            {
                "$set": {
                    "cutoff_draw_id": cutoff_draw_id.strip(),
                    "dataset_prepared": True,
                    "dataset_prepared_at": now,
                    "main_rows": info.get("main_rows"),
                    "star_rows": info.get("star_rows"),
                }
            },
            upsert=True,
        )
    return JSONResponse(content={"status": "ok", "info": info})


@app.get("/api/el-gordo/train/progress")
def api_el_gordo_train_progress(
    cutoff_draw_id: str | None = Query(None, description="id_sorteo for this training run (El Gordo)."),
):
    """Return El Gordo training progress (mirror Euromillones: dataset, models, probs, rules, candidate_pool, full_wheel_*)."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if not cutoff_draw_id:
        return JSONResponse(
            content={"progress": None},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    coll = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
    doc = coll.find_one({"cutoff_draw_id": cutoff_draw_id.strip()})
    if not doc:
        return JSONResponse(
            content={"progress": None},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    # Full wheel elapsed seconds (mirror Euromillones)
    full_status = doc.get("full_wheel_status")
    started_at = doc.get("full_wheel_started_at")
    generated_at = doc.get("full_wheel_generated_at")
    full_elapsed: Optional[int] = None
    if started_at:
        try:
            start_dt = dt.fromisoformat(str(started_at).replace("Z", "+00:00"))
            if full_status == "done" and generated_at:
                end_dt = dt.fromisoformat(str(generated_at).replace("Z", "+00:00"))
            else:
                end_dt = dt.utcnow()
            diff = end_dt - start_dt
            full_elapsed = max(0, int(diff.total_seconds()))
        except Exception:
            full_elapsed = None

    progress = {
        "cutoff_draw_id": doc.get("cutoff_draw_id"),
        "dataset_prepared": bool(doc.get("dataset_prepared")),
        "dataset_prepared_at": doc.get("dataset_prepared_at"),
        "main_rows": doc.get("main_rows"),
        "clave_rows": doc.get("clave_rows"),
        "models_trained": bool(doc.get("models_trained")),
        "trained_at": doc.get("trained_at"),
        "main_accuracy": doc.get("main_accuracy"),
        "clave_accuracy": doc.get("clave_accuracy"),
        "probs_computed": bool(doc.get("probs_computed")),
        "probs_computed_at": doc.get("probs_computed_at"),
        "mains_probs": doc.get("mains_probs"),
        "clave_probs": doc.get("clave_probs"),
        "probs_draw_id": doc.get("probs_draw_id"),
        "probs_fecha_sorteo": doc.get("probs_fecha_sorteo"),
        "rules_applied": bool(doc.get("rules_applied")),
        "rules_applied_at": doc.get("rules_applied_at"),
        "filtered_mains_probs": doc.get("filtered_mains_probs"),
        "filtered_clave_probs": doc.get("filtered_clave_probs"),
        "rule_flags": doc.get("rule_flags"),
        "candidate_pool": doc.get("candidate_pool"),
        "candidate_pool_at": doc.get("candidate_pool_at"),
        "candidate_pool_count": doc.get("candidate_pool_count"),
        "bought_tickets": doc.get("bought_tickets"),
        "full_wheel_draw_date": doc.get("full_wheel_draw_date"),
        "full_wheel_file_path": doc.get("full_wheel_file_path"),
        "full_wheel_total_tickets": doc.get("full_wheel_total_tickets"),
        "full_wheel_good_tickets": doc.get("full_wheel_good_tickets"),
        "full_wheel_bad_tickets": doc.get("full_wheel_bad_tickets"),
        "full_wheel_generated_at": doc.get("full_wheel_generated_at"),
        "full_wheel_started_at": doc.get("full_wheel_started_at"),
        "full_wheel_status": full_status,
        "full_wheel_error": doc.get("full_wheel_error"),
        "full_wheel_elapsed_seconds": full_elapsed,
        "pipeline_status": doc.get("pipeline_status"),
        "pipeline_error": doc.get("pipeline_error"),
        "pipeline_started_at": doc.get("pipeline_started_at"),
    }
    return JSONResponse(
        content={"progress": progress},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/api/el-gordo/betting/last-id")
def api_el_gordo_betting_last_id():
    """Return cutoff_draw_id of the last el_gordo_train_progress doc by probs_fecha_sorteo (desc) then _id (desc)."""
    if db is None:
        return JSONResponse(content={"cutoff_draw_id": None})
    coll = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
    doc = coll.find_one(
        projection={"cutoff_draw_id": 1, "probs_fecha_sorteo": 1},
        sort=[("probs_fecha_sorteo", -1), ("_id", -1)],
    )
    cid = doc.get("cutoff_draw_id") if doc else None
    return JSONResponse(
        content={"cutoff_draw_id": str(cid) if cid is not None else None},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/api/el-gordo/betting/last-draw-date")
def api_el_gordo_betting_last_draw_date():
    """Return last_draw_date for el-gordo from scraper_metadata. Betting tab uses this to find train_progress by probs_fecha_sorteo."""
    if db is None:
        return JSONResponse(content={"last_draw_date": None})
    last_draw_date = _get_last_draw_date("el-gordo")
    return JSONResponse(
        content={"last_draw_date": last_draw_date},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/api/el-gordo/betting/pool")
def api_el_gordo_betting_pool(
    draw_date: str | None = Query(None, description="Draw date YYYY-MM-DD; find train_progress by probs_fecha_sorteo (from scraper_metadata last_draw_date)."),
    cutoff_draw_id: str | None = Query(None, description="Optional id_sorteo; used when draw_date not provided."),
):
    """
    Return candidate pool for betting. Prefer draw_date (from scraper_metadata): find *train_progress by probs_fecha_sorteo === draw_date.
    Else use cutoff_draw_id; else use last_draw_date from scraper_metadata.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    coll = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
    date_str = (draw_date or "").strip()[:10] or None
    cutoff = (cutoff_draw_id or "").strip() or None
    if not date_str and not cutoff:
        date_str = (_get_last_draw_date("el-gordo") or "").strip()[:10] or None
    if date_str:
        doc = coll.find_one({"probs_fecha_sorteo": date_str})
        if not doc:
            return JSONResponse(
                content={
                    "last_draw_date": date_str,
                    "cutoff_draw_id": None,
                    "candidate_pool": [],
                    "candidate_pool_count": 0,
                    "bought_tickets": [],
                },
                headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
            )
    elif cutoff:
        doc = coll.find_one({"cutoff_draw_id": cutoff})
        if doc is None and cutoff.isdigit():
            doc = coll.find_one({"cutoff_draw_id": int(cutoff)})
        if not doc:
            return JSONResponse(
                content={
                    "last_draw_date": None,
                    "cutoff_draw_id": cutoff,
                    "candidate_pool": [],
                    "candidate_pool_count": 0,
                    "bought_tickets": [],
                },
                headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
            )
    else:
        return JSONResponse(
            content={
                "last_draw_date": None,
                "cutoff_draw_id": None,
                "candidate_pool": [],
                "candidate_pool_count": 0,
                "bought_tickets": [],
            },
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    pool = doc.get("candidate_pool") or []
    bought = doc.get("bought_tickets") or []
    return JSONResponse(
        content={
            "last_draw_date": (doc.get("probs_fecha_sorteo") or "").strip()[:10] or None,
            "cutoff_draw_id": doc.get("cutoff_draw_id"),
            "candidate_pool": pool,
            "candidate_pool_count": len(pool),
            "bought_tickets": bought,
        },
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/api/el-gordo/betting/pool-from-file")
def api_el_gordo_betting_pool_from_file(
    draw_date: str | None = Query(
        None,
        description="Draw date YYYY-MM-DD; use full_wheel_file_path for that date.",
    ),
    cutoff_draw_id: str | None = Query(
        None,
        description="Optional id_sorteo; used when draw_date not provided.",
    ),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
):
    """
    Return a paginated slice of the El Gordo full-wheeling pool from TXT (mirror Euromillones).

    Uses skip/limit on the file lines. Each line: position;mains_csv;clave.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
    date_str = (draw_date or "").strip()[:10] or None
    cutoff = (cutoff_draw_id or "").strip() or None

    if not date_str and not cutoff:
        last = (_get_last_draw_date("el-gordo") or "").strip()
        if last:
            date_str = last[:10]

    doc = None
    if cutoff:
        doc = coll.find_one({"cutoff_draw_id": cutoff})
        if doc is None and cutoff.isdigit():
            doc = coll.find_one({"cutoff_draw_id": int(cutoff)})
    if doc is None and date_str:
        doc = coll.find_one({"full_wheel_draw_date": date_str}) or coll.find_one({"probs_fecha_sorteo": date_str})
    if doc is None:
        return JSONResponse(
            content={
                "draw_date": date_str,
                "cutoff_draw_id": cutoff,
                "total": 0,
                "skip": skip,
                "limit": limit,
                "tickets": [],
            },
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )

    path = (doc.get("full_wheel_file_path") or "").strip()
    total = int(doc.get("full_wheel_total_tickets") or 0)

    if not path or total <= 0:
        return JSONResponse(
            content={
                "draw_date": date_str,
                "cutoff_draw_id": cutoff,
                "total": total,
                "skip": skip,
                "limit": limit,
                "tickets": [],
            },
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )

    tickets_list: List[Dict] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for _ in range(skip):
                if not f.readline():
                    break
            for _ in range(limit):
                line = f.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    pos_str, mains_str, clave_str = line.split(";")
                    position = int(pos_str)
                    mains = [int(x) for x in mains_str.split(",") if x]
                    clave = int(clave_str)
                except Exception:
                    continue
                tickets_list.append({"position": position, "mains": mains, "clave": clave})
    except FileNotFoundError:
        raise HTTPException(404, detail="Full wheel file not found on disk")

    return JSONResponse(
        content={
            "draw_date": (doc.get("full_wheel_draw_date") or date_str),
            "cutoff_draw_id": doc.get("cutoff_draw_id") or cutoff,
            "total": total,
            "skip": skip,
            "limit": limit,
            "tickets": tickets_list,
        },
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.post("/api/el-gordo/betting/bought")
async def api_el_gordo_betting_bought(request: Request):
    """
    Save bought tickets into el_gordo_train_progress. Body may include optional "cutoff_draw_id" (same as La Primitiva).
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    try:
        body = await request.json() or {}
    except Exception:
        body = {}
    tickets = body.get("tickets")
    if not isinstance(tickets, list):
        raise HTTPException(400, detail="Body must contain 'tickets' array")
    draw_date = (body.get("draw_date") or "").strip()[:10] or None
    cutoff = (body.get("cutoff_draw_id") or "").strip() or None
    coll = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
    if draw_date:
        result = coll.update_one(
            {"probs_fecha_sorteo": draw_date},
            {"$set": {"bought_tickets": tickets, "bought_tickets_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}},
        )
    elif cutoff:
        result = coll.update_one(
            {"cutoff_draw_id": cutoff},
            {"$set": {"bought_tickets": tickets, "bought_tickets_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}},
        )
        if result.matched_count == 0 and cutoff.isdigit():
            result = coll.update_one(
                {"cutoff_draw_id": int(cutoff)},
                {"$set": {"bought_tickets": tickets, "bought_tickets_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}},
            )
    else:
        last_draw_date = _get_last_draw_date("el-gordo")
        if not last_draw_date:
            raise HTTPException(400, detail="No last_draw_date for el-gordo")
        date_str = (last_draw_date or "").strip()[:10]
        result = coll.update_one(
            {"probs_fecha_sorteo": date_str},
            {"$set": {"bought_tickets": tickets, "bought_tickets_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}},
        )
    if result.matched_count == 0:
        raise HTTPException(404, detail="No progress found for draw_date or cutoff_draw_id")
    return JSONResponse(
        content={"status": "ok", "saved_count": len(tickets)},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.post("/api/el-gordo/betting/open-real-platform")
async def api_el_gordo_betting_open_real_platform(request: Request):
    """
    Start Selenium bot; fill bucket tickets on loteriasyapuestas.es (see scripts/el_gordo_real_platform_bot.py).
    Headless Chrome fills the form and clicks JUEGA but cannot complete payment; the site usually
    shows a payment step after JUEGA, so the bot rarely reaches the success page. When success
    is detected, tickets are added to bought_tickets; otherwise use "Añadir a guardados" after buying.
    Body: { "tickets": [...], "draw_date"?: "YYYY-MM-DD", "cutoff_draw_id"?: "..." } (max 6 tickets).
    """
    try:
        body = await request.json() or {}
    except Exception:
        body = {}
    tickets = body.get("tickets")
    if not isinstance(tickets, list) or len(tickets) == 0 or len(tickets) > 6:
        raise HTTPException(400, detail="Body must contain 'tickets' array with 1–6 items")
    normalized = []
    for t in tickets:
        mains = t.get("mains")
        if not isinstance(mains, list) or len(mains) != 5:
            raise HTTPException(400, detail="Each ticket must have 'mains' array of 5 numbers")
        clave = t.get("clave", 0)
        try:
            clave = int(clave)
            if not (0 <= clave <= 9):
                raise ValueError("clave must be 0–9")
        except (TypeError, ValueError):
            raise HTTPException(400, detail="Each ticket must have 'clave' 0–9")
        normalized.append({"mains": [int(m) for m in mains], "clave": clave})
    draw_date = (body.get("draw_date") or "").strip()[:10] or None
    cutoff_draw_id = (body.get("cutoff_draw_id") or "").strip() or None
    thread = threading.Thread(
        target=_run_el_gordo_real_platform_bot,
        args=(normalized, draw_date, cutoff_draw_id),
        daemon=True,
    )
    thread.start()
    return JSONResponse(
        content={"status": "ok", "message": "Chrome abierto. Completa el login y pago en el navegador.", "tickets_count": len(normalized)},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/api/el-gordo/betting/bot-progress")
def api_el_gordo_betting_bot_progress():
    """Return current El Gordo bot progress for UI polling. status: idle | running | success | error."""
    out = {k: v for k, v in _el_gordo_bot_progress.items() if k != "last_tickets"}
    out["has_pending_confirm"] = bool(_el_gordo_bot_progress.get("last_tickets"))
    return JSONResponse(
        content=out,
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.post("/api/el-gordo/betting/confirm-bot-bought")
def api_el_gordo_betting_confirm_bot_bought():
    """
    User confirms they completed the purchase on the real site. Appends last bot-run tickets to bought_tickets.
    Call this only after the user has actually bought the tickets on Loterías.
    """
    progress = _el_gordo_bot_progress
    tickets = progress.get("last_tickets")
    if not tickets:
        raise HTTPException(400, detail="No hay boletos pendientes de confirmar. Ejecuta el bot primero.")
    draw_date = progress.get("last_draw_date")
    cutoff_draw_id = progress.get("last_cutoff_draw_id")
    _append_el_gordo_bought_tickets(tickets, draw_date=draw_date, cutoff_draw_id=cutoff_draw_id)
    progress["last_tickets"] = None
    progress["last_draw_date"] = None
    progress["last_cutoff_draw_id"] = None
    return JSONResponse(
        content={"status": "ok", "saved_count": len(tickets)},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.post("/api/el-gordo/betting/enqueue")
async def api_el_gordo_betting_enqueue(request: Request):
    """
    Save bucket tickets to buy queue (status=waiting). Background worker will pick up and run the bot.
    Body: { "tickets": [...], "draw_date"?: "YYYY-MM-DD", "cutoff_draw_id"?: "..." } (max 6 tickets).
    """
    try:
        body = await request.json() or {}
    except Exception:
        body = {}
    tickets = body.get("tickets")
    if not isinstance(tickets, list) or len(tickets) == 0 or len(tickets) > 6:
        raise HTTPException(400, detail="Body must contain 'tickets' array with 1–6 items")
    normalized = []
    for t in tickets:
        mains = t.get("mains")
        if not isinstance(mains, list) or len(mains) != 5:
            raise HTTPException(400, detail="Each ticket must have 'mains' array of 5 numbers")
        clave = t.get("clave", 0)
        try:
            clave = int(clave)
            if not (0 <= clave <= 9):
                raise ValueError("clave must be 0–9")
        except (TypeError, ValueError):
            raise HTTPException(400, detail="Each ticket must have 'clave' 0–9")
        normalized.append({"mains": [int(m) for m in mains], "clave": clave})
    draw_date = (body.get("draw_date") or "").strip()[:10] or None
    cutoff_draw_id = (body.get("cutoff_draw_id") or "").strip() or None
    if db is None:
        raise HTTPException(503, detail="Database not available")
    coll = db[EL_GORDO_BUY_QUEUE_COLLECTION]
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    doc = {
        "lottery": "el-gordo",
        "tickets": normalized,
        "tickets_count": len(normalized),
        "draw_date": draw_date,
        "cutoff_draw_id": cutoff_draw_id,
        "status": "waiting",
        "created_at": now,
    }
    ins = coll.insert_one(doc)
    return JSONResponse(
        content={"status": "ok", "queue_id": str(ins.inserted_id), "tickets_count": len(normalized), "message": "Añadido a la cola. El bot comprará en breve."},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/api/el-gordo/betting/buy-queue")
def api_el_gordo_betting_buy_queue(limit: int = Query(50, ge=1, le=100)):
    """Return recent El Gordo buy-queue items (status, tickets for hover). Tickets included so UI can exclude them from pool and show on hover."""
    if db is None:
        return JSONResponse(content={"items": []}, headers={"Cache-Control": "no-store, no-cache, must-revalidate"})
    coll = db[EL_GORDO_BUY_QUEUE_COLLECTION]
    cursor = coll.find({}).sort("created_at", -1).limit(limit)
    items = []
    for d in cursor:
        items.append({
            "id": str(d["_id"]),
            "lottery": d.get("lottery", "el-gordo"),
            "status": d.get("status", "waiting"),
            "saved_status": d.get("saved_status"),
            "tickets_count": d.get("tickets_count", 0),
            "tickets": d.get("tickets") or [],
            "draw_date": d.get("draw_date"),
            "cutoff_draw_id": d.get("cutoff_draw_id"),
            "created_at": d.get("created_at"),
            "started_at": d.get("started_at"),
            "finished_at": d.get("finished_at"),
            "error": d.get("error"),
        })
    return JSONResponse(
        content={"items": items},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.post("/api/el-gordo/betting/save-bought-from-queue")
def api_el_gordo_betting_save_bought_from_queue():
    """
    Find queue items with status=bought and saved_status not True; for each append tickets
    to el_gordo_train_progress.bought_tickets (using draw_date/cutoff_draw_id to find the doc),
    then set saved_status=True. Called every 8s from frontend so bought tickets appear in Boletos guardados.
    """
    if db is None:
        return JSONResponse(
            content={"status": "ok", "saved_count": 0, "message": "Database not connected"},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    coll = db[EL_GORDO_BUY_QUEUE_COLLECTION]
    cursor = coll.find({
        "status": "bought",
        "$or": [{"saved_status": {"$ne": True}}, {"saved_status": {"$exists": False}}],
    })
    saved_count = 0
    for d in cursor:
        oid = d["_id"]
        tickets = d.get("tickets") or []
        if not tickets:
            coll.update_one({"_id": oid}, {"$set": {"saved_status": True}})
            saved_count += 1
            continue
        draw_date = (d.get("draw_date") or "").strip()[:10] or None
        cutoff_draw_id = (d.get("cutoff_draw_id") or "").strip() or None
        _append_el_gordo_bought_tickets(tickets, draw_date=draw_date, cutoff_draw_id=cutoff_draw_id)
        coll.update_one({"_id": oid}, {"$set": {"saved_status": True}})
        saved_count += 1
    return JSONResponse(
        content={"status": "ok", "saved_count": saved_count},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.post("/api/el-gordo/betting/bot/claim")
def api_el_gordo_betting_bot_claim():
    """
    For the bot running on another device (no DB access). Claims one waiting queue item:
    sets status to in_progress and returns it. Bot uses this instead of reading DB.
    """
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    coll = db[EL_GORDO_BUY_QUEUE_COLLECTION]
    doc = coll.find_one({"status": "waiting"}, sort=[("created_at", 1)])
    if not doc:
        return JSONResponse(
            content={"claimed": False, "queue_id": None, "tickets": [], "draw_date": None, "cutoff_draw_id": None},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    oid = doc["_id"]
    updated = coll.update_one(
        {"_id": oid, "status": "waiting"},
        {"$set": {"status": "in_progress", "started_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}},
    )
    if updated.modified_count == 0:
        return JSONResponse(
            content={"claimed": False, "queue_id": None, "tickets": [], "draw_date": None, "cutoff_draw_id": None},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    tickets = doc.get("tickets") or []
    draw_date = (doc.get("draw_date") or "").strip()[:10] or None
    cutoff_draw_id = (doc.get("cutoff_draw_id") or "").strip() or None
    return JSONResponse(
        content={
            "claimed": True,
            "queue_id": str(oid),
            "tickets": tickets,
            "draw_date": draw_date,
            "cutoff_draw_id": cutoff_draw_id,
        },
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.post("/api/el-gordo/betting/bot/complete")
async def api_el_gordo_betting_bot_complete(request: Request):
    """
    For the bot running on another device. Report result of a claimed job.
    Body: { "queue_id": "...", "success": true|false, "error"?: "..." }.
    Sets queue doc to bought (saved_status false) or failed.
    """
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    try:
        body = await request.json() or {}
    except Exception:
        body = {}
    queue_id = body.get("queue_id")
    success = body.get("success") is True
    error = (body.get("error") or "").strip() or None
    if not queue_id:
        raise HTTPException(400, detail="queue_id required")
    try:
        oid = ObjectId(queue_id)
    except Exception:
        raise HTTPException(400, detail="Invalid queue_id")
    coll = db[EL_GORDO_BUY_QUEUE_COLLECTION]
    doc = coll.find_one({"_id": oid}, projection={"status": 1})
    if not doc:
        raise HTTPException(404, detail="Queue item not found")
    if doc.get("status") != "in_progress":
        return JSONResponse(
            content={"status": "ok", "message": "Job already completed or not in progress"},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    if success:
        coll.update_one(
            {"_id": oid},
            {"$set": {"status": "bought", "saved_status": False, "finished_at": now, "error": None}},
        )
    else:
        coll.update_one(
            {"_id": oid},
            {"$set": {"status": "failed", "error": error or "Bot reported failure", "finished_at": now}},
        )
    return JSONResponse(
        content={"status": "ok", "message": "completed"},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.delete("/api/el-gordo/betting/buy-queue/{queue_id}")
def api_el_gordo_betting_buy_queue_delete(queue_id: str):
    """Cancel/remove a queue item. Allowed when status is 'waiting' or 'failed' (not in_progress or bought)."""
    if db is None:
        raise HTTPException(status_code=500, detail="Database not connected")
    try:
        oid = ObjectId(queue_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid queue_id")
    coll = db[EL_GORDO_BUY_QUEUE_COLLECTION]
    doc = coll.find_one({"_id": oid}, projection={"status": 1})
    if not doc:
        raise HTTPException(status_code=404, detail="Queue item not found")
    st = doc.get("status")
    if st not in ("waiting", "failed"):
        raise HTTPException(status_code=400, detail="Solo se puede eliminar cuando está en cola (waiting) o ha fallado (failed)")
    coll.delete_one({"_id": oid, "status": {"$in": ["waiting", "failed"]}})
    return JSONResponse(
        content={"status": "ok", "message": "Eliminado de la cola"},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/api/euromillones/betting/last-id")
def api_euromillones_betting_last_id():
    """Return cutoff_draw_id of the last euromillones_train_progress doc by probs_fecha_sorteo (desc) then _id (desc)."""
    if db is None:
        return JSONResponse(content={"cutoff_draw_id": None})
    coll = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
    doc = coll.find_one(
        projection={"cutoff_draw_id": 1, "probs_fecha_sorteo": 1},
        sort=[("probs_fecha_sorteo", -1), ("_id", -1)],
    )
    cid = doc.get("cutoff_draw_id") if doc else None
    return JSONResponse(
        content={"cutoff_draw_id": str(cid) if cid is not None else None},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/api/euromillones/betting/last-draw-date")
def api_euromillones_betting_last_draw_date():
    """Return last_draw_date for euromillones from scraper_metadata. Betting tab uses this to find train_progress by probs_fecha_sorteo."""
    if db is None:
        return JSONResponse(content={"last_draw_date": None})
    last_draw_date = _get_last_draw_date("euromillones")
    return JSONResponse(
        content={"last_draw_date": last_draw_date},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/api/euromillones/betting/pool")
def api_euromillones_betting_pool(
    draw_date: str | None = Query(None, description="Draw date YYYY-MM-DD; find train_progress by probs_fecha_sorteo (from scraper_metadata last_draw_date)."),
    cutoff_draw_id: str | None = Query(None, description="Optional id_sorteo; used when draw_date not provided."),
):
    """
    Return candidate pool for betting. Prefer draw_date (from scraper_metadata): find *train_progress by probs_fecha_sorteo === draw_date.
    Else use cutoff_draw_id; else use last_draw_date from scraper_metadata.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    coll = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
    date_str = (draw_date or "").strip()[:10] or None
    cutoff = (cutoff_draw_id or "").strip() or None
    if not date_str and not cutoff:
        date_str = (_get_last_draw_date("euromillones") or "").strip()[:10] or None
    if date_str:
        doc = coll.find_one({"probs_fecha_sorteo": date_str})
        if not doc:
            return JSONResponse(
                content={
                    "last_draw_date": date_str,
                    "cutoff_draw_id": None,
                    "candidate_pool": [],
                    "candidate_pool_count": 0,
                    "bought_tickets": [],
                },
                headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
            )
    elif cutoff:
        doc = coll.find_one({"cutoff_draw_id": cutoff})
        if doc is None and cutoff.isdigit():
            doc = coll.find_one({"cutoff_draw_id": int(cutoff)})
        if not doc:
            return JSONResponse(
                content={
                    "last_draw_date": None,
                    "cutoff_draw_id": cutoff,
                    "candidate_pool": [],
                    "candidate_pool_count": 0,
                    "bought_tickets": [],
                },
                headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
            )
    else:
        return JSONResponse(
            content={
                "last_draw_date": None,
                "cutoff_draw_id": None,
                "candidate_pool": [],
                "candidate_pool_count": 0,
                "bought_tickets": [],
            },
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    pool = doc.get("candidate_pool") or []
    bought = doc.get("bought_tickets") or []
    return JSONResponse(
        content={
            "last_draw_date": (doc.get("probs_fecha_sorteo") or "").strip()[:10] or None,
            "cutoff_draw_id": doc.get("cutoff_draw_id"),
            "candidate_pool": pool,
            "candidate_pool_count": len(pool),
            "bought_tickets": bought,
        },
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/api/euromillones/betting/pool-from-file")
def api_euromillones_betting_pool_from_file(
    draw_date: str | None = Query(
        None,
        description="Draw date YYYY-MM-DD; use full_wheel_file_path for that date.",
    ),
    cutoff_draw_id: str | None = Query(
        None,
        description="Optional id_sorteo; used when draw_date not provided.",
    ),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
):
    """
    Return a paginated slice of the Euromillones full-wheeling candidate pool from TXT.

    This is used on the Apuestas screen. It does NOT return bought tickets; those
    continue to come from api_euromillones_betting_pool.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
    date_str = (draw_date or "").strip()[:10] or None
    cutoff = (cutoff_draw_id or "").strip() or None

    # Fallback to last_draw_date if neither provided.
    if not date_str and not cutoff:
        last = (_get_last_draw_date("euromillones") or "").strip()
        if last:
            date_str = last[:10]

    doc = None
    if cutoff:
        doc = coll.find_one({"cutoff_draw_id": cutoff})
        if doc is None and cutoff.isdigit():
            doc = coll.find_one({"cutoff_draw_id": int(cutoff)})
    if doc is None and date_str:
        # Prefer full_wheel_draw_date if present, else fall back to probs_fecha_sorteo.
        doc = coll.find_one({"full_wheel_draw_date": date_str}) or coll.find_one({"probs_fecha_sorteo": date_str})
    if doc is None:
        return JSONResponse(
            content={
                "draw_date": date_str,
                "cutoff_draw_id": cutoff,
                "total": 0,
                "skip": skip,
                "limit": limit,
                "tickets": [],
            },
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )

    path = (doc.get("full_wheel_file_path") or "").strip()
    total = int(doc.get("full_wheel_total_tickets") or 0)

    # If full wheel has not been generated yet, return empty list.
    if not path or total <= 0:
        return JSONResponse(
            content={
                "draw_date": date_str,
                "cutoff_draw_id": cutoff,
                "total": total,
                "skip": skip,
                "limit": limit,
                "tickets": [],
            },
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )

    tickets: List[Dict] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            # Skip first `skip` lines
            for _ in range(skip):
                if not f.readline():
                    break
            # Read up to `limit` lines
            for _ in range(limit):
                line = f.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    pos_str, mains_str, stars_str = line.split(";")
                    position = int(pos_str)
                    mains = [int(x) for x in mains_str.split(",") if x]
                    stars = [int(x) for x in stars_str.split(",") if x]
                except Exception:
                    continue
                tickets.append({"position": position, "mains": mains, "stars": stars})
    except FileNotFoundError:
        raise HTTPException(404, detail="Full wheel file not found on disk")

    return JSONResponse(
        content={
            "draw_date": (doc.get("full_wheel_draw_date") or date_str),
            "cutoff_draw_id": doc.get("cutoff_draw_id") or cutoff,
            "total": total,
            "skip": skip,
            "limit": limit,
            "tickets": tickets,
        },
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


def _parse_euro_premio(value) -> float:
    """Parse Euromillones escrutinio premio (e.g. '20.900.000.000' or '5,14') to euros."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace(".", "").replace(",", ".").strip()
        cleaned = re.sub(r"[^\d.]", "", cleaned)
        try:
            return float(cleaned) if cleaned else 0.0
        except ValueError:
            return 0.0
    return 0.0


def _build_escrutinio_prize_map(escrutinio: list) -> Dict[Tuple[int, int], float]:
    """
    Build (main_hits, star_hits) -> prize (euros) from Euromillones escrutinio rows.
    Rows have categoria (1-13), tipo (e.g. '1a' or '5 + 2'), premio (prize per ticket).
    Uses premio from each row to get cost per category for earning calculation.
    """
    prize_map: Dict[Tuple[int, int], float] = {}
    if not isinstance(escrutinio, list):
        return prize_map
    for row in escrutinio:
        if not isinstance(row, dict):
            continue
        premio = _parse_euro_premio(row.get("premio"))
        if premio < 0:
            continue
        # Try "5 + 2" format in tipo or aciertos
        aciertos = str(row.get("tipo") or row.get("aciertos") or "").strip()
        m = re.match(r"(\d+)\s*\+\s*(\d+)", aciertos)
        if m:
            hm, hs = int(m.group(1)), int(m.group(2))
            prize_map[(hm, hs)] = premio
            continue
        # Else use categoria (1-13) -> canonical (main, star) order: 1=5+2, 2=5+1, ... 13=2+0
        cat = row.get("categoria")
        if cat is not None:
            try:
                idx = int(cat) - 1
            except (TypeError, ValueError):
                idx = -1
            if 0 <= idx < len(_EMIL_CATEGORY_ORDER):
                key = _EMIL_CATEGORY_ORDER[idx]
                prize_map[key] = premio
    return prize_map


# Canonical Euromillones category order for display (1st .. 13th): categoria 1 -> (5,2), 2 -> (5,1), ... 13 -> (2,0)
_EMIL_CATEGORY_ORDER = [
    (5, 2), (5, 1), (5, 0), (4, 2), (4, 1), (4, 0),
    (3, 2), (3, 1), (3, 0), (2, 2), (2, 1), (1, 2), (2, 0),
]


def _euromillones_full_wheel_reorder_txt(
    path: str,
    main_set: set,
    star_set: set,
    draw_date: str,
) -> None:
    """
    Reorder tickets in the full wheel TXT: swap only mains and stars so that
    jackpot moves to first_position, and selected 2th/3th/4th get distinct new
    positions in their ranges. Position numbers (No) on each line stay unchanged.
    Uses draw date from current_id (year, month) for first_position. Overwrites
    the file in place.
    """
    from refer.position import position_generator

    date_str = (draw_date or "")[:10]
    if not date_str or len(date_str) < 7:
        raise HTTPException(400, detail="Draw date required for reorder")
    try:
        year = int(date_str[:4])
        month = int(date_str[5:7])
    except (ValueError, IndexError):
        raise HTTPException(400, detail="Invalid draw date format for reorder")
    first_position = position_generator(year, month)
    if first_position < 1:
        raise HTTPException(400, detail="first_position must be positive")
    print(f"[reorder] draw_date={date_str} year={year} month={month} first_position={first_position}", flush=True)

    # First pass: stream until jackpot and collect positions for 2th/3th/4th; do NOT store content
    jackpot_position: Optional[int] = None
    second_positions: List[int] = []
    third_positions: List[int] = []
    fourth_positions: List[int] = []

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                pos_str, mains_str, stars_str = line.split(";")
                position = int(pos_str)
                mains = [int(x) for x in mains_str.split(",") if x]
                stars = [int(x) for x in stars_str.split(",") if x]
            except Exception:
                continue
            if len(mains) != 5 or len(stars) != 2:
                continue
            hits_main = sum(1 for n in mains if n in main_set)
            hits_star = sum(1 for n in stars if n in star_set)
            if hits_main == 5 and hits_star == 1:
                second_positions.append(position)
            elif hits_main == 5 and hits_star == 0:
                third_positions.append(position)
            elif hits_main == 4 and hits_star == 2:
                fourth_positions.append(position)
            if hits_main == 5 and hits_star == 2:
                jackpot_position = position
                break

    if jackpot_position is None:
        raise HTTPException(
            404,
            detail="Jackpot (5+2) not found in full wheel file; cannot reorder",
        )
    print(f"[reorder] jackpot_position={jackpot_position} | 2th count={len(second_positions)} 3th={len(third_positions)} 4th={len(fourth_positions)}", flush=True)

    # Marks: how many to move per tier; only move if current position > first_position
    n_2 = random.randint(1, 2)
    n_3 = random.randint(3, 5)
    n_4 = random.randint(10, 20)
    import random as rand_module
    cand_2 = [p for p in second_positions if p > first_position]
    cand_3 = [p for p in third_positions if p > first_position]
    cand_4 = [p for p in fourth_positions if p > first_position]
    to_move_2 = rand_module.sample(cand_2, min(n_2, len(cand_2))) if cand_2 else []
    to_move_3 = rand_module.sample(cand_3, min(n_3, len(cand_3))) if cand_3 else []
    to_move_4 = rand_module.sample(cand_4, min(n_4, len(cand_4))) if cand_4 else []
    print(f"[reorder] marks n_2={n_2} n_3={n_3} n_4={n_4} | candidates >first: 2th={len(cand_2)} 3th={len(cand_3)} 4th={len(cand_4)}", flush=True)
    print(f"[reorder] to_move_2={to_move_2} to_move_3={to_move_3} to_move_4={to_move_4}", flush=True)

    def range_list(lo: float, hi: int) -> List[int]:
        return list(range(max(1, int(lo)), hi + 1))

    r_2 = range_list(first_position * 0.8, first_position - 1)
    r_3 = range_list(first_position * 0.5, first_position - 1)
    r_4 = range_list(first_position * 0.3, first_position - 1)
    used: set = {first_position}

    def pick_distinct(pool: List[int], count: int, available: List[int]) -> List[Tuple[int, int]]:
        out: List[Tuple[int, int]] = []
        avail = [x for x in available if x not in used]
        if not avail or count <= 0:
            return out
        k = min(count, len(avail))
        chosen = rand_module.sample(avail, k)
        for old_pos, new_pos in zip(pool[:k], chosen):
            used.add(new_pos)
            out.append((old_pos, new_pos))
        return out

    moves: List[Tuple[int, int]] = [(jackpot_position, first_position)]
    used.add(first_position)
    moves.extend(pick_distinct(to_move_2, len(to_move_2), r_2))
    moves.extend(pick_distinct(to_move_3, len(to_move_3), r_3))
    moves.extend(pick_distinct(to_move_4, len(to_move_4), r_4))
    print(f"[reorder] ranges 2th len={len(r_2)} 3th len={len(r_3)} 4th len={len(r_4)} | moves ({len(moves)}): {moves}", flush=True)

    # Build mapping: for each new position -> old position (only for moved tickets)
    moved_from: set[int] = set()
    moved_to: Dict[int, int] = {}
    for old_pos, new_pos in moves:
        moved_from.add(old_pos)
        moved_to[new_pos] = old_pos

    # Second pass: read only the moved tickets' content into memory
    moved_content: Dict[int, Tuple[str, str]] = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            raw = line.rstrip("\n")
            if not raw:
                continue
            try:
                pos_str, mains_str, stars_str = raw.split(";")
                position = int(pos_str)
            except Exception:
                continue
            if position in moved_from:
                moved_content[position] = (mains_str, stars_str)
            if position >= jackpot_position and len(moved_content) == len(moved_from):
                # We have all moved tickets; can stop early
                break

    # Write to temp file, streaming from original; only swapped tickets use moved_content
    dirpath = os.path.dirname(path)
    fd, temp_path = tempfile.mkstemp(suffix=".txt", prefix="euromillones_reorder_", dir=dirpath if dirpath else ".")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as out:
            # Stream original file and rewrite lines up to jackpot_position when needed
            with open(path, "r", encoding="utf-8") as src:
                for line in src:
                    raw = line.rstrip("\n")
                    if not raw:
                        out.write(line)
                        continue
                    try:
                        pos_str, mains_str, stars_str = raw.split(";")
                        position = int(pos_str)
                    except Exception:
                        out.write(line)
                        continue
                    if position <= jackpot_position and position in moved_to:
                        # This output position should receive content from a different (old) position
                        old_pos = moved_to[position]
                        mains_str2, stars_str2 = moved_content.get(old_pos, (mains_str, stars_str))
                        out.write(f"{position};{mains_str2};{stars_str2}\n")
                    else:
                        # Either beyond jackpot_position or not moved: write original line
                        out.write(line if line.endswith("\n") else line + "\n")
        print(f"[reorder] writing swapped lines up to {jackpot_position} and streamed tail to {path}", flush=True)
        os.replace(temp_path, path)
    except Exception:
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        raise
    print("[reorder] done.", flush=True)


def _el_gordo_full_wheel_reorder_txt(
    path: str,
    main_set: set,
    clave_set: set,
    draw_date: str,
) -> None:
    """
    Reorder El Gordo full wheel TXT using position_generator_el_gordo rules.

    - Compute first_position from draw year/month via position_generator_el_gordo.
    - Stream once to find jackpot + 2th/3th/4th old positions.
    - If jackpot_position < first_position: skip reorder.
    - Else: choose a small number of 2th/3th/4th tickets to move into bands
      near first_position, build a swap list, and rewrite swapped lines up to
      jackpot_position (mirror Euromillones approach).
    """
    from refer.position import position_generator_el_gordo

    date_str = (draw_date or "")[:10]
    if not date_str or len(date_str) < 7:
        raise HTTPException(400, detail="Draw date required for reorder (El Gordo)")
    try:
        year = int(date_str[:4])
        month = int(date_str[5:7])
    except (ValueError, IndexError):
        raise HTTPException(400, detail="Invalid draw date format for reorder (El Gordo)")
    first_position = position_generator_el_gordo(year, month)
    if first_position < 1:
        raise HTTPException(400, detail="first_position must be positive (El Gordo)")
    print(f"[el-gordo-reorder] draw_date={date_str} year={year} month={month} first_position={first_position}", flush=True)

    jackpot_position: Optional[int] = None
    second_positions: List[int] = []
    third_positions: List[int] = []
    fourth_positions: List[int] = []

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                pos_str, mains_str, clave_str = line.split(";")
                position = int(pos_str)
                mains = [int(x) for x in mains_str.split(",") if x]
                clave = int(clave_str)
            except Exception:
                continue
            if len(mains) != 5:
                continue
            hits_main = sum(1 for n in mains if n in main_set)
            hits_clave = 1 if clave in clave_set else 0
            # 1th..4th categories for El Gordo (5+1, 5+0, 4+1, 4+0)
            if hits_main == 5 and hits_clave == 1:
                jackpot_position = position
                break
            if hits_main == 5 and hits_clave == 0:
                second_positions.append(position)
            elif hits_main == 4 and hits_clave == 1:
                third_positions.append(position)
            elif hits_main == 4 and hits_clave == 0:
                fourth_positions.append(position)

    if jackpot_position is None:
        raise HTTPException(
            404,
            detail="Jackpot (5+clave) not found in El Gordo full wheel file; cannot reorder",
        )
    print(
        f"[el-gordo-reorder] jackpot_position={jackpot_position} | 2th count={len(second_positions)} 3th={len(third_positions)} 4th={len(fourth_positions)}",
        flush=True,
    )

    # If jackpot is already before first_position, skip reorder
    if jackpot_position < first_position:
        print(
            f"[el-gordo-reorder] jackpot_position {jackpot_position} < first_position {first_position}; skipping reorder",
            flush=True,
        )
        return

    # Decide which high‑prize tickets to move:
    # Only move if there is currently NO ticket of that category before first_position.
    import random as rand_module

    def range_list_el(lo: float, hi: int) -> List[int]:
        return list(range(max(1, int(lo)), hi + 1))

    r_2 = range_list_el(first_position * 0.8, first_position - 1)
    r_3 = range_list_el(first_position * 0.5, first_position - 1)
    r_4 = range_list_el(first_position * 0.3, first_position - 1)

    used: set[int] = {first_position}

    def pick_single_move(candidates: List[int], band: List[int]) -> List[Tuple[int, int]]:
        """
        Select exactly ONE ticket from `candidates` and assign it to ONE new
        position in `band`, avoiding collisions with already used positions.
        Returns [(old_pos, new_pos)] or [].
        """
        if not candidates or not band:
            return []
        avail = [x for x in band if x not in used]
        if not avail:
            return []
        old_pos = rand_module.choice(candidates)
        new_pos = rand_module.choice(avail)
        used.add(new_pos)
        return [(old_pos, new_pos)]

    # Check if there is already at least one ticket of each category before first_position
    has_2_before = any(p < first_position for p in second_positions)
    has_3_before = any(p < first_position for p in third_positions)
    has_4_before = any(p < first_position for p in fourth_positions)

    cand_2 = [p for p in second_positions if p > first_position]
    cand_3 = [p for p in third_positions if p > first_position]
    cand_4 = [p for p in fourth_positions if p > first_position]

    moves: List[Tuple[int, int]] = [(jackpot_position, first_position)]
    used.add(first_position)
    if not has_2_before:
        moves.extend(pick_single_move(cand_2, r_2))
    if not has_3_before:
        moves.extend(pick_single_move(cand_3, r_3))
    if not has_4_before:
        moves.extend(pick_single_move(cand_4, r_4))
    print(
        f"[el-gordo-reorder] ranges 2th len={len(r_2)} 3th len={len(r_3)} 4th len={len(r_4)} | moves ({len(moves)}): {moves}",
        flush=True,
    )

    moved_from: set[int] = set()
    moved_to: Dict[int, int] = {}
    for old_pos, new_pos in moves:
        moved_from.add(old_pos)
        moved_to[new_pos] = old_pos

    moved_content: Dict[int, Tuple[str, str]] = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            raw = line.rstrip("\n")
            if not raw:
                continue
            try:
                pos_str, mains_str, clave_str = raw.split(";")
                position = int(pos_str)
            except Exception:
                continue
            if position in moved_from:
                moved_content[position] = (mains_str, clave_str)
            if position >= jackpot_position and len(moved_content) == len(moved_from):
                break

    dirpath = os.path.dirname(path)
    fd, temp_path = tempfile.mkstemp(suffix=".txt", prefix="el_gordo_reorder_", dir=dirpath if dirpath else ".")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as out:
            with open(path, "r", encoding="utf-8") as src:
                for line in src:
                    raw = line.rstrip("\n")
                    if not raw:
                        out.write(line)
                        continue
                    try:
                        pos_str, mains_str, clave_str = raw.split(";")
                        position = int(pos_str)
                    except Exception:
                        out.write(line)
                        continue
                    if position <= jackpot_position and position in moved_to:
                        old_pos = moved_to[position]
                        mains_str2, clave_str2 = moved_content.get(old_pos, (mains_str, clave_str))
                        out.write(f"{position};{mains_str2};{clave_str2}\n")
                    else:
                        out.write(line if line.endswith("\n") else line + "\n")
        print(
            f"[el-gordo-reorder] writing swapped lines up to {jackpot_position} and streamed tail to {path}",
            flush=True,
        )
        os.replace(temp_path, path)
    except Exception:
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        raise
    print("[el-gordo-reorder] done.", flush=True)


def _la_primitiva_full_wheel_reorder_txt(
    path: str,
    main_set: set,
    draw_date: str,
) -> None:
    """
    Reorder La Primitiva full wheel TXT using position_generator_la_primitiva rules.

    - Compute first_position from draw year/month via position_generator_la_primitiva.
    - First pass: loop TXT until jackpot (6 mains hit); while scanning, store
      2th/3th/4th "old" positions (5‑hit, 4‑hit, 3‑hit tickets).
    - Second pass: loop TXT until first_position and store 2th/3th/4th "new"
      positions (already early winners).
    - If there is no early winner of a given tier (new count < 1), choose one
      candidate from the corresponding old positions and move it into a band:
        r_2 = [0.8 * first_position ... first_position - 1]
        r_3 = [0.6 * first_position ... first_position - 1]
        r_4 = [0.4 * first_position ... first_position - 1]
      avoiding collisions and keeping jackpot moved to first_position.
    - Overwrite the TXT in place via a temporary file, swapping only mains
      content; line positions (No) remain unchanged.
    """
    from refer.position import position_generator_la_primitiva

    date_str = (draw_date or "")[:10]
    if not date_str or len(date_str) < 7:
        raise HTTPException(400, detail="Draw date required for reorder (La Primitiva)")
    try:
        year = int(date_str[:4])
        month = int(date_str[5:7])
    except (ValueError, IndexError):
        raise HTTPException(400, detail="Invalid draw date format for reorder (La Primitiva)")
    first_position = position_generator_la_primitiva(year, month)
    if first_position < 1:
        raise HTTPException(400, detail="first_position must be positive (La Primitiva)")
    print(
        f"[la-prim-reorder] draw_date={date_str} year={year} month={month} first_position={first_position}",
        flush=True,
    )

    # First pass: scan until jackpot and collect old positions for 2th/3th/4th.
    jackpot_position: Optional[int] = None
    second_old: List[int] = []
    third_old: List[int] = []
    fourth_old: List[int] = []

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                pos_str, mains_str = line.split(";")
                position = int(pos_str)
                mains = [int(x) for x in mains_str.split(",") if x]
            except Exception:
                continue
            if len(mains) != 6:
                continue
            hits_main = sum(1 for n in mains if n in main_set)
            if hits_main == 6:
                jackpot_position = position
                break
            elif hits_main == 5:
                second_old.append(position)
            elif hits_main == 4:
                third_old.append(position)
            elif hits_main == 3:
                fourth_old.append(position)

    if jackpot_position is None:
        raise HTTPException(
            404,
            detail="Jackpot (6 mains) not found in La Primitiva full wheel file; cannot reorder",
        )
    print(
        f"[la-prim-reorder] jackpot_position={jackpot_position} | 2th_old={len(second_old)} 3th_old={len(third_old)} 4th_old={len(fourth_old)}",
        flush=True,
    )

    # If jackpot is already before first_position, skip reorder.
    if jackpot_position < first_position:
        print(
            f"[la-prim-reorder] jackpot_position {jackpot_position} < first_position {first_position}; skipping reorder",
            flush=True,
        )
        return

    # Second pass: scan up to first_position and count existing early winners per tier.
    second_new: List[int] = []
    third_new: List[int] = []
    fourth_new: List[int] = []

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                pos_str, mains_str = line.split(";")
                position = int(pos_str)
                mains = [int(x) for x in mains_str.split(",") if x]
            except Exception:
                continue
            if position > first_position:
                break
            if len(mains) != 6:
                continue
            hits_main = sum(1 for n in mains if n in main_set)
            if hits_main == 5:
                second_new.append(position)
            elif hits_main == 4:
                third_new.append(position)
            elif hits_main == 3:
                fourth_new.append(position)

    has_2_before = len(second_new) > 0
    has_3_before = len(third_new) > 0
    has_4_before = len(fourth_new) > 0

    import random as rand_module

    def range_list_lp(lo: float, hi: int) -> List[int]:
        return list(range(max(1, int(lo)), hi + 1))

    r_2 = range_list_lp(first_position * 0.8, first_position - 1)
    r_3 = range_list_lp(first_position * 0.6, first_position - 1)
    r_4 = range_list_lp(first_position * 0.4, first_position - 1)

    used: set[int] = {first_position}

    def pick_single_move(candidates: List[int], band: List[int]) -> List[Tuple[int, int]]:
        """
        Select exactly ONE ticket from `candidates` (> first_position) and assign it
        to ONE new position in `band`, avoiding already used positions.
        Returns [(old_pos, new_pos)] or [].
        """
        if not candidates or not band:
            return []
        pool = [p for p in candidates if p > first_position]
        if not pool:
            return []
        avail = [x for x in band if x not in used]
        if not avail:
            return []
        old_pos = rand_module.choice(pool)
        new_pos = rand_module.choice(avail)
        used.add(new_pos)
        return [(old_pos, new_pos)]

    moves: List[Tuple[int, int]] = [(jackpot_position, first_position)]
    used.add(first_position)
    if not has_2_before:
        moves.extend(pick_single_move(second_old, r_2))
    if not has_3_before:
        moves.extend(pick_single_move(third_old, r_3))
    if not has_4_before:
        moves.extend(pick_single_move(fourth_old, r_4))

    print(
        f"[la-prim-reorder] ranges len r_2={len(r_2)} r_3={len(r_3)} r_4={len(r_4)} | moves ({len(moves)}): {moves}",
        flush=True,
    )

    # Build mapping: for each new position -> old position (only for moved tickets)
    moved_from: set[int] = set()
    moved_to: Dict[int, int] = {}
    for old_pos, new_pos in moves:
        moved_from.add(old_pos)
        moved_to[new_pos] = old_pos

    # Read only the moved tickets' content into memory
    moved_content: Dict[int, str] = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            raw = line.rstrip("\n")
            if not raw:
                continue
            try:
                pos_str, mains_str = raw.split(";")
                position = int(pos_str)
            except Exception:
                continue
            if position in moved_from:
                moved_content[position] = mains_str
            if position >= jackpot_position and len(moved_content) == len(moved_from):
                break

    # Write to temp file, streaming from original; only swapped tickets use moved_content
    dirpath = os.path.dirname(path)
    fd, temp_path = tempfile.mkstemp(
        suffix=".txt", prefix="la_primitiva_reorder_", dir=dirpath if dirpath else "."
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as out:
            with open(path, "r", encoding="utf-8") as src:
                for line in src:
                    raw = line.rstrip("\n")
                    if not raw:
                        out.write(line)
                        continue
                    try:
                        pos_str, mains_str = raw.split(";")
                        position = int(pos_str)
                    except Exception:
                        out.write(line)
                        continue
                    if position <= jackpot_position and position in moved_to:
                        old_pos = moved_to[position]
                        mains_str2 = moved_content.get(old_pos, mains_str)
                        out.write(f"{position};{mains_str2}\n")
                    else:
                        out.write(line if line.endswith("\n") else line + "\n")
        print(
            f"[la-prim-reorder] writing swapped lines up to {jackpot_position} and streamed tail to {path}",
            flush=True,
        )
        os.replace(temp_path, path)
    except Exception:
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        raise
    print("[la-prim-reorder] done.", flush=True)

def _euromillones_full_wheel_compare(
    current_id: str,
    pre_id: str,
    db_instance,
) -> Dict:
    """
    Compare full wheel TXT against draw result: stream TXT until jackpot (5+2), then
    aggregate prize counts and earnings per category using escrutinio premio per category.
    Save to euromillones_compare_results. If result already exists in DB, return it (calculate only once).
    """
    pre_id_clean = pre_id.strip()
    coll_compare = db_instance[EUROMILLONES_COMPARE_RESULTS_COLLECTION]
    existing = coll_compare.find_one({"current_id": current_id, "pre_id": pre_id_clean})
    if existing:
        out = {k: v for k, v in existing.items() if k != "_id"}
        return _item_to_json(out)

    coll_draws = db_instance["euromillones"]
    doc = coll_draws.find_one({"id_sorteo": current_id})
    if doc is None and current_id.isdigit():
        doc = coll_draws.find_one({"id_sorteo": int(current_id)})
    if not doc:
        raise HTTPException(404, detail="Draw not found")
    draw = _build_draw(doc, "EMIL")
    numbers = draw.get("numbers") or []
    combinacion_acta = draw.get("combinacion_acta") or ""
    if len(numbers) >= 7:
        main_draw = [int(x) for x in numbers[:5]]
        star_draw = [int(x) for x in numbers[5:7]]
    else:
        parts = re.split(r"[\s\-]+", str(combinacion_acta))
        nums = [int(p) for p in parts if p.isdigit()]
        main_draw = nums[:5] if len(nums) >= 5 else []
        star_draw = nums[5:7] if len(nums) >= 7 else []
    if len(main_draw) != 5 or len(star_draw) != 2:
        raise HTTPException(400, detail="Draw main/star numbers missing or invalid")
    main_set = set(main_draw)
    star_set = set(star_draw)
    escrutinio = draw.get("escrutinio") or []
    prize_map = _build_escrutinio_prize_map(escrutinio)
    # 1st prize (jackpot 5+2) comes from euromillones premio_bote, not escrutinio
    premio_bote = _parse_euro_premio(draw.get("premio_bote"))
    if premio_bote >= 0:
        prize_map[(5, 2)] = premio_bote
    draw_date = (draw.get("fecha_sorteo") or "")[:10] or None

    coll_progress = db_instance[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
    progress_doc = coll_progress.find_one({"cutoff_draw_id": pre_id.strip()})
    if progress_doc is None and pre_id.strip().isdigit():
        progress_doc = coll_progress.find_one({"cutoff_draw_id": int(pre_id.strip())})
    if not progress_doc:
        progress_doc = coll_progress.find_one({"probs_draw_id": pre_id.strip()})
    if not progress_doc:
        raise HTTPException(404, detail="Training progress not found for pre_id")
    path = (progress_doc.get("full_wheel_file_path") or "").strip()
    if not path:
        raise HTTPException(400, detail="Full wheel file not generated for this pre_id")
    if not os.path.isfile(path):
        raise HTTPException(404, detail="Full wheel file not found on disk")

    # category (main_hits, star_hits) -> (count, earning)
    category_stats: Dict[Tuple[int, int], Tuple[int, float]] = {}
    total_earning = 0.0
    jackpot_position: Optional[int] = None
    # Track all positions for 2nd, 3rd, 4th prize categories (5+1, 5+0, 4+2) until jackpot
    second_positions: List[int] = []
    third_positions: List[int] = []
    fourth_positions: List[int] = []
    chunk_size = 100_000

    with open(path, "r", encoding="utf-8") as f:
        while True:
            lines_read = 0
            for _ in range(chunk_size):
                line = f.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                lines_read += 1
                try:
                    pos_str, mains_str, stars_str = line.split(";")
                    position = int(pos_str)
                    mains = [int(x) for x in mains_str.split(",") if x]
                    stars = [int(x) for x in stars_str.split(",") if x]
                except Exception:
                    continue
                if len(mains) != 5 or len(stars) != 2:
                    continue
                hits_main = sum(1 for n in mains if n in main_set)
                hits_star = sum(1 for n in stars if n in star_set)
                prize = prize_map.get((hits_main, hits_star), 0.0)
                total_earning += prize
                key = (hits_main, hits_star)
                prev = category_stats.get(key, (0, 0.0))
                category_stats[key] = (prev[0] + 1, prev[1] + prize)
                # Track positions for top categories while scanning towards jackpot
                if hits_main == 5 and hits_star == 1:
                    second_positions.append(position)
                elif hits_main == 5 and hits_star == 0:
                    third_positions.append(position)
                elif hits_main == 4 and hits_star == 2:
                    fourth_positions.append(position)
                if hits_main == 5 and hits_star == 2:
                    jackpot_position = position
                    break
            if jackpot_position is not None:
                break
            if lines_read < chunk_size:
                break

    if jackpot_position is None:
        raise HTTPException(
            404,
            detail="Jackpot (5+2) not found in full wheel file; cannot compute compare result",
        )

    total_tickets = jackpot_position
    ticket_cost = total_tickets * EUROMILLONES_TICKET_COST_EUR
    # Build categories array in canonical order; label 1th..13th for known tiers
    category_labels = [
        "1th(5+2)", "2th(5+1)", "3th(5+0)", "4th(4+2)", "5th(4+1)", "6th(4+0)",
        "7th(3+2)", "8th(3+1)", "9th(3+0)", "10th(2+2)", "11th(2+1)", "12th(1+2)", "13th(2+0)",
    ]
    categories_out: List[Dict] = []
    for i, (hm, hs) in enumerate(_EMIL_CATEGORY_ORDER):
        label = category_labels[i] if i < len(category_labels) else f"{hm}+{hs}"
        count, earning = category_stats.get((hm, hs), (0, 0.0))
        categories_out.append({"category": label, "count": count, "earning": round(earning, 2)})
    # Append any other (main, star) pairs from category_stats not in canonical order
    for (hm, hs), (count, earning) in category_stats.items():
        if (hm, hs) in _EMIL_CATEGORY_ORDER:
            continue
        categories_out.append({"category": f"{hm}+{hs}", "count": count, "earning": round(earning, 2)})

    result = {
        "current_id": current_id,
        "date": draw_date,
        "pre_id": pre_id.strip(),
        "jackpot_position": jackpot_position,
        "second_positions": second_positions,
        "third_positions": third_positions,
        "fourth_positions": fourth_positions,
        "categories": categories_out,
        "total_tickets": total_tickets,
        "earning": round(total_earning, 2),
        "ticket_cost": round(ticket_cost, 2),
    }
    # Persist
    coll_compare = db_instance[EUROMILLONES_COMPARE_RESULTS_COLLECTION]
    coll_compare.replace_one(
        {"current_id": current_id, "pre_id": pre_id.strip()},
        {**result, "updated_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")},
        upsert=True,
    )
    return result


def _el_gordo_full_wheel_compare(
    current_id: str,
    pre_id: str,
    db_instance,
) -> Dict:
    """
    Compare El Gordo full wheel TXT against draw result.

    - Stream TXT and compute for each (main_hits, clave_hit) category:
      * first_position (first time it appears)
      * total count of tickets.
    - Also record jackpot_position (5 mains + clave), and counts/first positions
      for 2th(5+0), 3th(4+1), 4th(4+0).
    - Save summary to el_gordo_compare_results; if result already exists, return it.
    """
    pre_id_clean = pre_id.strip()
    coll_compare = db_instance[EL_GORDO_COMPARE_RESULTS_COLLECTION]
    existing = coll_compare.find_one({"current_id": current_id, "pre_id": pre_id_clean})
    if existing:
        out = {k: v for k, v in existing.items() if k != "_id"}
        return _item_to_json(out)

    coll_draws = db_instance["el_gordo"]
    doc = coll_draws.find_one({"id_sorteo": current_id})
    if doc is None and current_id.isdigit():
        doc = coll_draws.find_one({"id_sorteo": int(current_id)})
    if not doc:
        raise HTTPException(404, detail="El Gordo draw not found")
    # Normalized draw (used later for fecha_sorteo and optional fallbacks)
    draw = _build_draw(doc, "ELGR")
    # Use raw collection fields so we exactly match the UI:
    # - doc["numbers"]: first 5 entries = mains
    # - doc["reintegro"]: clave
    raw_numbers = doc.get("numbers") or []
    raw_reintegro = doc.get("reintegro")
    print(
        f"[el-gordo-compare] raw draw current_id={current_id!r} numbers={raw_numbers} reintegro={raw_reintegro}",
        flush=True,
    )

    main_draw: list[int] = [int(x) for x in raw_numbers[:5]]
    clave_draw: int | None = None
    try:
        if raw_reintegro is not None and str(raw_reintegro).strip():
            clave_draw = int(raw_reintegro)
    except (TypeError, ValueError):
        clave_draw = None
    print(
        f"[el-gordo-compare] parsed from raw -> mains={main_draw} clave={clave_draw}",
        flush=True,
    )

    # Fallback: if reintegro missing, try from normalized numbers / combinacion_acta.
    if len(main_draw) != 5 or clave_draw is None:
        draw_norm = normalize_draw(draw)
        numbers = draw_norm.get("numbers") or []
        combinacion_acta = draw.get("combinacion_acta") or ""
        if len(numbers) >= 6:
            main_draw = [int(x) for x in numbers[:5]]
            try:
                clave_draw = int(numbers[-1])
            except (TypeError, ValueError):
                clave_draw = None
        elif isinstance(combinacion_acta, str) and combinacion_acta.strip():
            parts = re.findall(r"\b\d{1,2}\b", combinacion_acta)
            nums = [int(p) for p in parts if p.isdigit()]
            if len(nums) >= 6:
                main_draw = nums[:5]
                clave_draw = nums[-1]
        print(
            f"[el-gordo-compare] parsed from fallback -> mains={main_draw} clave={clave_draw}",
            flush=True,
        )

    if len(main_draw) != 5 or clave_draw is None:
        print(
            f"[el-gordo-compare] ERROR mains/clave invalid -> mains={main_draw} clave={clave_draw}",
            flush=True,
        )
        raise HTTPException(400, detail="El Gordo draw mains/clave missing or invalid")
    main_set = set(main_draw)
    clave_set = {int(clave_draw)}
    draw_date = (doc.get("fecha_sorteo") or "")[:10] or None

    coll_progress = db_instance[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
    progress_doc = coll_progress.find_one({"cutoff_draw_id": pre_id_clean})
    if progress_doc is None and pre_id_clean.isdigit():
        progress_doc = coll_progress.find_one({"cutoff_draw_id": int(pre_id_clean)})
    if not progress_doc:
        raise HTTPException(404, detail="El Gordo training progress not found for pre_id")
    path = (progress_doc.get("full_wheel_file_path") or "").strip()
    if not path:
        raise HTTPException(400, detail="El Gordo full wheel file not generated for this pre_id")
    if not os.path.isfile(path):
        raise HTTPException(404, detail="El Gordo full wheel file not found on disk")

    category_first_pos: Dict[Tuple[int, int], int] = {}
    category_counts: Dict[Tuple[int, int], int] = {}
    jackpot_position: Optional[int] = None
    second_first: Optional[int] = None
    third_first: Optional[int] = None
    fourth_first: Optional[int] = None

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                pos_str, mains_str, clave_str = line.split(";")
                position = int(pos_str)
                mains = [int(x) for x in mains_str.split(",") if x]
                clave = int(clave_str)
            except Exception:
                continue
            if len(mains) != 5:
                continue
            hits_main = sum(1 for n in mains if n in main_set)
            hits_clave = 1 if clave in clave_set else 0
            key = (hits_main, hits_clave)
            if key not in category_first_pos:
                category_first_pos[key] = position
            category_counts[key] = category_counts.get(key, 0) + 1
            if hits_main == 5 and hits_clave == 1:
                if jackpot_position is None:
                    jackpot_position = position
                # Stop scanning once jackpot (5+clave) is found, like Euromillones.
                break
            elif hits_main == 5 and hits_clave == 0:
                if second_first is None:
                    second_first = position
            elif hits_main == 4 and hits_clave == 1:
                if third_first is None:
                    third_first = position
            elif hits_main == 4 and hits_clave == 0:
                if fourth_first is None:
                    fourth_first = position

    if jackpot_position is None:
        raise HTTPException(
            404,
            detail="Jackpot (5+clave) not found in El Gordo full wheel file; cannot compute compare result",
        )

    categories_out: List[Dict] = []
    for (hm, hc), first_pos in sorted(category_first_pos.items(), key=lambda kv: (-(kv[0][0]), -kv[0][1])):
        label = f"{hm}+{hc}"
        categories_out.append(
            {
                "category": label,
                "main_hits": hm,
                "clave_hit": hc,
                "first_position": first_pos,
                "count": category_counts.get((hm, hc), 0),
            }
        )

    result = {
        "current_id": current_id,
        "date": draw.get("fecha_sorteo"),
        "pre_id": pre_id_clean,
        "jackpot_position": jackpot_position,
        "pos_2th": second_first,
        "pos_3th": third_first,
        "pos_4th": fourth_first,
        "categories": categories_out,
        "total_categories": len(categories_out),
    }
    coll_compare.replace_one(
        {"current_id": current_id, "pre_id": pre_id_clean},
        {**result, "updated_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")},
        upsert=True,
    )
    print(f"[el-gordo-compare] saved result for current_id={current_id} pre_id={pre_id_clean} jackpot_position={jackpot_position}", flush=True)
    return result


def _la_primitiva_full_wheel_compare(
    current_id: str,
    pre_id: str,
    db_instance,
) -> Dict:
    """
    Compare La Primitiva full wheel TXT against draw result.

    - Stream TXT and compute for each (main_hits, reintegro_hit) category:
      * first_position (first time it appears)
      * total count of tickets.
    - Also record jackpot_position (6 mains), and first positions for
      2th(5 hits), 3th(4 hits), 4th(3 hits).
    - Stop scanning once jackpot appears (like Euromillones/El Gordo).
    - Save summary to la_primitiva_compare_results; if result already exists, return it.
    """
    pre_id_clean = pre_id.strip()
    coll_compare = db_instance[LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION]
    existing = coll_compare.find_one({"current_id": current_id, "pre_id": pre_id_clean})
    if existing:
        out = {k: v for k, v in existing.items() if k != "_id"}
        return _item_to_json(out)

    # Resolve draw from la_primitiva collection
    coll_draws = db_instance["la_primitiva"]
    doc = coll_draws.find_one({"id_sorteo": current_id})
    if doc is None and current_id.isdigit():
        doc = coll_draws.find_one({"id_sorteo": int(current_id)})
    if not doc:
        raise HTTPException(404, detail="La Primitiva draw not found")

    draw = _build_draw(doc, "LAPR")
    numbers = draw.get("numbers") or []
    combinacion_acta = draw.get("combinacion_acta") or ""
    reintegro_value = draw.get("reintegro")
    complementario_value = draw.get("complementario")

    main_draw: list[int] = []
    reintegro_draw: int | None = None
    complementario_draw: int | None = None

    # Prefer numbers array if it contains at least 6 mains.
    if len(numbers) >= 6:
        main_draw = [int(x) for x in numbers[:6]]
    else:
        # Fallback to combinacion_acta parsing.
        parts = re.findall(r"\b\d{1,2}\b", str(combinacion_acta))
        nums = [int(p) for p in parts if p.isdigit()]
        if len(nums) >= 6:
            main_draw = nums[:6]

    # Reintegro: prefer explicit field (not used in ranking, but kept for completeness/logging if needed).
    try:
        if reintegro_value is not None and str(reintegro_value).strip():
            reintegro_draw = int(reintegro_value)
    except (TypeError, ValueError):
        reintegro_draw = None

    # Complementario (bonus number).
    try:
        if complementario_value is not None and str(complementario_value).strip():
            complementario_draw = int(complementario_value)
    except (TypeError, ValueError):
        complementario_draw = None

    if len(main_draw) != 6:
        raise HTTPException(400, detail="La Primitiva draw mains missing or invalid")
    main_set = set(main_draw)
    complementario_set = {complementario_draw} if complementario_draw is not None else set()
    draw_date = (draw.get("fecha_sorteo") or doc.get("fecha_sorteo") or "")[:10] or None

    # Resolve TXT path from training progress
    coll_progress = db_instance[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
    progress_doc = coll_progress.find_one({"cutoff_draw_id": pre_id_clean})
    if progress_doc is None and pre_id_clean.isdigit():
        progress_doc = coll_progress.find_one({"cutoff_draw_id": int(pre_id_clean)})
    if not progress_doc:
        raise HTTPException(404, detail="La Primitiva training progress not found for pre_id")
    path = (progress_doc.get("full_wheel_file_path") or "").strip()
    if not path:
        raise HTTPException(400, detail="La Primitiva full wheel file not generated for this pre_id")
    if not os.path.isfile(path):
        raise HTTPException(404, detail="La Primitiva full wheel file not found on disk")

    # We only care about 5 official La Primitiva categories based on mains + complementario:
    # 1ª: (6,0) -> 6 aciertos
    # 2ª: (5,1) -> 5 + C
    # 3ª: (5,0) -> 5 aciertos
    # 4ª: (4,0) -> 4 aciertos
    # 5ª: (3,0) -> 3 aciertos
    category_first_pos: Dict[Tuple[int, int], int] = {}
    category_counts: Dict[Tuple[int, int], int] = {}
    jackpot_position: Optional[int] = None
    second_first: Optional[int] = None
    third_first: Optional[int] = None
    fourth_first: Optional[int] = None

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                pos_str, mains_str = line.split(";")
                position = int(pos_str)
                mains = [int(x) for x in mains_str.split(",") if x]
            except Exception:
                continue
            if len(mains) != 6:
                continue
            hits_main = sum(1 for n in mains if n in main_set)
            has_complementario = bool(
                complementario_set and any(n in complementario_set for n in mains)
            )

            # Map to one of the 5 official categories (or ignore if not matching any).
            key: Optional[Tuple[int, int]] = None
            if hits_main == 6:
                key = (6, 0)  # 1ª
            elif hits_main == 5 and has_complementario:
                key = (5, 1)  # 2ª
            elif hits_main == 5:
                key = (5, 0)  # 3ª
            elif hits_main == 4:
                key = (4, 0)  # 4ª
            elif hits_main == 3:
                key = (3, 0)  # 5ª

            if key is not None:
                if key not in category_first_pos:
                    category_first_pos[key] = position
                category_counts[key] = category_counts.get(key, 0) + 1

            if hits_main == 6:
                if jackpot_position is None:
                    jackpot_position = position
                # Stop scanning once jackpot (6 mains) is found.
                break
            elif hits_main == 5:
                if second_first is None:
                    second_first = position
            elif hits_main == 4:
                if third_first is None:
                    third_first = position
            elif hits_main == 3:
                if fourth_first is None:
                    fourth_first = position

    if jackpot_position is None:
        raise HTTPException(
            404,
            detail="Jackpot (6 mains) not found in La Primitiva full wheel file; cannot compute compare result",
        )

    # Build categories array in fixed canonical order (exactly 5 entries).
    ordered_keys: List[Tuple[Tuple[int, int], str]] = [
        ((6, 0), "1ª (6 aciertos)"),
        ((5, 1), "2ª (5 + C)"),
        ((5, 0), "3ª (5 aciertos)"),
        ((4, 0), "4ª (4 aciertos)"),
        ((3, 0), "5ª (3 aciertos)"),
    ]
    categories_out: List[Dict] = []
    for (key, label) in ordered_keys:
        hm, ch = key
        first_pos = category_first_pos.get(key, 0)
        count = category_counts.get(key, 0)
        categories_out.append(
            {
                "category": label,
                "main_hits": hm,
                "reintegro_hit": ch,
                "first_position": first_pos,
                "count": count,
            }
        )

    result = {
        "current_id": current_id,
        "date": draw_date,
        "pre_id": pre_id_clean,
        "jackpot_position": jackpot_position,
        "pos_2th": second_first,
        "pos_3th": third_first,
        "pos_4th": fourth_first,
        "categories": categories_out,
        "total_categories": len(categories_out),
    }
    coll_compare.replace_one(
        {"current_id": current_id, "pre_id": pre_id_clean},
        {**result, "updated_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")},
        upsert=True,
    )
    print(
        f"[la-prim-compare] saved result for current_id={current_id} pre_id={pre_id_clean} jackpot_position={jackpot_position}",
        flush=True,
    )
    return result

@app.get("/api/euromillones/compare/full-wheel")
def api_euromillones_compare_full_wheel(
    current_id: str = Query(..., description="id_sorteo of the draw to evaluate (result)."),
    pre_id: str = Query(..., description="cutoff_draw_id or probs_draw_id for the full wheel run."),
):
    """
    Compare full wheel TXT (from pre_id) against draw result (current_id): find jackpot position,
    aggregate prizes for tickets 1..jackpot, return and save table (current_id, date, categories, total_tickets, earning, ticket_cost).
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    try:
        result = _euromillones_full_wheel_compare(current_id.strip(), pre_id.strip(), db)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=f"Compare failed: {e}")
    return JSONResponse(content=_item_to_json(result))


@app.get("/api/el-gordo/compare/full-wheel")
def api_el_gordo_compare_full_wheel(
    current_id: str = Query(..., description="id_sorteo of the El Gordo draw to evaluate (result)."),
    pre_id: str = Query(..., description="cutoff_draw_id or probs_draw_id for the El Gordo full wheel run."),
):
    """
    Compare El Gordo full wheel TXT (from pre_id) against draw result (current_id).
    Returns and saves summary: jackpot_position, 2th/3th/4th first positions,
    and first_position + count for every (main_hits, clave_hit) category.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    try:
        result = _el_gordo_full_wheel_compare(current_id.strip(), pre_id.strip(), db)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=f"El Gordo compare failed: {e}")
    return JSONResponse(content=_item_to_json(result))


@app.get("/api/la-primitiva/compare/full-wheel")
def api_la_primitiva_compare_full_wheel(
    current_id: str = Query(..., description="id_sorteo of the La Primitiva draw to evaluate (result)."),
    pre_id: str = Query(..., description="cutoff_draw_id or probs_draw_id for the La Primitiva full wheel run."),
):
    """
    Compare La Primitiva full wheel TXT (from pre_id) against draw result (current_id).
    Returns and saves summary: jackpot_position, 2th/3th/4th first positions,
    and first_position + count for every (main_hits, reintegro_hit) category.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    try:
        result = _la_primitiva_full_wheel_compare(current_id.strip(), pre_id.strip(), db)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=f"La Primitiva compare failed: {e}")
    return JSONResponse(content=_item_to_json(result))

@app.get("/api/el-gordo/compare/full-wheel")
def api_el_gordo_compare_full_wheel(
    current_id: str = Query(..., description="id_sorteo of the El Gordo draw to evaluate (result)."),
    pre_id: str = Query(..., description="cutoff_draw_id or probs_draw_id for the El Gordo full wheel run."),
):
    """
    Compare El Gordo full wheel TXT (from pre_id) against draw result (current_id).
    Returns and saves summary: jackpot_position, 2th/3th/4th first positions,
    and first_position + count for every (main_hits, clave_hit) category.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    try:
        result = _el_gordo_full_wheel_compare(current_id.strip(), pre_id.strip(), db)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=f"El Gordo compare failed: {e}")
    return JSONResponse(content=_item_to_json(result))


@app.get("/api/euromillones/compare/analysis")
def api_euromillones_compare_analysis(
    limit: int = Query(200, ge=1, le=1000, description="Max rows to return, sorted by date desc"),
):
    """
    Analysis of Euromillones full-wheel compares.
    Reads euromillones_compare_results and returns rows sorted by date desc:
      - date
      - jackpot_position (1th)
      - first 2th position
      - first 3th position
      - first 4th position
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    coll = db[EUROMILLONES_COMPARE_RESULTS_COLLECTION]
    cursor = coll.find(
        {},
        projection={
            "_id": 0,
            "date": 1,
            "jackpot_position": 1,
            "second_positions": 1,
            "third_positions": 1,
            "fourth_positions": 1,
            "current_id": 1,
            "pre_id": 1,
        },
    ).sort("date", -1).limit(limit)
    rows = []
    for doc in cursor:
        date_str = (doc.get("date") or "")[:10]
        second_positions = doc.get("second_positions") or []
        third_positions = doc.get("third_positions") or []
        fourth_positions = doc.get("fourth_positions") or []
        rows.append(
            {
                "date": date_str,
                "current_id": str(doc.get("current_id") or ""),
                "pre_id": str(doc.get("pre_id") or ""),
                "pos_1th": int(doc.get("jackpot_position") or 0),
                "pos_2th": int(second_positions[0]) if second_positions else None,
                "pos_3th": int(third_positions[0]) if third_positions else None,
                "pos_4th": int(fourth_positions[0]) if fourth_positions else None,
            }
        )
    return JSONResponse(content={"rows": rows})


@app.get("/api/el-gordo/compare/analysis")
def api_el_gordo_compare_analysis(
    limit: int = Query(200, ge=1, le=1000, description="Max rows to return, sorted by date desc"),
):
    """
    Analysis of El Gordo full-wheel compares.

    Reads el_gordo_compare_results and returns rows sorted by date desc:
      - date
      - current_id
      - pre_id
      - jackpot_position (5+clave)
      - pos_2th (5+0), pos_3th (4+1), pos_4th (4+0)
      - all categories from 'categories' array (1ª..8ª, Reintegro, etc.)
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    coll = db[EL_GORDO_COMPARE_RESULTS_COLLECTION]
    cursor = coll.find(
        {},
        projection={
            "_id": 0,
            "date": 1,
            "current_id": 1,
            "pre_id": 1,
            "jackpot_position": 1,
            "pos_2th": 1,
            "pos_3th": 1,
            "pos_4th": 1,
            "categories": 1,
        },
    ).sort("date", -1).limit(limit)
    rows: List[Dict[str, Any]] = []
    for doc in cursor:
        date_str = (doc.get("date") or "")[:10]
        cats = doc.get("categories") or []
        # Ensure categories is always a list of dicts
        norm_cats: List[Dict[str, Any]] = []
        for c in cats:
            if not isinstance(c, dict):
                continue
            norm_cats.append(
                {
                    "category": str(c.get("category") or ""),
                    "main_hits": int(c.get("main_hits") or 0),
                    "clave_hit": int(c.get("clave_hit") or 0),
                    "first_position": int(c.get("first_position") or 0),
                    "count": int(c.get("count") or 0),
                }
            )
        rows.append(
            {
                "date": date_str,
                "current_id": str(doc.get("current_id") or ""),
                "pre_id": str(doc.get("pre_id") or ""),
                "jackpot_position": int(doc.get("jackpot_position") or 0),
                "pos_2th": int(doc.get("pos_2th") or 0) if doc.get("pos_2th") is not None else None,
                "pos_3th": int(doc.get("pos_3th") or 0) if doc.get("pos_3th") is not None else None,
                "pos_4th": int(doc.get("pos_4th") or 0) if doc.get("pos_4th") is not None else None,
                "categories": norm_cats,
            }
        )
    return JSONResponse(content={"rows": rows})


@app.get("/api/la-primitiva/compare/analysis")
def api_la_primitiva_compare_analysis(
    limit: int = Query(200, ge=1, le=1000, description="Max rows to return, sorted by date desc"),
):
    """
    Analysis of La Primitiva full-wheel compares.

    Reads la_primitiva_compare_results and returns rows sorted by date desc:
      - date
      - current_id
      - pre_id
      - jackpot_position (1ª: 6 aciertos)
      - pos_2th (2ª: 5 + C), pos_3th (3ª: 5), pos_4th (4ª: 4), pos_5th (5ª: 3)
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    coll = db[LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION]
    cursor = coll.find(
        {},
        projection={
            "_id": 0,
            "date": 1,
            "current_id": 1,
            "pre_id": 1,
            "jackpot_position": 1,
            "pos_2th": 1,
            "pos_3th": 1,
            "pos_4th": 1,
            "categories": 1,
        },
    ).sort("date", -1).limit(limit)
    rows: List[Dict[str, Any]] = []
    for doc in cursor:
        date_str = (doc.get("date") or "")[:10]
        cats = doc.get("categories") or []
        pos_5th: Optional[int] = None
        if isinstance(cats, list):
            for c in cats:
                if not isinstance(c, dict):
                    continue
                if int(c.get("main_hits") or 0) == 3 and int(c.get("reintegro_hit") or 0) == 0:
                    fp = int(c.get("first_position") or 0)
                    if fp > 0:
                        pos_5th = fp
                        break
        rows.append(
            {
                "date": date_str,
                "current_id": str(doc.get("current_id") or ""),
                "pre_id": str(doc.get("pre_id") or ""),
                "pos_1th": int(doc.get("jackpot_position") or 0),
                "pos_2th": int(doc.get("pos_2th") or 0) if doc.get("pos_2th") is not None else None,
                "pos_3th": int(doc.get("pos_3th") or 0) if doc.get("pos_3th") is not None else None,
                "pos_4th": int(doc.get("pos_4th") or 0) if doc.get("pos_4th") is not None else None,
                "pos_5th": pos_5th,
            }
        )
    return JSONResponse(content={"rows": rows})


@app.get("/api/euromillones/compare/full-wheel/tickets")
def api_euromillones_compare_full_wheel_tickets(
    current_id: str = Query(..., description="id_sorteo of the draw (result)."),
    pre_id: str = Query(..., description="cutoff_draw_id or probs_draw_id for the full wheel run."),
    skip: int = Query(0, ge=0, description="Number of tickets to skip from the start (0‑based)."),
    limit: int = Query(100, ge=1, le=100, description="Page size (max 100)."),
):
    """
    Paginated view of tickets in the full wheel TXT (after any reorder).
    Returns tickets with:
      - position (No)
      - mains (array)
      - stars (array)
      - first_main (first main number)
      - category (1th..13th or hm+hs) based on hits vs result draw.
    Uses skip/limit on the position number (1‑based). Does not load the whole file into RAM.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    pre_id_clean = pre_id.strip()
    current_id_clean = current_id.strip()

    # Resolve draw and main/star sets (same as compare)
    coll_draws = db["euromillones"]
    doc = coll_draws.find_one({"id_sorteo": current_id_clean})
    if doc is None and current_id_clean.isdigit():
        doc = coll_draws.find_one({"id_sorteo": int(current_id_clean)})
    if not doc:
        raise HTTPException(404, detail="Draw not found")
    draw = _build_draw(doc, "EMIL")
    numbers = draw.get("numbers") or []
    combinacion_acta = draw.get("combinacion_acta") or ""
    if len(numbers) >= 7:
        main_draw = [int(x) for x in numbers[:5]]
        star_draw = [int(x) for x in numbers[5:7]]
    else:
        parts = re.split(r"[\s\\-]+", str(combinacion_acta))
        nums = [int(p) for p in parts if p.isdigit()]
        main_draw = nums[:5] if len(nums) >= 5 else []
        star_draw = nums[5:7] if len(nums) >= 7 else []
    if len(main_draw) != 5 or len(star_draw) != 2:
        raise HTTPException(400, detail="Draw main/star numbers missing or invalid")
    main_set = set(main_draw)
    star_set = set(star_draw)

    # Resolve TXT path from train progress
    coll_progress = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
    progress_doc = coll_progress.find_one({"cutoff_draw_id": pre_id_clean})
    if progress_doc is None and pre_id_clean.isdigit():
        progress_doc = coll_progress.find_one({"cutoff_draw_id": int(pre_id_clean)})
    if not progress_doc:
        progress_doc = coll_progress.find_one({"probs_draw_id": pre_id_clean})
    if not progress_doc:
        raise HTTPException(404, detail="Training progress not found for pre_id")
    path = (progress_doc.get("full_wheel_file_path") or "").strip()
    if not path:
        raise HTTPException(400, detail="Full wheel file not generated for this pre_id")
    if not os.path.isfile(path):
        raise HTTPException(404, detail="Full wheel file not found on disk")

    # Total tickets: prefer compare result if present; else estimate as last position in file
    total_tickets: Optional[int] = None
    coll_compare = db[EUROMILLONES_COMPARE_RESULTS_COLLECTION]
    existing = coll_compare.find_one({"current_id": current_id_clean, "pre_id": pre_id_clean})
    if existing:
        try:
            total_tickets = int(existing.get("total_tickets") or 0)
        except (TypeError, ValueError):
            total_tickets = None
    if not total_tickets:
        # Fallback: scan to get last valid position (streaming)
        last_pos = 0
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    pos_str, _m, _s = line.split(";")
                    p = int(pos_str)
                    if p > last_pos:
                        last_pos = p
                except Exception:
                    continue
        total_tickets = last_pos

    # Map (hm, hs) -> 1th..13th label
    category_labels = [
        "1th(5+2)", "2th(5+1)", "3th(5+0)", "4th(4+2)", "5th(4+1)", "6th(4+0)",
        "7th(3+2)", "8th(3+1)", "9th(3+0)", "10th(2+2)", "11th(2+1)", "12th(1+2)", "13th(2+0)",
    ]

    def label_for(hm: int, hs: int) -> str:
        try:
            idx = _EMIL_CATEGORY_ORDER.index((hm, hs))
        except ValueError:
            return f"{hm}+{hs}"
        return category_labels[idx] if idx < len(category_labels) else f"{hm}+{hs}"

    # Stream TXT and collect only tickets in [skip, skip+limit)
    items = []
    start_pos = skip + 1  # positions are 1‑based
    end_pos = skip + limit
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                pos_str, mains_str, stars_str = line.split(";")
                position = int(pos_str)
            except Exception:
                continue
            if position < start_pos:
                continue
            if position > end_pos:
                break
            mains = [int(x) for x in mains_str.split(",") if x]
            stars = [int(x) for x in stars_str.split(",") if x]
            if len(mains) != 5 or len(stars) != 2:
                continue
            hits_main = sum(1 for n in mains if n in main_set)
            hits_star = sum(1 for n in stars if n in star_set)
            items.append(
                {
                    "position": position,
                    "mains": mains,
                    "stars": stars,
                    "first_main": mains[0],
                    "category": label_for(hits_main, hits_star),
                    "main_hits": hits_main,
                    "star_hits": hits_star,
                }
            )
            if len(items) >= limit:
                break

    return JSONResponse(
        content={
            "current_id": current_id_clean,
            "pre_id": pre_id_clean,
            "skip": skip,
            "limit": limit,
            "total_tickets": total_tickets,
            "tickets": items,
        }
    )


@app.get("/api/el-gordo/compare/full-wheel/tickets")
def api_el_gordo_compare_full_wheel_tickets(
    current_id: str = Query(..., description="id_sorteo of the draw (result)."),
    pre_id: str = Query(..., description="cutoff_draw_id for the full wheel run."),
    skip: int = Query(0, ge=0, description="Number of tickets to skip from the start (0‑based)."),
    limit: int = Query(100, ge=1, le=100, description="Page size (max 100)."),
):
    """
    Paginated view of tickets in the El Gordo full wheel TXT (after any reorder).

    Returns tickets with:
      - position (No)
      - mains (array)
      - clave (number)
      - main_hits / clave_hit
      - category label (1ª..8ª, Reintegro) based on hits vs result draw.
    Uses skip/limit on the position number (1‑based). Does not load the whole file into RAM.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    pre_id_clean = pre_id.strip()
    current_id_clean = current_id.strip()

    # Resolve draw and main/clave sets (same parsing as _el_gordo_full_wheel_compare)
    coll_draws = db["el_gordo"]
    doc = coll_draws.find_one({"id_sorteo": current_id_clean})
    if doc is None and current_id_clean.isdigit():
        doc = coll_draws.find_one({"id_sorteo": int(current_id_clean)})
    if not doc:
        raise HTTPException(404, detail="El Gordo draw not found")

    raw_numbers = doc.get("numbers") or []
    raw_reintegro = doc.get("reintegro")
    main_draw: List[int] = [int(x) for x in raw_numbers[:5]]
    clave_draw: Optional[int] = None
    try:
        if raw_reintegro is not None and str(raw_reintegro).strip():
            clave_draw = int(raw_reintegro)
    except (TypeError, ValueError):
        clave_draw = None

    if len(main_draw) != 5 or clave_draw is None:
        raise HTTPException(400, detail="El Gordo draw mains/clave missing or invalid")

    main_set = set(main_draw)
    clave_set = {clave_draw}

    # Resolve TXT path from train progress
    coll_progress = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
    progress_doc = coll_progress.find_one({"cutoff_draw_id": pre_id_clean})
    if progress_doc is None and pre_id_clean.isdigit():
        progress_doc = coll_progress.find_one({"cutoff_draw_id": int(pre_id_clean)})
    if not progress_doc:
        raise HTTPException(404, detail="El Gordo training progress not found for pre_id")
    path = (progress_doc.get("full_wheel_file_path") or "").strip()
    if not path:
        raise HTTPException(400, detail="El Gordo full wheel file not generated for this pre_id")
    if not os.path.isfile(path):
        raise HTTPException(404, detail="El Gordo full wheel file not found on disk")

    # Total tickets: estimate as last position in file (streaming)
    total_tickets: Optional[int] = None
    last_pos = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                pos_str, _m, _c = line.split(";")
                p = int(pos_str)
                if p > last_pos:
                    last_pos = p
            except Exception:
                continue
    total_tickets = last_pos

    # Map (hm, hc) -> label like "1ª (5 + 1)" etc.
    category_labels_el_gordo: Dict[Tuple[int, int], str] = {
        (5, 1): "1ª (5 + 1)",
        (5, 0): "2ª (5 + 0)",
        (4, 1): "3ª (4 + 1)",
        (4, 0): "4ª (4 + 0)",
        (3, 1): "5ª (3 + 1)",
        (3, 0): "6ª (3 + 0)",
        (2, 1): "7ª (2 + 1)",
        (2, 0): "8ª (2 + 0)",
        (0, 1): "Reintegro",
    }

    def label_for_el_gordo(hm: int, hc: int) -> str:
        return category_labels_el_gordo.get((hm, hc), f"{hm}+{hc}")

    # Stream TXT and collect only tickets in [skip, skip+limit)
    items: List[Dict] = []
    start_pos = skip + 1  # positions are 1‑based
    end_pos = skip + limit
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                pos_str, mains_str, clave_str = line.split(";")
                position = int(pos_str)
            except Exception:
                continue
            if position < start_pos:
                continue
            if position > end_pos:
                break
            mains = [int(x) for x in mains_str.split(",") if x]
            try:
                clave = int(clave_str)
            except Exception:
                continue
            if len(mains) != 5:
                continue
            hits_main = sum(1 for n in mains if n in main_set)
            hits_clave = 1 if clave in clave_set else 0
            items.append(
                {
                    "position": position,
                    "mains": mains,
                    "clave": clave,
                    "first_main": mains[0],
                    "category": label_for_el_gordo(hits_main, hits_clave),
                    "main_hits": hits_main,
                    "clave_hit": hits_clave,
                }
            )
            if len(items) >= limit:
                break

    return JSONResponse(
        content={
            "current_id": current_id_clean,
            "pre_id": pre_id_clean,
            "skip": skip,
            "limit": limit,
            "total_tickets": total_tickets,
            "tickets": items,
        }
    )


@app.get("/api/la-primitiva/compare/full-wheel/tickets")
def api_la_primitiva_compare_full_wheel_tickets(
    current_id: str = Query(..., description="id_sorteo of the draw (result)."),
    pre_id: str = Query(..., description="cutoff_draw_id for the full wheel run."),
    skip: int = Query(0, ge=0, description="Number of tickets to skip from the start (0‑based)."),
    limit: int = Query(100, ge=1, le=100, description="Page size (max 100)."),
):
    """
    Paginated view of tickets in the La Primitiva full wheel TXT (after any reorder).

    Returns tickets with:
      - position (No)
      - mains (array of 6 numbers)
    Uses skip/limit on the position number (1‑based). Does not load the whole file into RAM.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    pre_id_clean = pre_id.strip()
    current_id_clean = current_id.strip()

    # Resolve TXT path from train progress
    coll_progress = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
    progress_doc = coll_progress.find_one({"cutoff_draw_id": pre_id_clean})
    if progress_doc is None and pre_id_clean.isdigit():
        progress_doc = coll_progress.find_one({"cutoff_draw_id": int(pre_id_clean)})
    if not progress_doc:
        raise HTTPException(404, detail="La Primitiva training progress not found for pre_id")
    path = (progress_doc.get("full_wheel_file_path") or "").strip()
    if not path:
        raise HTTPException(400, detail="Full wheel file not generated for this pre_id")
    if not os.path.isfile(path):
        raise HTTPException(404, detail="Full wheel file not found on disk")

    # Total tickets: estimate as last position in file (streaming)
    total_tickets: Optional[int] = None
    last_pos = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                pos_str, _m = line.split(";")
                p = int(pos_str)
                if p > last_pos:
                    last_pos = p
            except Exception:
                continue
    total_tickets = last_pos

    # Stream TXT and collect only tickets in [skip, skip+limit)
    items: List[Dict] = []
    start_pos = skip + 1  # positions are 1‑based
    end_pos = skip + limit
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                pos_str, mains_str = line.split(";")
                position = int(pos_str)
            except Exception:
                continue
            if position < start_pos:
                continue
            if position > end_pos:
                break
            mains = [int(x) for x in mains_str.split(",") if x]
            if len(mains) != 6:
                continue
            items.append(
                {
                    "position": position,
                    "mains": mains,
                }
            )
            if len(items) >= limit:
                break

    return JSONResponse(
        content={
            "current_id": current_id_clean,
            "pre_id": pre_id_clean,
            "skip": skip,
            "limit": limit,
            "total_tickets": total_tickets,
            "tickets": items,
        }
    )


@app.post("/api/euromillones/compare/full-wheel/reorder")
def api_euromillones_compare_full_wheel_reorder(
    current_id: str = Query(..., description="id_sorteo of the draw (result); draw date from this."),
    pre_id: str = Query(..., description="cutoff_draw_id or probs_draw_id for the full wheel TXT."),
):
    """
    Reorder tickets in the full wheel TXT (swap mains/stars only, position numbers unchanged),
    then run compare. Uses draw date from current_id for first_position; all new positions distinct.
    If reorder was already done for this (current_id, pre_id), returns cached result without lock.
    If another reorder is in progress, returns 503.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    pre_id_clean = pre_id.strip()
    current_id_clean = current_id.strip()
    coll_compare = db[EUROMILLONES_COMPARE_RESULTS_COLLECTION]
    existing = coll_compare.find_one({"current_id": current_id_clean, "pre_id": pre_id_clean})
    if existing and existing.get("reorder_applied") is True:
        result = {k: v for k, v in existing.items() if k != "_id"}
        return JSONResponse(content=_item_to_json(result))
    if not _EUROMILLONES_REORDER_LOCK.acquire(blocking=False):
        raise HTTPException(
            503,
            detail="Reorder already in progress. Please retry later.",
        )
    try:
        result = _api_euromillones_compare_full_wheel_reorder_impl(current_id, pre_id)
        return JSONResponse(content=_item_to_json(result))
    finally:
        _EUROMILLONES_REORDER_LOCK.release()


@app.post("/api/el-gordo/compare/full-wheel/reorder")
def api_el_gordo_compare_full_wheel_reorder(
    current_id: str = Query(..., description="id_sorteo of the El Gordo draw (result); draw date from this."),
    pre_id: str = Query(..., description="cutoff_draw_id or probs_draw_id for the El Gordo full wheel TXT."),
):
    """
    Reorder tickets in the El Gordo full wheel TXT (swap mains/clave only),
    then run compare. Uses draw date from current_id for first_position via
    position_generator_el_gordo. If result already exists with reorder_applied,
    returns cached result. If another reorder is in progress, returns 503.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    pre_id_clean = pre_id.strip()
    current_id_clean = current_id.strip()
    print(f"[el-gordo-reorder-api] START current_id={current_id_clean!r} pre_id={pre_id_clean!r}", flush=True)
    coll_compare = db[EL_GORDO_COMPARE_RESULTS_COLLECTION]
    existing = coll_compare.find_one({"current_id": current_id_clean, "pre_id": pre_id_clean})
    if existing and existing.get("reorder_applied") is True:
        print("[el-gordo-reorder-api] existing result with reorder_applied=true, returning cached", flush=True)
        result = {k: v for k, v in existing.items() if k != "_id"}
        return JSONResponse(content=_item_to_json(result))
    if not _EL_GORDO_REORDER_LOCK.acquire(blocking=False):
        print("[el-gordo-reorder-api] reorder lock busy, returning 503", flush=True)
        raise HTTPException(
            503,
            detail="El Gordo reorder already in progress. Please retry later.",
        )
    try:
        coll_draws = db["el_gordo"]
        doc = coll_draws.find_one({"id_sorteo": current_id_clean})
        if doc is None and current_id_clean.isdigit():
            doc = coll_draws.find_one({"id_sorteo": int(current_id_clean)})
        if not doc:
            raise HTTPException(404, detail="El Gordo draw not found")

        # Parse mains + clave exactly like compare
        raw_numbers = doc.get("numbers") or []
        raw_reintegro = doc.get("reintegro")
        print(
            f"[el-gordo-reorder-api] raw draw numbers={raw_numbers} reintegro={raw_reintegro}",
            flush=True,
        )
        main_draw: list[int] = [int(x) for x in raw_numbers[:5]]
        clave_draw: int | None = None
        try:
            if raw_reintegro is not None and str(raw_reintegro).strip():
                clave_draw = int(raw_reintegro)
        except (TypeError, ValueError):
            clave_draw = None

        if len(main_draw) != 5 or clave_draw is None:
            print(
                f"[el-gordo-reorder-api] ERROR mains/clave invalid -> mains={main_draw} clave={clave_draw}",
                flush=True,
            )
            raise HTTPException(400, detail="El Gordo draw mains/clave missing or invalid")

        main_set = set(main_draw)
        clave_set = {int(clave_draw)}
        draw_date = (doc.get("fecha_sorteo") or "")[:10] or None
        if not draw_date:
            raise HTTPException(400, detail="El Gordo draw date missing")

        coll_progress = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
        progress_doc = coll_progress.find_one({"cutoff_draw_id": pre_id_clean})
        if progress_doc is None and pre_id_clean.isdigit():
            progress_doc = coll_progress.find_one({"cutoff_draw_id": int(pre_id_clean)})
        if not progress_doc:
            raise HTTPException(404, detail="El Gordo training progress not found for pre_id")
        path = (progress_doc.get("full_wheel_file_path") or "").strip()
        if not path:
            raise HTTPException(400, detail="El Gordo full wheel file not generated for this pre_id")
        if not os.path.isfile(path):
            raise HTTPException(404, detail="El Gordo full wheel file not found on disk")

        print(
            f"[el-gordo-reorder-api] calling _el_gordo_full_wheel_reorder_txt path={path} mains={sorted(main_set)} clave_set={clave_set}",
            flush=True,
        )
        _el_gordo_full_wheel_reorder_txt(path, main_set, clave_set, draw_date)

        coll_compare.delete_one({"current_id": current_id_clean, "pre_id": pre_id_clean})
        print("[el-gordo-reorder-api] running compare after reorder", flush=True)
        result = _el_gordo_full_wheel_compare(current_id_clean, pre_id_clean, db)
        coll_compare.update_one(
            {"current_id": current_id_clean, "pre_id": pre_id_clean},
            {"$set": {"reorder_applied": True}},
        )
        print("[el-gordo-reorder-api] DONE", flush=True)
        return JSONResponse(content=_item_to_json(result))
    finally:
        _EL_GORDO_REORDER_LOCK.release()


@app.post("/api/la-primitiva/compare/full-wheel/reorder")
def api_la_primitiva_compare_full_wheel_reorder(
    current_id: str = Query(..., description="id_sorteo of the La Primitiva draw (result); draw date from this."),
    pre_id: str = Query(..., description="cutoff_draw_id or probs_draw_id for the La Primitiva full wheel TXT."),
):
    """
    Reorder tickets in the La Primitiva full wheel TXT (swap mains only),
    then run compare. Uses draw date from current_id for first_position via
    position_generator_la_primitiva. If result already exists with reorder_applied,
    returns cached result. If another reorder is in progress, returns 503.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    pre_id_clean = pre_id.strip()
    current_id_clean = current_id.strip()
    print(f"[la-prim-reorder-api] START current_id={current_id_clean!r} pre_id={pre_id_clean!r}", flush=True)
    coll_compare = db[LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION]
    existing = coll_compare.find_one({"current_id": current_id_clean, "pre_id": pre_id_clean})
    if existing and existing.get("reorder_applied") is True:
        print("[la-prim-reorder-api] existing result with reorder_applied=true, returning cached", flush=True)
        result = {k: v for k, v in existing.items() if k != "_id"}
        return JSONResponse(content=_item_to_json(result))
    if not _LA_PRIMITIVA_REORDER_LOCK.acquire(blocking=False):
        print("[la-prim-reorder-api] reorder lock busy, returning 503", flush=True)
        raise HTTPException(
            503,
            detail="La Primitiva reorder already in progress. Please retry later.",
        )
    try:
        # Resolve draw to get date and main_set
        coll_draws = db["la_primitiva"]
        doc = coll_draws.find_one({"id_sorteo": current_id_clean})
        if doc is None and current_id_clean.isdigit():
            doc = coll_draws.find_one({"id_sorteo": int(current_id_clean)})
        if not doc:
            raise HTTPException(404, detail="La Primitiva draw not found")
        draw = _build_draw(doc, "LAPR")
        numbers = draw.get("numbers") or []
        combinacion_acta = draw.get("combinacion_acta") or ""
        if len(numbers) >= 6:
            main_draw = [int(x) for x in numbers[:6]]
        else:
            parts = re.findall(r"\b\d{1,2}\b", str(combinacion_acta))
            nums = [int(p) for p in parts if p.isdigit()]
            main_draw = nums[:6] if len(nums) >= 6 else []
        if len(main_draw) != 6:
            raise HTTPException(400, detail="La Primitiva draw mains missing or invalid for reorder")
        main_set = set(main_draw)
        draw_date = (draw.get("fecha_sorteo") or doc.get("fecha_sorteo") or "")[:10]

        # Resolve TXT path from training progress
        coll_progress = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
        progress_doc = coll_progress.find_one({"cutoff_draw_id": pre_id_clean})
        if progress_doc is None and pre_id_clean.isdigit():
            progress_doc = coll_progress.find_one({"cutoff_draw_id": int(pre_id_clean)})
        if not progress_doc:
            raise HTTPException(404, detail="La Primitiva training progress not found for pre_id")
        path = (progress_doc.get("full_wheel_file_path") or "").strip()
        if not path:
            raise HTTPException(400, detail="La Primitiva full wheel file not generated for this pre_id")
        if not os.path.isfile(path):
            raise HTTPException(404, detail="La Primitiva full wheel file not found on disk")

        print(f"[la-prim-reorder-api] calling _la_primitiva_full_wheel_reorder_txt path={path!r}", flush=True)
        _la_primitiva_full_wheel_reorder_txt(
            path=path,
            main_set=main_set,
            draw_date=draw_date,
        )
        print("[la-prim-reorder-api] reorder done, now running compare", flush=True)
        result = _la_primitiva_full_wheel_compare(current_id_clean, pre_id_clean, db)
        # Mark that reorder was applied so we don't repeat it.
        coll_compare.replace_one(
            {"current_id": current_id_clean, "pre_id": pre_id_clean},
            {**result, "reorder_applied": True, "updated_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")},
            upsert=True,
        )
        print("[la-prim-reorder-api] saved result with reorder_applied=true", flush=True)
        return JSONResponse(content=_item_to_json(result))
    finally:
        _LA_PRIMITIVA_REORDER_LOCK.release()



def _api_euromillones_compare_full_wheel_reorder_impl(current_id: str, pre_id: str):
    """Implementation of reorder + compare (called with reorder lock held)."""
    pre_id_clean = pre_id.strip()
    current_id_clean = current_id.strip()
    coll_draws = db["euromillones"]
    doc = coll_draws.find_one({"id_sorteo": current_id_clean})
    if doc is None and current_id_clean.isdigit():
        doc = coll_draws.find_one({"id_sorteo": int(current_id_clean)})
    if not doc:
        raise HTTPException(404, detail="Draw not found")
    draw = _build_draw(doc, "EMIL")
    numbers = draw.get("numbers") or []
    combinacion_acta = draw.get("combinacion_acta") or ""
    if len(numbers) >= 7:
        main_draw = [int(x) for x in numbers[:5]]
        star_draw = [int(x) for x in numbers[5:7]]
    else:
        parts = re.split(r"[\s\-]+", str(combinacion_acta))
        nums = [int(p) for p in parts if p.isdigit()]
        main_draw = nums[:5] if len(nums) >= 5 else []
        star_draw = nums[5:7] if len(nums) >= 7 else []
    if len(main_draw) != 5 or len(star_draw) != 2:
        raise HTTPException(400, detail="Draw main/star numbers missing or invalid")
    main_set = set(main_draw)
    star_set = set(star_draw)
    draw_date = (draw.get("fecha_sorteo") or "")[:10] or None
    if not draw_date:
        raise HTTPException(400, detail="Draw date missing")

    coll_progress = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
    progress_doc = coll_progress.find_one({"cutoff_draw_id": pre_id_clean})
    if progress_doc is None and pre_id_clean.isdigit():
        progress_doc = coll_progress.find_one({"cutoff_draw_id": int(pre_id_clean)})
    if not progress_doc:
        progress_doc = coll_progress.find_one({"probs_draw_id": pre_id_clean})
    if not progress_doc:
        raise HTTPException(404, detail="Training progress not found for pre_id")
    path = (progress_doc.get("full_wheel_file_path") or "").strip()
    if not path:
        raise HTTPException(400, detail="Full wheel file not generated for this pre_id")
    if not os.path.isfile(path):
        raise HTTPException(404, detail="Full wheel file not found on disk")

    try:
        _euromillones_full_wheel_reorder_txt(path, main_set, star_set, draw_date)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=f"Reorder failed: {e}")

    coll_compare = db[EUROMILLONES_COMPARE_RESULTS_COLLECTION]
    coll_compare.delete_one({"current_id": current_id_clean, "pre_id": pre_id_clean})
    try:
        result = _euromillones_full_wheel_compare(current_id_clean, pre_id_clean, db)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=f"Compare after reorder failed: {e}")
    coll_compare.update_one(
        {"current_id": current_id_clean, "pre_id": pre_id_clean},
        {"$set": {"reorder_applied": True}},
    )
    return result


@app.post("/api/euromillones/betting/bought")
async def api_euromillones_betting_bought(request: Request):
    """
    Save bought tickets into euromillones_train_progress. Body may include optional "cutoff_draw_id" (same as La Primitiva).
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    try:
        body = await request.json() or {}
    except Exception:
        body = {}
    tickets = body.get("tickets")
    if not isinstance(tickets, list):
        raise HTTPException(400, detail="Body must contain 'tickets' array")
    draw_date = (body.get("draw_date") or "").strip()[:10] or None
    cutoff = (body.get("cutoff_draw_id") or "").strip() or None
    coll = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
    if draw_date:
        result = coll.update_one(
            {"probs_fecha_sorteo": draw_date},
            {"$set": {"bought_tickets": tickets, "bought_tickets_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}},
        )
    elif cutoff:
        result = coll.update_one(
            {"cutoff_draw_id": cutoff},
            {"$set": {"bought_tickets": tickets, "bought_tickets_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}},
        )
        if result.matched_count == 0 and cutoff.isdigit():
            result = coll.update_one(
                {"cutoff_draw_id": int(cutoff)},
                {"$set": {"bought_tickets": tickets, "bought_tickets_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}},
            )
    else:
        last_draw_date = _get_last_draw_date("euromillones")
        if not last_draw_date:
            raise HTTPException(400, detail="No last_draw_date for euromillones")
        date_str = (last_draw_date or "").strip()[:10]
        result = coll.update_one(
            {"probs_fecha_sorteo": date_str},
            {"$set": {"bought_tickets": tickets, "bought_tickets_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}},
        )
    if result.matched_count == 0:
        raise HTTPException(404, detail="No progress found for draw_date or cutoff_draw_id")
    return JSONResponse(
        content={"status": "ok", "saved_count": len(tickets)},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.post("/api/dev/euromillones/betting/seed-test-bought")
def api_dev_euromillones_seed_test_bought(
    cutoff_draw_id: str = Query("", description="cutoff_draw_id of euromillones_train_progress doc to seed"),
):
    """
    DEV-ONLY: seed some test bought tickets into euromillones_train_progress.bought_tickets.
    This endpoint is public (no Authorization). Do NOT enable in production.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    cutoff = cutoff_draw_id.strip()
    if not cutoff:
        # Fallback to last cutoff_draw_id from train progress
        coll = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
        doc = coll.find_one(
            projection={"cutoff_draw_id": 1},
            sort=[("probs_fecha_sorteo", -1), ("_id", -1)],
        )
        if not doc:
            raise HTTPException(404, detail="No euromillones_train_progress doc found to seed")
        cutoff = str(doc.get("cutoff_draw_id") or "").strip()
        if not cutoff and isinstance(doc.get("cutoff_draw_id"), int):
            cutoff = str(doc.get("cutoff_draw_id"))
        if not cutoff:
            raise HTTPException(400, detail="cutoff_draw_id missing and cannot infer last one")

    tickets = [
        {"mains": [3, 12, 25, 36, 44], "stars": [2, 9]},
        {"mains": [5, 19, 28, 37, 45], "stars": [1, 6]},
        {"mains": [7, 14, 21, 35, 42], "stars": [3, 11]},
        {"mains": [1, 2, 3, 4, 5], "stars": [1, 2]},
        {"mains": [10, 20, 30, 40, 50], "stars": [7, 8]},
    ]

    coll = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
    # Try string cutoff_draw_id then int
    doc = coll.find_one({"cutoff_draw_id": cutoff})
    if not doc and cutoff.isdigit():
        doc = coll.find_one({"cutoff_draw_id": int(cutoff)})
    if not doc:
        raise HTTPException(404, detail=f"euromillones_train_progress not found for cutoff_draw_id={cutoff}")

    coll.update_one(
        {"_id": doc["_id"]},
        {
            "$set": {
                "bought_tickets": tickets,
                "bought_tickets_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        },
    )
    return JSONResponse(
        content={
            "status": "ok",
            "cutoff_draw_id": cutoff,
            "saved_count": len(tickets),
        },
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.post("/api/dev/el-gordo/betting/seed-test-bought")
def api_dev_el_gordo_seed_test_bought(
    cutoff_draw_id: str = Query("", description="cutoff_draw_id of el_gordo_train_progress doc to seed"),
):
    """
    DEV-ONLY: seed some test bought tickets into el_gordo_train_progress.bought_tickets.
    This endpoint is public (no Authorization). Do NOT enable in production.

    It mirrors the Euromillones dev endpoint but uses El Gordo ticket shape:
      { "mains": [..5 numbers..], "clave": X }.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    cutoff = cutoff_draw_id.strip()
    if not cutoff:
        # Fallback to last cutoff_draw_id from train progress
        coll = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
        doc = coll.find_one(
            projection={"cutoff_draw_id": 1},
            sort=[("probs_fecha_sorteo", -1), ("_id", -1)],
        )
        if not doc:
            raise HTTPException(404, detail="No el_gordo_train_progress doc found to seed")
        cutoff = str(doc.get("cutoff_draw_id") or "").strip()
        if not cutoff and isinstance(doc.get("cutoff_draw_id"), int):
            cutoff = str(doc.get("cutoff_draw_id"))
        if not cutoff:
            raise HTTPException(400, detail="cutoff_draw_id missing and cannot infer last one")

    tickets = [
        {"mains": [1, 2, 3, 4, 5], "clave": 1},
        {"mains": [10, 20, 30, 40, 50], "clave": 5},
        {"mains": [7, 14, 21, 28, 35], "clave": 7},
        {"mains": [11, 22, 33, 44, 54], "clave": 9},
        {"mains": [6, 16, 26, 36, 46], "clave": 3},
    ]

    coll = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
    # Try string cutoff_draw_id then int
    doc = coll.find_one({"cutoff_draw_id": cutoff})
    if not doc and cutoff.isdigit():
        doc = coll.find_one({"cutoff_draw_id": int(cutoff)})
    if not doc:
        raise HTTPException(404, detail=f"el_gordo_train_progress not found for cutoff_draw_id={cutoff}")

    coll.update_one(
        {"_id": doc["_id"]},
        {
            "$set": {
                "bought_tickets": tickets,
                "bought_tickets_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        },
    )


@app.post("/api/dev/la-primitiva/betting/seed-test-bought")
def api_dev_la_primitiva_seed_test_bought(
    cutoff_draw_id: str = Query(
        "",
        description="cutoff_draw_id of la_primitiva_train_progress doc to seed",
    ),
):
    """
    DEV-ONLY: seed some test bought tickets into la_primitiva_train_progress.bought_tickets.
    This endpoint is public (no Authorization). Do NOT enable in production.

    Mirrors the Euromillones dev endpoint but uses La Primitiva ticket shape:
    { mains: [6 numbers], reintegro: number }.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    cutoff = cutoff_draw_id.strip()
    coll = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]

    if not cutoff:
        # Fallback to last cutoff_draw_id from train progress (by probs_fecha_sorteo desc)
        doc_last = coll.find_one(
            projection={"cutoff_draw_id": 1},
            sort=[("probs_fecha_sorteo", -1), ("_id", -1)],
        )
        if not doc_last:
            raise HTTPException(
                404, detail="No la_primitiva_train_progress doc found to seed"
            )
        cutoff = str(doc_last.get("cutoff_draw_id") or "").strip()
        if not cutoff and isinstance(doc_last.get("cutoff_draw_id"), int):
            cutoff = str(doc_last.get("cutoff_draw_id"))
        if not cutoff:
            raise HTTPException(
                400, detail="cutoff_draw_id missing and cannot infer last one"
            )

    tickets = [
        {"mains": [1, 5, 12, 23, 34, 45], "reintegro": 0},
        {"mains": [3, 9, 17, 28, 39, 47], "reintegro": 5},
        {"mains": [4, 14, 24, 33, 42, 49], "reintegro": 7},
        {"mains": [2, 8, 16, 25, 36, 44], "reintegro": 2},
        {"mains": [6, 11, 19, 27, 38, 41], "reintegro": 9},
    ]

    # Try string cutoff_draw_id then int
    doc = coll.find_one({"cutoff_draw_id": cutoff})
    if not doc and cutoff.isdigit():
        doc = coll.find_one({"cutoff_draw_id": int(cutoff)})
    if not doc:
        raise HTTPException(
            404,
            detail=f"la_primitiva_train_progress not found for cutoff_draw_id={cutoff}",
        )

    coll.update_one(
        {"_id": doc["_id"]},
        {
            "$set": {
                "bought_tickets": tickets,
                "bought_tickets_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        },
    )
    return JSONResponse(
        content={
            "status": "ok",
            "cutoff_draw_id": cutoff,
            "saved_count": len(tickets),
        },
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )
    return JSONResponse(
        content={
            "status": "ok",
            "cutoff_draw_id": cutoff,
            "saved_count": len(tickets),
        },
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )

@app.post("/api/euromillones/betting/enqueue")
async def api_euromillones_betting_enqueue(request: Request):
    """Save bucket tickets to buy queue (status=waiting). Body: { "tickets": [{ "mains": [5], "stars": [2] }, ...], "draw_date"?, "cutoff_draw_id"? } (max 5)."""
    try:
        body = await request.json() or {}
    except Exception:
        body = {}
    tickets = body.get("tickets")
    if not isinstance(tickets, list) or len(tickets) == 0 or len(tickets) > 5:
        raise HTTPException(400, detail="Body must contain 'tickets' array with 1–5 items")
    normalized = []
    for t in tickets:
        mains = t.get("mains")
        stars = t.get("stars")
        if not isinstance(mains, list) or len(mains) != 5 or not isinstance(stars, list) or len(stars) != 2:
            raise HTTPException(400, detail="Each ticket must have 'mains' (5 numbers) and 'stars' (2 numbers)")
        normalized.append({"mains": [int(m) for m in mains], "stars": [int(s) for s in stars]})
    draw_date = (body.get("draw_date") or "").strip()[:10] or None
    cutoff_draw_id = (body.get("cutoff_draw_id") or "").strip() or None
    if db is None:
        raise HTTPException(503, detail="Database not available")
    coll = db[EUROMILLONES_BUY_QUEUE_COLLECTION]
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    doc = {
        "lottery": "euromillones",
        "tickets": normalized,
        "tickets_count": len(normalized),
        "draw_date": draw_date,
        "cutoff_draw_id": cutoff_draw_id,
        "status": "waiting",
        "created_at": now,
    }
    ins = coll.insert_one(doc)
    return JSONResponse(
        content={"status": "ok", "queue_id": str(ins.inserted_id), "tickets_count": len(normalized), "message": "Añadido a la cola. El bot comprará en breve."},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/api/euromillones/betting/buy-queue")
def api_euromillones_betting_buy_queue(limit: int = Query(50, ge=1, le=100)):
    if db is None:
        return JSONResponse(content={"items": []}, headers={"Cache-Control": "no-store, no-cache, must-revalidate"})
    coll = db[EUROMILLONES_BUY_QUEUE_COLLECTION]
    cursor = coll.find({}).sort("created_at", -1).limit(limit)
    items = []
    for d in cursor:
        items.append({
            "id": str(d["_id"]),
            "lottery": d.get("lottery", "euromillones"),
            "status": d.get("status", "waiting"),
            "saved_status": d.get("saved_status"),
            "tickets_count": d.get("tickets_count", 0),
            "tickets": d.get("tickets") or [],
            "draw_date": d.get("draw_date"),
            "cutoff_draw_id": d.get("cutoff_draw_id"),
            "created_at": d.get("created_at"),
            "started_at": d.get("started_at"),
            "finished_at": d.get("finished_at"),
            "error": d.get("error"),
        })
    return JSONResponse(content={"items": items}, headers={"Cache-Control": "no-store, no-cache, must-revalidate"})


@app.post("/api/euromillones/betting/save-bought-from-queue")
def api_euromillones_betting_save_bought_from_queue():
    if db is None:
        return JSONResponse(content={"status": "ok", "saved_count": 0}, headers={"Cache-Control": "no-store, no-cache, must-revalidate"})
    coll = db[EUROMILLONES_BUY_QUEUE_COLLECTION]
    cursor = coll.find({"status": "bought", "$or": [{"saved_status": {"$ne": True}}, {"saved_status": {"$exists": False}}]})
    saved_count = 0
    for d in cursor:
        oid = d["_id"]
        tickets = d.get("tickets") or []
        if not tickets:
            coll.update_one({"_id": oid}, {"$set": {"saved_status": True}})
            saved_count += 1
            continue
        draw_date = (d.get("draw_date") or "").strip()[:10] or None
        cutoff_draw_id = (d.get("cutoff_draw_id") or "").strip() or None
        _append_euromillones_bought_tickets(tickets, draw_date=draw_date, cutoff_draw_id=cutoff_draw_id)
        coll.update_one({"_id": oid}, {"$set": {"saved_status": True}})
        saved_count += 1
    return JSONResponse(content={"status": "ok", "saved_count": saved_count}, headers={"Cache-Control": "no-store, no-cache, must-revalidate"})


@app.delete("/api/euromillones/betting/buy-queue/{queue_id}")
def api_euromillones_betting_buy_queue_delete(queue_id: str):
    if db is None:
        raise HTTPException(status_code=500, detail="Database not connected")
    try:
        oid = ObjectId(queue_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid queue_id")
    coll = db[EUROMILLONES_BUY_QUEUE_COLLECTION]
    doc = coll.find_one({"_id": oid}, projection={"status": 1})
    if not doc:
        raise HTTPException(status_code=404, detail="Queue item not found")
    st = doc.get("status")
    if st not in ("waiting", "failed"):
        raise HTTPException(status_code=400, detail="Solo se puede eliminar cuando está en cola (waiting) o ha fallado (failed)")
    coll.delete_one({"_id": oid, "status": {"$in": ["waiting", "failed"]}})
    return JSONResponse(content={"status": "ok", "message": "Eliminado de la cola"}, headers={"Cache-Control": "no-store, no-cache, must-revalidate"})


@app.post("/api/euromillones/betting/bot/claim")
def api_euromillones_betting_bot_claim():
    """For the Euromillones bot (another device). Claim one waiting queue item; set in_progress and return it."""
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    coll = db[EUROMILLONES_BUY_QUEUE_COLLECTION]
    doc = coll.find_one({"status": "waiting"}, sort=[("created_at", 1)])
    if not doc:
        return JSONResponse(
            content={"claimed": False, "queue_id": None, "tickets": [], "draw_date": None, "cutoff_draw_id": None},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    oid = doc["_id"]
    updated = coll.update_one(
        {"_id": oid, "status": "waiting"},
        {"$set": {"status": "in_progress", "started_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}},
    )
    if updated.modified_count == 0:
        return JSONResponse(
            content={"claimed": False, "queue_id": None, "tickets": [], "draw_date": None, "cutoff_draw_id": None},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    tickets = doc.get("tickets") or []
    draw_date = (doc.get("draw_date") or "").strip()[:10] or None
    cutoff_draw_id = (doc.get("cutoff_draw_id") or "").strip() or None
    return JSONResponse(
        content={
            "claimed": True,
            "queue_id": str(oid),
            "tickets": tickets,
            "draw_date": draw_date,
            "cutoff_draw_id": cutoff_draw_id,
        },
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.post("/api/euromillones/betting/bot/complete")
async def api_euromillones_betting_bot_complete(request: Request):
    """For the Euromillones bot. Report result: body { "queue_id", "success", "error"?. }."""
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    try:
        body = await request.json() or {}
    except Exception:
        body = {}
    queue_id = body.get("queue_id")
    success = body.get("success") is True
    error = (body.get("error") or "").strip() or None
    if not queue_id:
        raise HTTPException(400, detail="queue_id required")
    try:
        oid = ObjectId(queue_id)
    except Exception:
        raise HTTPException(400, detail="Invalid queue_id")
    coll = db[EUROMILLONES_BUY_QUEUE_COLLECTION]
    doc = coll.find_one({"_id": oid}, projection={"status": 1})
    if not doc:
        raise HTTPException(404, detail="Queue item not found")
    if doc.get("status") != "in_progress":
        return JSONResponse(
            content={"status": "ok", "message": "Job already completed or not in progress"},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    if success:
        coll.update_one(
            {"_id": oid},
            {"$set": {"status": "bought", "saved_status": False, "finished_at": now, "error": None}},
        )
    else:
        coll.update_one(
            {"_id": oid},
            {"$set": {"status": "failed", "error": error or "Bot reported failure", "finished_at": now}},
        )
    return JSONResponse(
        content={"status": "ok", "message": "completed"},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


def _get_la_primitiva_cutoff_from_last_draw_date(last_draw_date: str | None) -> str | None:
    """Resolve cutoff_draw_id (id_sorteo) from last_draw_date by finding the draw in la_primitiva."""
    if not db or not last_draw_date:
        return None
    last_date_str = (last_draw_date or "").strip()[:10]
    if not last_date_str:
        return None
    coll = db["la_primitiva"]
    doc = coll.find_one(
        {"fecha_sorteo": {"$regex": f"^{re.escape(last_date_str)}"}},
        projection={"id_sorteo": 1},
        sort=[("fecha_sorteo", -1)],
    )
    if not doc:
        doc = coll.find_one(sort=[("fecha_sorteo", -1)], projection={"id_sorteo": 1})
    if not doc:
        return None
    cid = doc.get("id_sorteo")
    return str(cid) if cid is not None else None


def _append_la_primitiva_bought_tickets(
    tickets: list,
    draw_date: str | None = None,
    cutoff_draw_id: str | None = None,
) -> None:
    """Append tickets to bought_tickets in la_primitiva_train_progress (merge, dedupe by mains). Queue tickets are {mains}; we add reintegro=0."""
    if db is None or not tickets:
        return
    draw_date = (draw_date or "").strip()[:10] or None
    cutoff = (cutoff_draw_id or "").strip() or None
    coll = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
    doc = None
    if draw_date:
        doc = coll.find_one({"probs_fecha_sorteo": draw_date}, projection={"bought_tickets": 1})
    elif cutoff:
        doc = coll.find_one({"cutoff_draw_id": cutoff}, projection={"bought_tickets": 1})
        if not doc and cutoff.isdigit():
            doc = coll.find_one({"cutoff_draw_id": int(cutoff)}, projection={"bought_tickets": 1})
    if not doc and not draw_date and not cutoff:
        last = _get_last_draw_date("la-primitiva")
        if last:
            cutoff = _get_la_primitiva_cutoff_from_last_draw_date(last)
            if cutoff:
                doc = coll.find_one({"cutoff_draw_id": cutoff}, projection={"bought_tickets": 1})
                if not doc and cutoff.isdigit():
                    doc = coll.find_one({"cutoff_draw_id": int(cutoff)}, projection={"bought_tickets": 1})
    if not doc:
        return
    existing = doc.get("bought_tickets") or []
    seen = {tuple(sorted(t.get("mains") or [])) for t in existing}
    merged = list(existing)
    for t in tickets:
        mains = t.get("mains") or []
        if len(mains) != 6:
            continue
        key = tuple(sorted(mains))
        if key not in seen:
            seen.add(key)
            merged.append({"mains": list(mains), "reintegro": int(t.get("reintegro", 0))})
    query = {"_id": doc["_id"]}
    coll.update_one(
        query,
        {"$set": {"bought_tickets": merged, "bought_tickets_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}},
    )
    logger.info("la_primitiva: appended %s queue tickets to bought_tickets (total %s)", len(tickets), len(merged))


def _append_euromillones_bought_tickets(
    tickets: list,
    draw_date: str | None = None,
    cutoff_draw_id: str | None = None,
) -> None:
    """Append tickets to bought_tickets in euromillones_train_progress (merge, dedupe by mains+stars)."""
    if db is None or not tickets:
        return
    draw_date = (draw_date or "").strip()[:10] or None
    cutoff = (cutoff_draw_id or "").strip() or None
    coll = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
    doc = None
    if draw_date:
        doc = coll.find_one({"probs_fecha_sorteo": draw_date}, projection={"bought_tickets": 1})
    elif cutoff:
        doc = coll.find_one({"cutoff_draw_id": cutoff}, projection={"bought_tickets": 1})
        if not doc and cutoff.isdigit():
            doc = coll.find_one({"cutoff_draw_id": int(cutoff)}, projection={"bought_tickets": 1})
    if not doc and not draw_date and not cutoff:
        last = _get_last_draw_date("euromillones")
        if last:
            date_str = (last or "").strip()[:10]
            doc = coll.find_one({"probs_fecha_sorteo": date_str}, projection={"bought_tickets": 1})
    if not doc:
        return
    existing = doc.get("bought_tickets") or []
    seen = {(tuple(t.get("mains") or []), tuple(t.get("stars") or [])) for t in existing}
    merged = list(existing)
    for t in tickets:
        mains = t.get("mains") or []
        stars = t.get("stars") or []
        if len(mains) != 5 or len(stars) != 2:
            continue
        key = (tuple(mains), tuple(stars))
        if key not in seen:
            seen.add(key)
            merged.append({"mains": list(mains), "stars": list(stars)})
    query = {"_id": doc["_id"]}
    coll.update_one(
        query,
        {"$set": {"bought_tickets": merged, "bought_tickets_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}},
    )
    logger.info("euromillones: appended %s queue tickets to bought_tickets (total %s)", len(tickets), len(merged))


def _la_primitiva_betting_pool_response(
    last_draw_date: str | None,
    cutoff_draw_id: str | None,
    candidate_pool: list,
    bought_tickets: list,
) -> JSONResponse:
    last_date_str = (last_draw_date or "").strip()[:10] if last_draw_date else ""
    return JSONResponse(
        content={
            "last_draw_date": last_date_str or None,
            "cutoff_draw_id": cutoff_draw_id,
            "candidate_pool": candidate_pool,
            "candidate_pool_count": len(candidate_pool),
            "bought_tickets": bought_tickets,
        },
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/api/la-primitiva/betting/last-cutoff")
def api_la_primitiva_betting_last_cutoff():
    """
    Return cutoff_draw_id of the last la_primitiva_train_progress doc by probs_fecha_sorteo (desc) then _id (desc).
    """
    if db is None:
        return JSONResponse(content={"cutoff_draw_id": None})
    coll = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
    doc = coll.find_one(
        projection={"cutoff_draw_id": 1, "probs_fecha_sorteo": 1},
        sort=[("probs_fecha_sorteo", -1), ("_id", -1)],
    )
    cid = doc.get("cutoff_draw_id") if doc else None
    return JSONResponse(
        content={"cutoff_draw_id": str(cid) if cid is not None else None},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/api/la-primitiva/betting/last-draw-date")
def api_la_primitiva_betting_last_draw_date():
    """Return last_draw_date for la-primitiva from scraper_metadata. Betting tab uses this to find train_progress by probs_fecha_sorteo."""
    if db is None:
        return JSONResponse(content={"last_draw_date": None})
    last_draw_date = _get_last_draw_date("la-primitiva")
    return JSONResponse(
        content={"last_draw_date": last_draw_date},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/api/la-primitiva/betting/pool")
def api_la_primitiva_betting_pool(
    draw_date: str | None = Query(None, description="Draw date YYYY-MM-DD; find train_progress by probs_fecha_sorteo (from scraper_metadata last_draw_date)."),
    cutoff_draw_id: str | None = Query(None, description="Optional id_sorteo; used when draw_date not provided."),
):
    """
    Return candidate pool for betting. Prefer draw_date (from scraper_metadata): find *train_progress by probs_fecha_sorteo === draw_date.
    Else use cutoff_draw_id; else use last_draw_date from scraper_metadata.
    candidate_pool items are { mains, reintegro }; frontend may use mains-only for selection.
    """
    try:
        if db is None:
            return _la_primitiva_betting_pool_response(None, None, [], [])
        coll = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
        date_str = (draw_date or "").strip()[:10] or None
        cutoff: str | None = (cutoff_draw_id or "").strip() or None
        if not date_str and not cutoff:
            last_draw_date = _get_last_draw_date("la-primitiva")
            if not last_draw_date:
                return _la_primitiva_betting_pool_response(None, None, [], [])
            date_str = (last_draw_date or "").strip()[:10]
        if date_str:
            doc = coll.find_one({"probs_fecha_sorteo": date_str})
            if not doc:
                return _la_primitiva_betting_pool_response(date_str, None, [], [])
            cutoff = str(doc.get("cutoff_draw_id") or "") or None
        elif cutoff:
            doc = coll.find_one({"cutoff_draw_id": cutoff})
            if doc is None and cutoff.isdigit():
                doc = coll.find_one({"cutoff_draw_id": int(cutoff)})
            if not doc:
                return _la_primitiva_betting_pool_response(None, cutoff, [], [])
            date_str = (doc.get("probs_fecha_sorteo") or "").strip()[:10] or None
        else:
            return _la_primitiva_betting_pool_response(None, None, [], [])
        pool = doc.get("candidate_pool") or []
        bought = doc.get("bought_tickets") or []
        return _la_primitiva_betting_pool_response(date_str or None, cutoff, pool, bought)
    except Exception as e:
        logger.exception("la-primitiva betting pool: %s", e)
        return _la_primitiva_betting_pool_response(None, None, [], [])


@app.get("/api/la-primitiva/betting/pool-from-file")
def api_la_primitiva_betting_pool_from_file(
    draw_date: str | None = Query(
        None,
        description="Draw date YYYY-MM-DD; use full_wheel_file_path for that date.",
    ),
    cutoff_draw_id: str | None = Query(
        None,
        description="Optional id_sorteo; used when draw_date not provided.",
    ),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
):
    """
    Return a paginated slice of the La Primitiva full-wheeling pool from TXT (mirror Euromillones/El Gordo).

    Uses skip/limit on the file lines. Each line: position;m1,...,m6
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
    date_str = (draw_date or "").strip()[:10] or None
    cutoff = (cutoff_draw_id or "").strip() or None

    # Fallback to last_draw_date if neither provided.
    if not date_str and not cutoff:
        last = (_get_last_draw_date("la-primitiva") or "").strip()
        if last:
            date_str = last[:10]

    doc = None
    if cutoff:
        doc = coll.find_one({"cutoff_draw_id": cutoff})
        if doc is None and cutoff.isdigit():
            doc = coll.find_one({"cutoff_draw_id": int(cutoff)})
    if doc is None and date_str:
        doc = coll.find_one({"full_wheel_draw_date": date_str}) or coll.find_one(
            {"probs_fecha_sorteo": date_str}
        )
    if doc is None:
        return JSONResponse(
            content={
                "draw_date": date_str,
                "cutoff_draw_id": cutoff,
                "total": 0,
                "skip": skip,
                "limit": limit,
                "tickets": [],
            },
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )

    path = (doc.get("full_wheel_file_path") or "").strip()
    total = int(doc.get("full_wheel_total_tickets") or 0)

    if not path or total <= 0:
        return JSONResponse(
            content={
                "draw_date": date_str,
                "cutoff_draw_id": cutoff,
                "total": total,
                "skip": skip,
                "limit": limit,
                "tickets": [],
            },
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )

    tickets_list: List[Dict] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            # Skip first `skip` lines
            for _ in range(skip):
                if not f.readline():
                    break
            # Read up to `limit` lines
            for _ in range(limit):
                line = f.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    pos_str, mains_str = line.split(";")
                    position = int(pos_str)
                    mains = [int(x) for x in mains_str.split(",") if x]
                except Exception:
                    continue
                tickets_list.append({"position": position, "mains": mains})
    except FileNotFoundError:
        raise HTTPException(404, detail="Full wheel file not found on disk")

    return JSONResponse(
        content={
            "draw_date": (doc.get("full_wheel_draw_date") or date_str),
            "cutoff_draw_id": doc.get("cutoff_draw_id") or cutoff,
            "total": total,
            "skip": skip,
            "limit": limit,
            "tickets": tickets_list,
        },
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.post("/api/la-primitiva/betting/bought")
async def api_la_primitiva_betting_bought(request: Request):
    """
    Save bought tickets into la_primitiva_train_progress. Body may include draw_date (prefer) or cutoff_draw_id.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    try:
        body = await request.json() or {}
    except Exception:
        body = {}
    tickets = body.get("tickets")
    if not isinstance(tickets, list):
        raise HTTPException(400, detail="Body must contain 'tickets' array")
    draw_date = (body.get("draw_date") or "").strip()[:10] or None
    cutoff = (body.get("cutoff_draw_id") or "").strip() or None
    coll = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
    if draw_date:
        result = coll.update_one(
            {"probs_fecha_sorteo": draw_date},
            {"$set": {"bought_tickets": tickets, "bought_tickets_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}},
        )
    elif cutoff:
        result = coll.update_one(
            {"cutoff_draw_id": cutoff},
            {"$set": {"bought_tickets": tickets, "bought_tickets_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}},
        )
        if result.matched_count == 0 and cutoff.isdigit():
            result = coll.update_one(
                {"cutoff_draw_id": int(cutoff)},
                {"$set": {"bought_tickets": tickets, "bought_tickets_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}},
            )
    else:
        last_draw_date = _get_last_draw_date("la-primitiva")
        if not last_draw_date:
            raise HTTPException(400, detail="No last_draw_date for la-primitiva")
        cutoff = _get_la_primitiva_cutoff_from_last_draw_date(last_draw_date)
        if not cutoff:
            raise HTTPException(404, detail="No draw found for last_draw_date")
        result = coll.update_one(
            {"cutoff_draw_id": cutoff},
            {"$set": {"bought_tickets": tickets, "bought_tickets_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}},
        )
        if result.matched_count == 0 and cutoff.isdigit():
            result = coll.update_one(
                {"cutoff_draw_id": int(cutoff)},
                {"$set": {"bought_tickets": tickets, "bought_tickets_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}},
            )
    if result.matched_count == 0:
        raise HTTPException(404, detail="No progress found for draw_date or cutoff_draw_id")
    return JSONResponse(
        content={"status": "ok", "saved_count": len(tickets)},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.post("/api/la-primitiva/betting/enqueue")
async def api_la_primitiva_betting_enqueue(request: Request):
    """Save bucket tickets to buy queue (status=waiting). Body: { "tickets": [{ "mains": [6 numbers] }, ...], "draw_date"?, "cutoff_draw_id"? } (max 8)."""
    try:
        body = await request.json() or {}
    except Exception:
        body = {}
    tickets = body.get("tickets")
    if not isinstance(tickets, list) or len(tickets) == 0 or len(tickets) > 8:
        raise HTTPException(400, detail="Body must contain 'tickets' array with 1–8 items")
    normalized = []
    for t in tickets:
        mains = t.get("mains")
        if not isinstance(mains, list) or len(mains) != 6:
            raise HTTPException(400, detail="Each ticket must have 'mains' array of 6 numbers")
        normalized.append({"mains": [int(m) for m in mains], "reintegro": int(t.get("reintegro", 0))})
    draw_date = (body.get("draw_date") or "").strip()[:10] or None
    cutoff_draw_id = (body.get("cutoff_draw_id") or "").strip() or None
    if db is None:
        raise HTTPException(503, detail="Database not available")
    coll = db[LA_PRIMITIVA_BUY_QUEUE_COLLECTION]
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    doc = {
        "lottery": "la-primitiva",
        "tickets": normalized,
        "tickets_count": len(normalized),
        "draw_date": draw_date,
        "cutoff_draw_id": cutoff_draw_id,
        "status": "waiting",
        "created_at": now,
    }
    ins = coll.insert_one(doc)
    return JSONResponse(
        content={"status": "ok", "queue_id": str(ins.inserted_id), "tickets_count": len(normalized), "message": "Añadido a la cola. El bot comprará en breve."},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/api/la-primitiva/betting/buy-queue")
def api_la_primitiva_betting_buy_queue(limit: int = Query(50, ge=1, le=100)):
    if db is None:
        return JSONResponse(content={"items": []}, headers={"Cache-Control": "no-store, no-cache, must-revalidate"})
    coll = db[LA_PRIMITIVA_BUY_QUEUE_COLLECTION]
    cursor = coll.find({}).sort("created_at", -1).limit(limit)
    items = []
    for d in cursor:
        items.append({
            "id": str(d["_id"]),
            "lottery": d.get("lottery", "la-primitiva"),
            "status": d.get("status", "waiting"),
            "saved_status": d.get("saved_status"),
            "tickets_count": d.get("tickets_count", 0),
            "tickets": d.get("tickets") or [],
            "draw_date": d.get("draw_date"),
            "cutoff_draw_id": d.get("cutoff_draw_id"),
            "created_at": d.get("created_at"),
            "started_at": d.get("started_at"),
            "finished_at": d.get("finished_at"),
            "error": d.get("error"),
        })
    return JSONResponse(content={"items": items}, headers={"Cache-Control": "no-store, no-cache, must-revalidate"})


@app.post("/api/la-primitiva/betting/save-bought-from-queue")
def api_la_primitiva_betting_save_bought_from_queue():
    if db is None:
        return JSONResponse(content={"status": "ok", "saved_count": 0}, headers={"Cache-Control": "no-store, no-cache, must-revalidate"})
    coll = db[LA_PRIMITIVA_BUY_QUEUE_COLLECTION]
    cursor = coll.find({"status": "bought", "$or": [{"saved_status": {"$ne": True}}, {"saved_status": {"$exists": False}}]})
    saved_count = 0
    for d in cursor:
        oid = d["_id"]
        tickets = d.get("tickets") or []
        if not tickets:
            coll.update_one({"_id": oid}, {"$set": {"saved_status": True}})
            saved_count += 1
            continue
        draw_date = (d.get("draw_date") or "").strip()[:10] or None
        cutoff_draw_id = (d.get("cutoff_draw_id") or "").strip() or None
        _append_la_primitiva_bought_tickets(tickets, draw_date=draw_date, cutoff_draw_id=cutoff_draw_id)
        coll.update_one({"_id": oid}, {"$set": {"saved_status": True}})
        saved_count += 1
    return JSONResponse(content={"status": "ok", "saved_count": saved_count}, headers={"Cache-Control": "no-store, no-cache, must-revalidate"})


@app.delete("/api/la-primitiva/betting/buy-queue/{queue_id}")
def api_la_primitiva_betting_buy_queue_delete(queue_id: str):
    if db is None:
        raise HTTPException(status_code=500, detail="Database not connected")
    try:
        oid = ObjectId(queue_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid queue_id")
    coll = db[LA_PRIMITIVA_BUY_QUEUE_COLLECTION]
    doc = coll.find_one({"_id": oid}, projection={"status": 1})
    if not doc:
        raise HTTPException(status_code=404, detail="Queue item not found")
    st = doc.get("status")
    if st not in ("waiting", "failed"):
        raise HTTPException(status_code=400, detail="Solo se puede eliminar cuando está en cola (waiting) o ha fallado (failed)")
    coll.delete_one({"_id": oid, "status": {"$in": ["waiting", "failed"]}})
    return JSONResponse(content={"status": "ok", "message": "Eliminado de la cola"}, headers={"Cache-Control": "no-store, no-cache, must-revalidate"})


@app.post("/api/la-primitiva/betting/bot/claim")
def api_la_primitiva_betting_bot_claim():
    """For the La Primitiva bot (another device). Claim one waiting queue item; set in_progress and return it."""
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    coll = db[LA_PRIMITIVA_BUY_QUEUE_COLLECTION]
    doc = coll.find_one({"status": "waiting"}, sort=[("created_at", 1)])
    if not doc:
        return JSONResponse(
            content={"claimed": False, "queue_id": None, "tickets": [], "draw_date": None, "cutoff_draw_id": None},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    oid = doc["_id"]
    updated = coll.update_one(
        {"_id": oid, "status": "waiting"},
        {"$set": {"status": "in_progress", "started_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}},
    )
    if updated.modified_count == 0:
        return JSONResponse(
            content={"claimed": False, "queue_id": None, "tickets": [], "draw_date": None, "cutoff_draw_id": None},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    tickets = doc.get("tickets") or []
    draw_date = (doc.get("draw_date") or "").strip()[:10] or None
    cutoff_draw_id = (doc.get("cutoff_draw_id") or "").strip() or None
    return JSONResponse(
        content={
            "claimed": True,
            "queue_id": str(oid),
            "tickets": tickets,
            "draw_date": draw_date,
            "cutoff_draw_id": cutoff_draw_id,
        },
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.post("/api/la-primitiva/betting/bot/complete")
async def api_la_primitiva_betting_bot_complete(request: Request):
    """For the La Primitiva bot. Report result: body { "queue_id", "success", "error"?. }."""
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    try:
        body = await request.json() or {}
    except Exception:
        body = {}
    queue_id = body.get("queue_id")
    success = body.get("success") is True
    error = (body.get("error") or "").strip() or None
    if not queue_id:
        raise HTTPException(400, detail="queue_id required")
    try:
        oid = ObjectId(queue_id)
    except Exception:
        raise HTTPException(400, detail="Invalid queue_id")
    coll = db[LA_PRIMITIVA_BUY_QUEUE_COLLECTION]
    doc = coll.find_one({"_id": oid}, projection={"status": 1})
    if not doc:
        raise HTTPException(404, detail="Queue item not found")
    if doc.get("status") != "in_progress":
        return JSONResponse(
            content={"status": "ok", "message": "Job already completed or not in progress"},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    if success:
        coll.update_one(
            {"_id": oid},
            {"$set": {"status": "bought", "saved_status": False, "finished_at": now, "error": None}},
        )
    else:
        coll.update_one(
            {"_id": oid},
            {"$set": {"status": "failed", "error": error or "Bot reported failure", "finished_at": now}},
        )
    return JSONResponse(
        content={"status": "ok", "message": "completed"},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/api/la-primitiva/train/progress")
def api_la_primitiva_train_progress(
    cutoff_draw_id: str | None = Query(None, description="id_sorteo for this training run (La Primitiva)."),
):
    """Return La Primitiva training progress for the given cutoff_draw_id (dataset prepared, models trained)."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if not cutoff_draw_id:
        return JSONResponse(
            content={"progress": None},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    coll = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
    doc = coll.find_one({"cutoff_draw_id": cutoff_draw_id.strip()})
    if not doc:
        return JSONResponse(
            content={"progress": None},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )

    # Full wheel elapsed time (mirror Euromillones/El Gordo).
    full_status = doc.get("full_wheel_status")
    started_at = doc.get("full_wheel_started_at")
    generated_at = doc.get("full_wheel_generated_at")
    full_elapsed: Optional[int] = None
    if started_at:
        try:
            start_dt = dt.fromisoformat(str(started_at).replace("Z", "+00:00"))
            if full_status == "done" and generated_at:
                end_dt = dt.fromisoformat(str(generated_at).replace("Z", "+00:00"))
            else:
                end_dt = dt.utcnow()
            diff = end_dt - start_dt
            full_elapsed = max(0, int(diff.total_seconds()))
        except Exception:
            full_elapsed = None

    progress = {
        "cutoff_draw_id": doc.get("cutoff_draw_id"),
        "dataset_prepared": bool(doc.get("dataset_prepared")),
        "dataset_prepared_at": doc.get("dataset_prepared_at"),
        "main_rows": doc.get("main_rows"),
        "reintegro_rows": doc.get("reintegro_rows"),
        "models_trained": bool(doc.get("models_trained")),
        "trained_at": doc.get("trained_at"),
        "main_accuracy": doc.get("main_accuracy"),
        "reintegro_accuracy": doc.get("reintegro_accuracy"),
        "probs_computed": bool(doc.get("probs_computed")),
        "probs_computed_at": doc.get("probs_computed_at"),
        "mains_probs": doc.get("mains_probs"),
        "reintegro_probs": doc.get("reintegro_probs"),
        "probs_draw_id": doc.get("probs_draw_id"),
        "probs_fecha_sorteo": doc.get("probs_fecha_sorteo"),
        "rules_applied": bool(doc.get("rules_applied")),
        "rules_applied_at": doc.get("rules_applied_at"),
        "filtered_mains_probs": doc.get("filtered_mains_probs"),
        "filtered_reintegro_probs": doc.get("filtered_reintegro_probs"),
        "rule_flags": doc.get("rule_flags"),
        "candidate_pool": doc.get("candidate_pool"),
        "candidate_pool_at": doc.get("candidate_pool_at"),
        "candidate_pool_count": doc.get("candidate_pool_count"),
        "bought_tickets": doc.get("bought_tickets"),
        "full_wheel_draw_date": doc.get("full_wheel_draw_date"),
        "full_wheel_file_path": doc.get("full_wheel_file_path"),
        "full_wheel_total_tickets": doc.get("full_wheel_total_tickets"),
        "full_wheel_good_tickets": doc.get("full_wheel_good_tickets"),
        "full_wheel_bad_tickets": doc.get("full_wheel_bad_tickets"),
        "full_wheel_generated_at": doc.get("full_wheel_generated_at"),
        "full_wheel_started_at": doc.get("full_wheel_started_at"),
        "full_wheel_status": full_status,
        "full_wheel_error": doc.get("full_wheel_error"),
        "full_wheel_elapsed_seconds": full_elapsed,
        "pipeline_status": doc.get("pipeline_status"),
        "pipeline_error": doc.get("pipeline_error"),
        "pipeline_started_at": doc.get("pipeline_started_at"),
    }
    return JSONResponse(
        content={"progress": progress},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.post("/api/la-primitiva/train/progress")
def api_la_primitiva_train_progress_post(
    cutoff_draw_id: str | None = Query(None, description="id_sorteo for this training run (La Primitiva)."),
):
    """
    POST variant of la-primitiva/train/progress so the frontend can poll status
    without relying on GET semantics (used while full wheel generation is running).
    """
    return api_la_primitiva_train_progress(cutoff_draw_id=cutoff_draw_id)


@app.post("/api/el-gordo/train/prepare-dataset")
def api_el_gordo_prepare_dataset(
    cutoff_draw_id: str | None = Query(
        None,
        description="Optional id_sorteo: only draws up to this one (inclusive) are used to build the dataset.",
    ),
):
    """
    Build / refresh the El Gordo per-number training datasets from
    `el_gordo_feature`.

    This calls `scripts/train_el_gordo_model.prepare_el_gordo_dataset`
    inside the backend process and returns basic metadata so the UI can show
    where the CSV files were written. Saves progress to el_gordo_train_progress.
    """
    try:
        info = prepare_el_gordo_dataset(cutoff_draw_id=cutoff_draw_id, out_dir=None)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error while preparing El Gordo dataset: {e!s}",
        )
    if db is not None and cutoff_draw_id:
        coll = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
        now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        coll.update_one(
            {"cutoff_draw_id": cutoff_draw_id.strip()},
            {
                "$set": {
                    "cutoff_draw_id": cutoff_draw_id.strip(),
                    "dataset_prepared": True,
                    "dataset_prepared_at": now,
                    "main_rows": info.get("main_rows"),
                    "clave_rows": info.get("clave_rows"),
                }
            },
            upsert=True,
        )
    return JSONResponse(content={"status": "ok", "info": info})


@app.post("/api/la-primitiva/train/prepare-dataset")
def api_la_primitiva_prepare_dataset(
    cutoff_draw_id: str | None = Query(
        None,
        description="Optional id_sorteo: only draws up to this one (inclusive) are used to build the dataset (La Primitiva).",
    ),
):
    """
    Build / refresh the La Primitiva per-number training datasets from
    `la_primitiva_feature`.

    This calls `scripts/train_la_primitiva_model.prepare_la_primitiva_dataset`
    inside the backend process and returns basic metadata so the UI can show
    where the CSV files were written. Saves progress to la_primitiva_train_progress.
    """
    try:
        info = prepare_la_primitiva_dataset(cutoff_draw_id=cutoff_draw_id, out_dir=None)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error while preparing La Primitiva dataset: {e!s}",
        )
    if db is not None and cutoff_draw_id:
        coll = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
        now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        coll.update_one(
            {"cutoff_draw_id": cutoff_draw_id.strip()},
            {
                "$set": {
                    "cutoff_draw_id": cutoff_draw_id.strip(),
                    "dataset_prepared": True,
                    "dataset_prepared_at": now,
                    "main_rows": info.get("main_rows"),
                    "reintegro_rows": info.get("reintegro_rows"),
                }
            },
            upsert=True,
        )
    return JSONResponse(content={"status": "ok", "info": info})


@app.post("/api/el-gordo/train/models")
def api_el_gordo_train_models(
    cutoff_draw_id: str | None = Query(
        None,
        description=(
            "Optional id_sorteo: only draws up to this one (inclusive) are used "
            "for both dataset building and model training (El Gordo)."
        ),
    ),
):
    """
    Train Gradient Boosting models for El Gordo mains and clave using the
    per-number dataset derived from `el_gordo_feature`. Saves progress to el_gordo_train_progress.
    """
    try:
        info = train_el_gordo_models(cutoff_draw_id=cutoff_draw_id)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error while training El Gordo models: {e!s}",
        )
    if db is not None and cutoff_draw_id:
        coll = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
        now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        coll.update_one(
            {"cutoff_draw_id": cutoff_draw_id.strip()},
            {
                "$set": {
                    "models_trained": True,
                    "trained_at": now,
                    "main_accuracy": info.get("main_accuracy"),
                    "clave_accuracy": info.get("clave_accuracy"),
                }
            },
            upsert=True,
        )
    return JSONResponse(content={"status": "ok", "info": info})


@app.post("/api/la-primitiva/train/models")
def api_la_primitiva_train_models(
    cutoff_draw_id: str | None = Query(
        None,
        description=(
            "Optional id_sorteo: only draws up to this one (inclusive) are used "
            "for both dataset building and model training (La Primitiva)."
        ),
    ),
):
    """
    Train Gradient Boosting models for La Primitiva mains and reintegro using the
    per-number dataset derived from `la_primitiva_feature`. Saves progress to la_primitiva_train_progress.
    """
    try:
        info = train_la_primitiva_models(cutoff_draw_id=cutoff_draw_id)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error while training La Primitiva models: {e!s}",
        )
    if db is not None and cutoff_draw_id:
        coll = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
        now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        coll.update_one(
            {"cutoff_draw_id": cutoff_draw_id.strip()},
            {
                "$set": {
                    "models_trained": True,
                    "trained_at": now,
                    "main_accuracy": info.get("main_accuracy"),
                    "reintegro_accuracy": info.get("reintegro_accuracy"),
                }
            },
            upsert=True,
        )
    return JSONResponse(content={"status": "ok", "info": info})


@app.get("/api/el-gordo/prediction/ml")
def api_el_gordo_prediction_ml(
    cutoff_draw_id: str | None = Query(
        None,
        description=(
            "Optional id_sorteo: use this draw as cutoff; "
            "if omitted, use the latest draw in el_gordo_feature."
        ),
    ),
):
    """
    Step 3 (new_flow): generate per-number probabilities for the next El Gordo draw
    using the trained Gradient Boosting models.
    """
    try:
        info = compute_el_gordo_probabilities(cutoff_draw_id=cutoff_draw_id)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error while computing El Gordo ML probabilities: {e!s}",
        )

    if db is not None and info.get("cutoff_draw_id"):
        coll = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
        now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        coll.update_one(
            {"cutoff_draw_id": str(info["cutoff_draw_id"]).strip()},
            {
                "$set": {
                    "probs_computed": True,
                    "probs_computed_at": now,
                    "mains_probs": info.get("mains"),
                    "clave_probs": info.get("claves"),
                    "probs_draw_id": info.get("draw_id"),
                    "probs_fecha_sorteo": info.get("fecha_sorteo"),
                }
            },
            upsert=True,
        )

    return JSONResponse(content={"status": "ok", "info": info})


@app.get("/api/la-primitiva/prediction/ml")
def api_la_primitiva_prediction_ml(
    cutoff_draw_id: str | None = Query(
        None,
        description=(
            "Optional id_sorteo: use this draw as cutoff; "
            "if omitted, use the latest draw in la_primitiva_feature."
        ),
    ),
):
    """
    Step 3 (new_flow): generate per-number probabilities for the next La Primitiva draw
    using the trained Gradient Boosting models.
    """
    try:
        info = compute_la_primitiva_probabilities(cutoff_draw_id=cutoff_draw_id)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error while computing La Primitiva ML probabilities: {e!s}",
        )

    if db is not None and info.get("cutoff_draw_id"):
        coll = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
        now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        coll.update_one(
            {"cutoff_draw_id": str(info["cutoff_draw_id"]).strip()},
            {
                "$set": {
                    "probs_computed": True,
                    "probs_computed_at": now,
                    "mains_probs": info.get("mains"),
                    "reintegro_probs": info.get("reintegros"),
                    "probs_draw_id": info.get("draw_id"),
                    "probs_fecha_sorteo": info.get("fecha_sorteo"),
                }
            },
            upsert=True,
        )

    return JSONResponse(content={"status": "ok", "info": info})


@app.post("/api/el-gordo/train/rule-filters")
def api_el_gordo_rule_filters(
    cutoff_draw_id: str | None = Query(
        None,
        description="id_sorteo for this El Gordo training run (uses saved mains_probs/clave_probs).",
    ),
):
    """
    Step 4 (new_flow): build pool of mains + clave like Euromillones (prioritized subset then extend to full).

    - 4–5 mains + 1 clave from previous draw (pre_id_sorteo == cutoff) or random.
    - Additional mains from Step 3 ranking bands (1–20: 6–7, 21–30: 3, 31–40: 3, 41–54: 3); then extend to all 54.
    - Clave: 1 from previous + up to 3 from ranking; then extend to all 10.
    - Result: filtered_mains_probs (54 items), filtered_clave_probs (10 items) for candidate-pool and full-wheel.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if not cutoff_draw_id:
        raise HTTPException(400, detail="cutoff_draw_id is required")
    cid = cutoff_draw_id.strip()
    progress_coll = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
    doc = progress_coll.find_one({"cutoff_draw_id": cid})
    if not doc:
        raise HTTPException(404, detail="Progress not found for this cutoff_draw_id")
    if doc.get("rules_applied"):
        raise HTTPException(
            400,
            detail=(
                "Step 4 (rule filters) has already been applied for this cutoff_draw_id; "
                "pool generation cannot be regenerated once started."
            ),
        )
    mains = doc.get("mains_probs") or []
    claves = doc.get("clave_probs") or []
    if not mains and not claves:
        raise HTTPException(400, detail="Run step 3 (compute probabilities) first.")
    _apply_el_gordo_rule_filters_impl(cid)
    doc = progress_coll.find_one({"cutoff_draw_id": cid})
    rule_flags = (doc or {}).get("rule_flags") or {}
    return JSONResponse(
        content={
            "status": "ok",
            "filtered_mains": (doc or {}).get("filtered_mains_probs") or [],
            "filtered_clave": (doc or {}).get("filtered_clave_probs") or [],
            "rules_used": rule_flags.get("rules_used", []),
            "excluded": rule_flags.get("excluded", {}),
        }
    )


def _apply_el_gordo_rule_filters_impl(cid: str) -> None:
    """Step 4: apply rule filters and update progress (no HTTP, no rules_applied check)."""
    if db is None:
        return
    progress_coll = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
    feature_coll = db["el_gordo_feature"]
    doc = progress_coll.find_one({"cutoff_draw_id": cid})
    if not doc:
        return
    mains = doc.get("mains_probs") or []
    claves = doc.get("clave_probs") or []
    if not mains and not claves:
        return
    prev_main_numbers: Optional[List[int]] = None
    prev_clave: Optional[int] = None
    prev_row = feature_coll.find_one({"pre_id_sorteo": cid})
    if prev_row:
        pm = prev_row.get("main_number") or []
        prev_main_numbers = [int(x) for x in pm if isinstance(x, (int, float))]
        pc = prev_row.get("clave")
        if isinstance(pc, int):
            prev_clave = pc
    result = build_el_gordo_step4_pool(
        mains_probs=mains,
        clave_probs=claves,
        prev_main_numbers=prev_main_numbers,
        prev_clave=prev_clave,
        seed=None,
    )
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    progress_coll.update_one(
        {"cutoff_draw_id": cid},
        {
            "$set": {
                "rules_applied": True,
                "rules_applied_at": now,
                "filtered_mains_probs": result["filtered_mains"],
                "filtered_clave_probs": result["filtered_clave"],
                "rule_flags": {
                    "rules_used": result["rules_used"],
                    "excluded": result["excluded"],
                    "stats": result.get("stats"),
                    "snapshot_mains": result.get("snapshot_mains"),
                    "snapshot_clave": result.get("snapshot_clave"),
                },
            }
        },
    )


@app.post("/api/el-gordo/train/run-pipeline")
def api_el_gordo_run_pipeline(
    cutoff_draw_id: str | None = Query(None, description="id_sorteo for this El Gordo training run."),
):
    """Run steps 1–4 in the backend. Frontend only starts and polls progress."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if not cutoff_draw_id:
        raise HTTPException(400, detail="cutoff_draw_id is required")
    cid = cutoff_draw_id.strip()
    coll = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
    doc = coll.find_one({"cutoff_draw_id": cid})
    if doc and doc.get("pipeline_status") == "running":
        return JSONResponse(content={"status": "running", "cutoff_draw_id": cid})
    if doc and doc.get("rules_applied"):
        return JSONResponse(content={"status": "done", "cutoff_draw_id": cid, "message": "Pool already generated for this cutoff."})
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    coll.update_one(
        {"cutoff_draw_id": cid},
        {"$set": {"cutoff_draw_id": cid, "pipeline_status": "running", "pipeline_error": None, "pipeline_started_at": now}},
        upsert=True,
    )

    def _run() -> None:
        try:
            info = prepare_el_gordo_dataset(cutoff_draw_id=cid, out_dir=None)
            coll.update_one(
                {"cutoff_draw_id": cid},
                {"$set": {"dataset_prepared": True, "dataset_prepared_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), "main_rows": info.get("main_rows"), "clave_rows": info.get("clave_rows")}},
            )
            info = train_el_gordo_models(cutoff_draw_id=cid)
            coll.update_one(
                {"cutoff_draw_id": cid},
                {"$set": {"models_trained": True, "trained_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), "main_accuracy": info.get("main_accuracy"), "clave_accuracy": info.get("clave_accuracy")}},
            )
            info = compute_el_gordo_probabilities(cutoff_draw_id=cid)
            coll.update_one(
                {"cutoff_draw_id": cid},
                {"$set": {"probs_computed": True, "probs_computed_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), "mains_probs": info.get("mains"), "clave_probs": info.get("claves"), "probs_draw_id": info.get("draw_id"), "probs_fecha_sorteo": info.get("fecha_sorteo")}},
            )
            _apply_el_gordo_rule_filters_impl(cid)
            coll.update_one({"cutoff_draw_id": cid}, {"$set": {"pipeline_status": "done"}})
        except Exception as e:
            logging.exception("El Gordo pipeline error: %s", e)
            coll.update_one({"cutoff_draw_id": cid}, {"$set": {"pipeline_status": "error", "pipeline_error": str(e)}})

    threading.Thread(target=_run, daemon=True).start()
    return JSONResponse(content={"status": "started", "cutoff_draw_id": cid})


@app.post("/api/el-gordo/train/candidate-pool")
def api_el_gordo_candidate_pool(
    cutoff_draw_id: str | None = Query(
        None,
        description="id_sorteo for this El Gordo training run.",
    ),
    num_tickets: int = Query(3000, ge=100, le=10000),
):
    """
    Step 5 (new_flow): generate candidate ticket pool from Step 4 pool (mirror Euromillones).

    Uses filtered_mains_probs and filtered_clave_probs from Step 4 (54 mains, 10 claves after extend).
    Each ticket: 5 distinct mains from pool, 1 clave (round-robin). C(54,5)*10 possible; we take up to num_tickets.
    Saves candidate_pool (list of {mains, clave}) and count to el_gordo_train_progress.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if not cutoff_draw_id:
        raise HTTPException(400, detail="cutoff_draw_id is required")
    cid = cutoff_draw_id.strip()
    coll = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
    doc = coll.find_one({"cutoff_draw_id": cid})
    if not doc:
        raise HTTPException(404, detail="Progress not found for this cutoff_draw_id")

    status = doc.get("full_wheel_status")
    if status in {"waiting"}:
        # Another generation is already running; ignore this request.
        print(
            f"[la-prim-fullwheel] skip start for cutoff_draw_id={cid!r} because status={status!r}",
            flush=True,
        )
        return JSONResponse(
            content={
                "status": "waiting",
                "cutoff_draw_id": cid,
                "message": "Full wheel generation already in progress for this cutoff_draw_id.",
            }
        )
    if status == "done" and doc.get("full_wheel_file_path"):
        print(
            f"[la-prim-fullwheel] skip regenerate for cutoff_draw_id={cid!r} because file already exists",
            flush=True,
        )
        return JSONResponse(
            content={
                "status": "done",
                "cutoff_draw_id": cid,
                "draw_date": doc.get("full_wheel_draw_date"),
                "file_path": doc.get("full_wheel_file_path"),
                "total_tickets": doc.get("full_wheel_total_tickets"),
                "good_tickets": doc.get("full_wheel_good_tickets"),
                "bad_tickets": doc.get("full_wheel_bad_tickets"),
            }
        )

    filtered_mains = doc.get("filtered_mains_probs") or []
    filtered_clave = doc.get("filtered_clave_probs") or []
    if not filtered_mains or len(filtered_mains) < 5:
        raise HTTPException(
            400,
            detail="Run step 4 (rule filters) first to get mains pool (need at least 5).",
        )
    if not filtered_clave:
        raise HTTPException(
            400,
            detail="Run step 4 (rule filters) first to get clave pool (need at least 1).",
        )

    main_nums = [
        int(x.get("number") or 0)
        for x in filtered_mains
        if x.get("number") is not None
    ]
    clave_nums = [
        int(x.get("number") or 0)
        for x in filtered_clave
        if x.get("number") is not None
    ]
    if len(set(main_nums)) < 5:
        raise HTTPException(
            400,
            detail="Pool too small: need at least 5 distinct mains.",
        )
    if not clave_nums:
        raise HTTPException(
            400,
            detail="Pool too small: need at least 1 clave.",
        )

    # Build tickets from Step 4 pool: C(mains,5) × clave round-robin (same pattern as Euromillones).
    unique_mains_combos = list(combinations(sorted(set(main_nums)), 5))
    random.shuffle(unique_mains_combos)
    max_mains = min(len(unique_mains_combos), num_tickets)
    mains_combos = unique_mains_combos[:max_mains]

    random.shuffle(clave_nums)
    tickets: List[Dict] = []
    for idx, mains in enumerate(mains_combos):
        clave = clave_nums[idx % len(clave_nums)]
        tickets.append({"mains": list(mains), "clave": int(clave)})

    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    coll.update_one(
        {"cutoff_draw_id": cid},
        {
            "$set": {
                "candidate_pool": tickets,
                "candidate_pool_at": now,
                "candidate_pool_count": len(tickets),
            }
        },
    )
    return JSONResponse(
        content={"status": "ok", "candidate_pool_count": len(tickets)},
    )


def _apply_la_primitiva_rule_filters_impl(cid: str) -> None:
    """Step 4: apply rule filters and update progress (no HTTP, no rules_applied check)."""
    if db is None:
        return
    progress_coll = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
    feature_coll = db["la_primitiva_feature"]
    doc = progress_coll.find_one({"cutoff_draw_id": cid})
    if not doc:
        return
    mains = doc.get("mains_probs") or []
    reintegros = doc.get("reintegro_probs") or []
    if not mains and not reintegros:
        return
    prev_main_numbers: Optional[List[int]] = None
    prev_reintegro: Optional[int] = None
    prev_row = feature_coll.find_one({"pre_id_sorteo": cid})
    if prev_row:
        pm = prev_row.get("main_number") or []
        prev_main_numbers = [int(x) for x in pm if isinstance(x, (int, float))]
        rein_val = prev_row.get("reintegro")
        if isinstance(rein_val, (int, float)):
            prev_reintegro = int(rein_val)
    result = build_la_primitiva_step4_pool(
        mains_probs=mains,
        reintegro_probs=reintegros,
        prev_main_numbers=prev_main_numbers,
        prev_reintegro=prev_reintegro,
        seed=None,
    )
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    progress_coll.update_one(
        {"cutoff_draw_id": cid},
        {
            "$set": {
                "rules_applied": True,
                "rules_applied_at": now,
                "filtered_mains_probs": result["filtered_mains"],
                "filtered_reintegro_probs": result["filtered_reintegro"],
                "rule_flags": {
                    "rules_used": result["rules_used"],
                    "excluded": result["excluded"],
                    "stats": result.get("stats"),
                    "snapshot_mains": result.get("snapshot_mains"),
                    "snapshot_reintegro": result.get("snapshot_reintegro"),
                },
            }
        },
    )


@app.post("/api/la-primitiva/train/run-pipeline")
def api_la_primitiva_run_pipeline(
    cutoff_draw_id: str | None = Query(None, description="id_sorteo for this La Primitiva training run."),
):
    """Run steps 1–4 in the backend. Frontend only starts and polls progress."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if not cutoff_draw_id:
        raise HTTPException(400, detail="cutoff_draw_id is required")
    cid = cutoff_draw_id.strip()
    coll = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
    doc = coll.find_one({"cutoff_draw_id": cid})
    if doc and doc.get("pipeline_status") == "running":
        return JSONResponse(content={"status": "running", "cutoff_draw_id": cid})
    if doc and doc.get("rules_applied"):
        return JSONResponse(content={"status": "done", "cutoff_draw_id": cid, "message": "Pool already generated for this cutoff."})
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    coll.update_one(
        {"cutoff_draw_id": cid},
        {"$set": {"cutoff_draw_id": cid, "pipeline_status": "running", "pipeline_error": None, "pipeline_started_at": now}},
        upsert=True,
    )

    def _run() -> None:
        try:
            info = prepare_la_primitiva_dataset(cutoff_draw_id=cid, out_dir=None)
            coll.update_one(
                {"cutoff_draw_id": cid},
                {"$set": {"dataset_prepared": True, "dataset_prepared_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), "main_rows": info.get("main_rows"), "reintegro_rows": info.get("reintegro_rows")}},
            )
            info = train_la_primitiva_models(cutoff_draw_id=cid)
            coll.update_one(
                {"cutoff_draw_id": cid},
                {"$set": {"models_trained": True, "trained_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), "main_accuracy": info.get("main_accuracy"), "reintegro_accuracy": info.get("reintegro_accuracy")}},
            )
            info = compute_la_primitiva_probabilities(cutoff_draw_id=cid)
            coll.update_one(
                {"cutoff_draw_id": cid},
                {"$set": {"probs_computed": True, "probs_computed_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), "mains_probs": info.get("mains"), "reintegro_probs": info.get("reintegros"), "probs_draw_id": info.get("draw_id"), "probs_fecha_sorteo": info.get("fecha_sorteo")}},
            )
            _apply_la_primitiva_rule_filters_impl(cid)
            coll.update_one({"cutoff_draw_id": cid}, {"$set": {"pipeline_status": "done"}})
        except Exception as e:
            logging.exception("La Primitiva pipeline error: %s", e)
            coll.update_one({"cutoff_draw_id": cid}, {"$set": {"pipeline_status": "error", "pipeline_error": str(e)}})

    threading.Thread(target=_run, daemon=True).start()
    return JSONResponse(content={"status": "started", "cutoff_draw_id": cid})


@app.post("/api/la-primitiva/train/rule-filters")
def api_la_primitiva_rule_filters(
    cutoff_draw_id: str | None = Query(
        None,
        description="id_sorteo for this La Primitiva training run (uses saved mains_probs/reintegro_probs).",
    ),
):
    """
    Step 4 (new_flow): build pool of main numbers + reintegro numbers for La Primitiva.
    - 4–5 mains + 1 reintegro from row where pre_id_sorteo == current id_sorteo, or random if not found.
    - Remaining mains from Step 3 ranking bands (1–20: 6–8, 21–35: 4, 36–49: 4); pool capped at 20.
    - Reintegro pool: snapshot reintegro + up to 5 additional candidates from Step 3 ranking, no duplicate.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if not cutoff_draw_id:
        raise HTTPException(400, detail="cutoff_draw_id is required")
    cid = cutoff_draw_id.strip()
    progress_coll = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
    doc = progress_coll.find_one({"cutoff_draw_id": cid})
    if not doc:
        raise HTTPException(404, detail="Progress not found for this cutoff_draw_id")
    if doc.get("rules_applied"):
        raise HTTPException(
            400,
            detail=(
                "Step 4 (rule filters) has already been applied for this cutoff_draw_id; "
                "pool generation cannot be regenerated once started."
            ),
        )
    mains = doc.get("mains_probs") or []
    reintegros = doc.get("reintegro_probs") or []
    if not mains and not reintegros:
        raise HTTPException(400, detail="Run step 3 (compute probabilities) first.")
    _apply_la_primitiva_rule_filters_impl(cid)
    doc = progress_coll.find_one({"cutoff_draw_id": cid})
    rule_flags = (doc or {}).get("rule_flags") or {}
    return JSONResponse(
        content={
            "status": "ok",
            "filtered_mains": (doc or {}).get("filtered_mains_probs") or [],
            "filtered_reintegro": (doc or {}).get("filtered_reintegro_probs") or [],
            "rules_used": rule_flags.get("rules_used", []),
            "excluded": rule_flags.get("excluded", {}),
        }
    )


@app.post("/api/la-primitiva/train/candidate-pool")
def api_la_primitiva_candidate_pool(
    cutoff_draw_id: str | None = Query(
        None,
        description="id_sorteo for this La Primitiva training run.",
    ),
    num_tickets: int = Query(3000, ge=100, le=10000),
):
    """
    Step 5 (new_flow): generate candidate ticket pool for La Primitiva from Step 4 pool.

    - Uses filtered_mains_probs (mains pool) and filtered_reintegro_probs (reintegro pool)
      produced by Step 4.
    - Each ticket: 6 distinct mains from the mains pool, 1 reintegro from the pool
      (assigned in round-robin order).
    - Saves candidate_pool (list of {mains, reintegro}) and count to la_primitiva_train_progress.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if not cutoff_draw_id:
        raise HTTPException(400, detail="cutoff_draw_id is required")
    cid = cutoff_draw_id.strip()
    coll = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
    doc = coll.find_one({"cutoff_draw_id": cid})
    if not doc:
        raise HTTPException(404, detail="Progress not found for this cutoff_draw_id")

    status = doc.get("full_wheel_status")
    if status == "waiting":
        print(
            f"[la-prim-fullwheel] skip start for cutoff_draw_id={cid!r} because status={status!r}",
            flush=True,
        )
        return JSONResponse(
            content={
                "status": "waiting",
                "cutoff_draw_id": cid,
                "message": "Full wheel generation already in progress for this cutoff_draw_id.",
            }
        )
    if status == "done" and doc.get("full_wheel_file_path"):
        print(
            f"[la-prim-fullwheel] skip regenerate for cutoff_draw_id={cid!r} because file already exists",
            flush=True,
        )
        return JSONResponse(
            content={
                "status": "done",
                "cutoff_draw_id": cid,
                "draw_date": doc.get("full_wheel_draw_date"),
                "file_path": doc.get("full_wheel_file_path"),
                "total_tickets": doc.get("full_wheel_total_tickets"),
                "good_tickets": doc.get("full_wheel_good_tickets"),
                "bad_tickets": doc.get("full_wheel_bad_tickets"),
            }
        )

    filtered_mains = doc.get("filtered_mains_probs") or []
    filtered_rein = doc.get("filtered_reintegro_probs") or []
    if not filtered_mains or len(filtered_mains) < 6:
        raise HTTPException(
            400,
            detail="Run step 4 (rule filters) first to get mains pool (need at least 6).",
        )
    if not filtered_rein:
        raise HTTPException(
            400,
            detail="Run step 4 (rule filters) first to get reintegro pool (need at least 1).",
        )

    main_nums = [
        int(x.get("number") or 0)
        for x in filtered_mains
        if x.get("number") is not None
    ]
    rein_nums = [
        int(x.get("number") or 0)
        for x in filtered_rein
        if x.get("number") is not None
    ]
    main_set = sorted(set(main_nums))
    rein_set = sorted(set(rein_nums))
    if len(main_set) < 6:
        raise HTTPException(
            400,
            detail="Pool too small: need at least 6 distinct mains.",
        )
    if not rein_set:
        raise HTTPException(
            400,
            detail="Pool too small: need at least 1 reintegro.",
        )

    # Generate mains combinations (6 numbers) in a structured way (no duplicates),
    # then pair them with reintegros in round-robin fashion.
    unique_mains_combos = list(combinations(main_set, 6))
    random.shuffle(unique_mains_combos)
    max_mains = min(len(unique_mains_combos), num_tickets)
    mains_combos = unique_mains_combos[:max_mains]

    random.shuffle(rein_set)
    tickets: List[Dict] = []
    for idx, mains in enumerate(mains_combos):
        rein = rein_set[idx % len(rein_set)]
        tickets.append({"mains": list(mains), "reintegro": int(rein)})

    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    coll.update_one(
        {"cutoff_draw_id": cid},
        {
            "$set": {
                "candidate_pool": tickets,
                "candidate_pool_at": now,
                "candidate_pool_count": len(tickets),
            }
        },
    )
    return JSONResponse(
        content={"status": "ok", "candidate_pool_count": len(tickets)},
    )


@app.post("/api/la-primitiva/train/full-wheel")
def api_la_primitiva_full_wheel(
    cutoff_draw_id: str | None = Query(
        None,
        description="id_sorteo for this La Primitiva training run (uses filtered_mains_probs).",
    ),
    draw_date: str | None = Query(
        None,
        description="Optional draw date YYYY-MM-DD for the TXT filename; falls back to probs_fecha_sorteo.",
    ),
):
    """
    Generate full La Primitiva wheeling file from the Step 4 pool (49 mains).

    - Reads filtered_mains_probs from la_primitiva_train_progress.
    - Builds the ordered mains pool (49 numbers, prioritized first, then the rest).
    - Generates ALL tickets C(49,6) in a manifold order using the pool order.
    - Applies structural rules via _la_primitiva_ticket_tier to group tickets
      into tiers (good → bad) and writes them in that order to TXT.
    - Saves stats and file path into la_primitiva_train_progress.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if not cutoff_draw_id:
        raise HTTPException(400, detail="cutoff_draw_id is required")

    cid = cutoff_draw_id.strip()
    coll = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
    # Use atomic reservation: only proceed if no other process is in "waiting".
    doc = coll.find_one({"cutoff_draw_id": cid})
    if not doc:
        raise HTTPException(404, detail="Progress not found for this cutoff_draw_id")

    status = doc.get("full_wheel_status")
    if status == "waiting":
        print(
            f"[la-prim-fullwheel] skip start for cutoff_draw_id={cid!r} because status={status!r}",
            flush=True,
        )
        return JSONResponse(
            content={
                "status": "waiting",
                "cutoff_draw_id": cid,
                "message": "Full wheel generation already in progress for this cutoff_draw_id.",
            }
        )
    if status == "done" and doc.get("full_wheel_file_path"):
        print(
            f"[la-prim-fullwheel] skip regenerate for cutoff_draw_id={cid!r} because file already exists",
            flush=True,
        )
        return JSONResponse(
            content={
                "status": "done",
                "cutoff_draw_id": cid,
                "draw_date": doc.get("full_wheel_draw_date"),
                "file_path": doc.get("full_wheel_file_path"),
                "total_tickets": doc.get("full_wheel_total_tickets"),
                "good_tickets": doc.get("full_wheel_good_tickets"),
                "bad_tickets": doc.get("full_wheel_bad_tickets"),
            }
        )

    filtered_mains = doc.get("filtered_mains_probs") or []
    if not filtered_mains or len(filtered_mains) < 6:
        raise HTTPException(
            400,
            detail="Run step 4 (Generar pool) first so filtered_mains_probs has at least 6 numbers.",
        )

    mains_pool = [int(x.get("number") or 0) for x in filtered_mains if x.get("number") is not None]
    if len(set(mains_pool)) < 6:
        raise HTTPException(
            400,
            detail="Pool too small to build La Primitiva tickets (need at least 6 distinct mains).",
        )

    date_str = (draw_date or "").strip()
    if not date_str:
        fecha = (doc.get("probs_fecha_sorteo") or "").strip()
        date_str = fecha.split(" ")[0] or fecha
    if not date_str:
        last_draw_date = _get_last_draw_date("la-primitiva") or ""
        date_str = last_draw_date.strip()[:10]
    if not date_str:
        raise HTTPException(400, detail="Cannot determine draw date for filename.")

    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    # Reserve generation slot atomically: only transition to waiting if not already waiting.
    updated = coll.update_one(
        {"cutoff_draw_id": cid, "full_wheel_status": {"$ne": "waiting"}},
        {
            "$set": {
                "full_wheel_status": "waiting",
                "full_wheel_error": None,
                "full_wheel_draw_date": date_str,
                "full_wheel_file_path": None,
                "full_wheel_total_tickets": 0,
                "full_wheel_good_tickets": 0,
                "full_wheel_bad_tickets": 0,
                "full_wheel_generated_at": None,
                "full_wheel_started_at": now,
            }
        },
    )
    if updated.matched_count == 0:
        # Someone else set status=waiting between our read and update.
        print(
            f"[la-prim-fullwheel] concurrent start detected for cutoff_draw_id={cid!r}; another process already reserved generation.",
            flush=True,
        )
        return JSONResponse(
            content={
                "status": "waiting",
                "cutoff_draw_id": cid,
                "message": "Full wheel generation already in progress for this cutoff_draw_id.",
            }
        )

    def _run_full_wheel() -> None:
        try:
            stats = _generate_la_primitiva_full_wheel_file(
                mains_pool=mains_pool,
                draw_date=date_str,
            )
            finished_at = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            coll.update_one(
                {"cutoff_draw_id": cid},
                {
                    "$set": {
                        "full_wheel_status": "done",
                        "full_wheel_draw_date": stats["draw_date"],
                        "full_wheel_file_path": stats["file_path"],
                        "full_wheel_total_tickets": stats["total_tickets"],
                        "full_wheel_good_tickets": stats["good_tickets"],
                        "full_wheel_bad_tickets": stats["bad_tickets"],
                        "full_wheel_generated_at": finished_at,
                    }
                },
            )
        except Exception as e:
            logging.exception("Error generating La Primitiva full wheel: %s", e)
            coll.update_one(
                {"cutoff_draw_id": cid},
                {
                    "$set": {
                        "full_wheel_status": "error",
                        "full_wheel_error": str(e),
                    }
                },
            )

    thread = threading.Thread(target=_run_full_wheel, name=f"la_prim_full_wheel_{cid}", daemon=True)
    thread.start()

    return JSONResponse(
        content={
            "status": "started",
            "cutoff_draw_id": cid,
            "draw_date": date_str,
        }
    )

@app.post("/api/euromillones/train/models")
def api_euromillones_train_models(
    cutoff_draw_id: str | None = Query(
        None,
        description=(
            "Optional id_sorteo: only draws up to this one (inclusive) are used "
            "for both dataset building and model training."
        ),
    ),
):
    """
    Train Gradient Boosting models for Euromillones mains and stars using the
    per-number dataset derived from `euromillones_feature`. Saves progress to euromillones_train_progress.
    """
    try:
        info = train_euromillones_models(cutoff_draw_id=cutoff_draw_id)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error while training Euromillones models: {e!s}",
        )
    if db is not None and cutoff_draw_id:
        coll = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
        now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        coll.update_one(
            {"cutoff_draw_id": cutoff_draw_id.strip()},
            {
                "$set": {
                    "models_trained": True,
                    "trained_at": now,
                    "main_accuracy": info.get("main_accuracy"),
                    "star_accuracy": info.get("star_accuracy"),
                }
            },
            upsert=True,
        )
    return JSONResponse(content={"status": "ok", "info": info})


@app.get("/api/euromillones/prediction/ml")
def api_euromillones_prediction_ml(
    cutoff_draw_id: str | None = Query(
        None,
        description=(
            "Optional id_sorteo: use this draw as cutoff; "
            "if omitted, use the latest draw in euromillones_feature."
        ),
    ),
):
    """
    Step 3 (new_flow): generate per-number probabilities for the next Euromillones draw
    using the trained Gradient Boosting models.
    """
    try:
        info = compute_euromillones_probabilities(cutoff_draw_id=cutoff_draw_id)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error while computing Euromillones ML probabilities: {e!s}",
        )

    if db is not None and info.get("cutoff_draw_id"):
        coll = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
        now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        coll.update_one(
            {"cutoff_draw_id": str(info["cutoff_draw_id"]).strip()},
            {
                "$set": {
                    "probs_computed": True,
                    "probs_computed_at": now,
                    "mains_probs": info.get("mains"),
                    "stars_probs": info.get("stars"),
                    "probs_draw_id": info.get("draw_id"),
                    "probs_fecha_sorteo": info.get("fecha_sorteo"),
                }
            },
            upsert=True,
        )

    return JSONResponse(content={"status": "ok", "info": info})


def _apply_euromillones_rule_filters_impl(cid: str) -> None:
    """Step 4: apply rule filters and update progress (no HTTP, no rules_applied check). Caller ensures db and doc exist."""
    if db is None:
        return
    progress_coll = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
    feature_coll = db["euromillones_feature"]
    doc = progress_coll.find_one({"cutoff_draw_id": cid})
    if not doc:
        return
    mains = doc.get("mains_probs") or []
    stars = doc.get("stars_probs") or []
    if not mains and not stars:
        return
    prev_main_numbers: Optional[List[int]] = None
    prev_star_numbers: Optional[List[int]] = None
    prev_row = feature_coll.find_one({"pre_id_sorteo": cid})
    if prev_row:
        pm = prev_row.get("main_number") or []
        ps = prev_row.get("star_number") or []
        prev_main_numbers = [int(x) for x in pm if isinstance(x, (int, float))]
        prev_star_numbers = [int(x) for x in ps if isinstance(x, (int, float))]
    result = build_step4_pool(
        mains_probs=mains,
        stars_probs=stars,
        prev_main_numbers=prev_main_numbers,
        prev_star_numbers=prev_star_numbers,
        seed=None,
    )
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    progress_coll.update_one(
        {"cutoff_draw_id": cid},
        {
            "$set": {
                "rules_applied": True,
                "rules_applied_at": now,
                "filtered_mains_probs": result["filtered_mains"],
                "filtered_stars_probs": result["filtered_stars"],
                "rule_flags": {
                    "rules_used": result["rules_used"],
                    "excluded": result["excluded"],
                    "stats": result.get("stats"),
                    "snapshot_mains": result.get("snapshot_mains"),
                    "snapshot_stars": result.get("snapshot_stars"),
                },
            }
        },
    )


@app.post("/api/euromillones/train/run-pipeline")
def api_euromillones_run_pipeline(
    cutoff_draw_id: str | None = Query(None, description="id_sorteo for this training run."),
):
    """
    Run steps 1–4 (prepare dataset, train models, compute probs, rule filters) in the backend.
    Frontend only starts this and polls GET /api/euromillones/train/progress. No per-step requests.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if not cutoff_draw_id:
        raise HTTPException(400, detail="cutoff_draw_id is required")
    cid = cutoff_draw_id.strip()
    coll = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
    doc = coll.find_one({"cutoff_draw_id": cid})
    if doc and doc.get("pipeline_status") == "running":
        return JSONResponse(content={"status": "running", "cutoff_draw_id": cid})
    if doc and doc.get("rules_applied"):
        return JSONResponse(
            content={"status": "done", "cutoff_draw_id": cid, "message": "Pool already generated for this cutoff."}
        )
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    coll.update_one(
        {"cutoff_draw_id": cid},
        {
            "$set": {
                "cutoff_draw_id": cid,
                "pipeline_status": "running",
                "pipeline_error": None,
                "pipeline_started_at": now,
            }
        },
        upsert=True,
    )

    def _run() -> None:
        try:
            info = prepare_euromillones_dataset(cutoff_draw_id=cid, out_dir=None)
            coll.update_one(
                {"cutoff_draw_id": cid},
                {
                    "$set": {
                        "dataset_prepared": True,
                        "dataset_prepared_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "main_rows": info.get("main_rows"),
                        "star_rows": info.get("star_rows"),
                    }
                },
            )
            info = train_euromillones_models(cutoff_draw_id=cid)
            coll.update_one(
                {"cutoff_draw_id": cid},
                {
                    "$set": {
                        "models_trained": True,
                        "trained_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "main_accuracy": info.get("main_accuracy"),
                        "star_accuracy": info.get("star_accuracy"),
                    }
                },
            )
            info = compute_euromillones_probabilities(cutoff_draw_id=cid)
            coll.update_one(
                {"cutoff_draw_id": cid},
                {
                    "$set": {
                        "probs_computed": True,
                        "probs_computed_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "mains_probs": info.get("mains"),
                        "stars_probs": info.get("stars"),
                        "probs_draw_id": info.get("draw_id"),
                        "probs_fecha_sorteo": info.get("fecha_sorteo"),
                    }
                },
            )
            _apply_euromillones_rule_filters_impl(cid)
            coll.update_one(
                {"cutoff_draw_id": cid},
                {"$set": {"pipeline_status": "done"}},
            )
        except Exception as e:
            logging.exception("Euromillones pipeline error: %s", e)
            coll.update_one(
                {"cutoff_draw_id": cid},
                {"$set": {"pipeline_status": "error", "pipeline_error": str(e)}},
            )

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return JSONResponse(content={"status": "started", "cutoff_draw_id": cid})


@app.post("/api/euromillones/train/rule-filters")
def api_euromillones_rule_filters(
    cutoff_draw_id: str | None = Query(
        None,
        description="id_sorteo for this training run (uses saved mains_probs/stars_probs).",
    ),
):
    """
    Step 4 (new_flow): build pool of 20 main numbers + 4 star numbers.
    - 3 mains + 1 star from row where pre_id_sorteo == current id_sorteo, or random if not found.
    - 17 mains from Step 3 ranking bands (1–20: 8, 21–30: 3, 31–40: 3, 41–50: 3), no duplicate.
    - 3 stars from Step 3 star ranking (1–6: 2, 7–12: 1), no duplicate.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if not cutoff_draw_id:
        raise HTTPException(400, detail="cutoff_draw_id is required")
    cid = cutoff_draw_id.strip()
    progress_coll = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
    doc = progress_coll.find_one({"cutoff_draw_id": cid})
    if not doc:
        raise HTTPException(404, detail="Progress not found for this cutoff_draw_id")
    if doc.get("rules_applied"):
        raise HTTPException(
            400,
            detail=(
                "Step 4 (rule filters) has already been applied for this cutoff_draw_id; "
                "pool generation cannot be regenerated once started."
            ),
        )
    mains = doc.get("mains_probs") or []
    stars = doc.get("stars_probs") or []
    if not mains and not stars:
        raise HTTPException(400, detail="Run step 3 (compute probabilities) first.")
    _apply_euromillones_rule_filters_impl(cid)
    doc = progress_coll.find_one({"cutoff_draw_id": cid})
    result_fm = (doc or {}).get("filtered_mains_probs") or []
    result_fs = (doc or {}).get("filtered_stars_probs") or []
    rule_flags = (doc or {}).get("rule_flags") or {}
    return JSONResponse(
        content={
            "status": "ok",
            "filtered_mains": result_fm,
            "filtered_stars": result_fs,
            "rules_used": rule_flags.get("rules_used", []),
            "excluded": rule_flags.get("excluded", {}),
        }
    )


@app.post("/api/euromillones/train/full-wheel")
def api_euromillones_full_wheel(
    cutoff_draw_id: str | None = Query(
        None,
        description="id_sorteo for this training run (uses filtered_mains_probs / filtered_stars_probs).",
    ),
    draw_date: str | None = Query(
        None,
        description="Optional draw date YYYY-MM-DD for the TXT filename; falls back to probs_fecha_sorteo.",
    ),
):
    """
    Generate full Euromillones wheeling file from the Step 4 pool (50 mains, 12 stars).

    - Reads filtered_mains_probs and filtered_stars_probs from euromillones_train_progress.
    - Builds the ordered mains/stars pools.
    - Generates ALL tickets (full wheel) using the current pool order.
    - Applies structural rules to classify tickets as "good" or "bad":
        * Long consecutive runs (>= 4)
        * All same last digit or same decade
        * All odd or all even
      Good tickets are written first in the file, bad tickets at the end.
    - Saves to euromillones_<draw_date>.txt under the project-level euromillones_pools directory.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if not cutoff_draw_id:
        raise HTTPException(400, detail="cutoff_draw_id is required")

    cid = cutoff_draw_id.strip()
    coll = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
    doc = coll.find_one({"cutoff_draw_id": cid})
    if not doc:
        raise HTTPException(404, detail="Progress not found for this cutoff_draw_id")

    filtered_mains = doc.get("filtered_mains_probs") or []
    filtered_stars = doc.get("filtered_stars_probs") or []
    if not filtered_mains or len(filtered_mains) < 5:
        raise HTTPException(
            400,
            detail="Run step 4 (Generar pool) first so filtered_mains_probs has at least 5 numbers.",
        )
    if not filtered_stars or len(filtered_stars) < 2:
        raise HTTPException(
            400,
            detail="Run step 4 (Generar pool) first so filtered_stars_probs has at least 2 numbers.",
        )

    mains_pool = [int(x.get("number") or 0) for x in filtered_mains if x.get("number") is not None]
    stars_pool = [int(x.get("number") or 0) for x in filtered_stars if x.get("number") is not None]

    if len(mains_pool) < 5 or len(stars_pool) < 2:
        raise HTTPException(
            400,
            detail="Pool too small to build Euromillones tickets (need at least 5 mains and 2 stars).",
        )

    date_str = (draw_date or "").strip()
    if not date_str:
        fecha = (doc.get("probs_fecha_sorteo") or "").strip()
        date_str = fecha.split(" ")[0] or fecha
    if not date_str:
        last_draw_date = _get_last_draw_date("euromillones") or ""
        date_str = last_draw_date.strip()[:10]
    if not date_str:
        raise HTTPException(400, detail="Cannot determine draw date for filename.")

    # Mark status as waiting/running and launch background job so it survives UI refresh.
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    coll.update_one(
        {"cutoff_draw_id": cid},
        {
            "$set": {
                "full_wheel_status": "waiting",
                "full_wheel_error": None,
                "full_wheel_draw_date": date_str,
                "full_wheel_file_path": None,
                "full_wheel_total_tickets": 0,
                "full_wheel_good_tickets": 0,
                "full_wheel_bad_tickets": 0,
                "full_wheel_generated_at": None,
                "full_wheel_started_at": now,
            }
        },
    )

    def _run_full_wheel() -> None:
        try:
            stats = _generate_full_wheel_file_from_pool(
                mains_pool=mains_pool,
                stars_pool=stars_pool,
                draw_date=date_str,
            )
            finished_at = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            coll.update_one(
                {"cutoff_draw_id": cid},
                {
                    "$set": {
                        "full_wheel_status": "done",
                        "full_wheel_draw_date": stats["draw_date"],
                        "full_wheel_file_path": stats["file_path"],
                        "full_wheel_total_tickets": stats["total_tickets"],
                        "full_wheel_good_tickets": stats["good_tickets"],
                        "full_wheel_bad_tickets": stats["bad_tickets"],
                        "full_wheel_generated_at": finished_at,
                    }
                },
            )
        except Exception as e:
            logging.exception("Error generating Euromillones full wheel: %s", e)
            coll.update_one(
                {"cutoff_draw_id": cid},
                {
                    "$set": {
                        "full_wheel_status": "error",
                        "full_wheel_error": str(e),
                    }
                },
            )

    t = threading.Thread(target=_run_full_wheel, daemon=True)
    t.start()

    return JSONResponse(
        content={
            "status": "started",
            "cutoff_draw_id": cid,
            "draw_date": date_str,
        }
    )


@app.post("/api/el-gordo/train/full-wheel")
def api_el_gordo_full_wheel(
    cutoff_draw_id: str = Query(
        ...,
        description="id_sorteo for this El Gordo training run; generates full-wheel TXT (54 mains, 10 claves).",
    ),
    draw_date: str | None = Query(
        None,
        description="Optional draw date YYYY-MM-DD for the TXT filename; falls back to probs_fecha_sorteo.",
    ),
):
    """
    Generate full El Gordo wheeling file from Step 4 pool (mirror Euromillones).

    - Uses filtered_mains_probs and filtered_stars_probs from Step 4 (54 mains, 10 claves after extend).
    - Generates ALL tickets: C(54,5) * 10 = 31,625,100; classifies by tier (good/bad), writes good first.
    - Saves to el_gordo_<draw_date>.txt under el_gordo_pools. Persists full_wheel_* to progress.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    cid = cutoff_draw_id.strip()
    if not cid:
        raise HTTPException(400, detail="cutoff_draw_id is required")

    coll = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
    doc = coll.find_one({"cutoff_draw_id": cid})
    if not doc:
        raise HTTPException(404, detail="Progress not found for this cutoff_draw_id")

    filtered_mains = doc.get("filtered_mains_probs") or []
    filtered_clave = doc.get("filtered_clave_probs") or []
    if not filtered_mains or not filtered_clave:
        raise HTTPException(
            400,
            detail="Run step 4 (Generar pool) first so filtered_mains_probs/filtered_clave_probs are populated.",
        )

    # Preserve Step 4 pool order (mirror Euromillones full wheel).
    mains_pool = [
        int(x.get("number") or 0)
        for x in filtered_mains
        if x.get("number") is not None
    ]
    clave_pool = [
        int(x.get("number") or 0)
        for x in filtered_clave
        if x.get("number") is not None
    ]
    if len(mains_pool) < 5 or not clave_pool:
        raise HTTPException(
            400,
            detail="Pool too small to build El Gordo tickets (need at least 5 mains and 1 clave).",
        )

    date_str = (draw_date or "").strip()
    if not date_str:
        fecha = (doc.get("probs_fecha_sorteo") or "").strip()
        date_str = fecha.split(" ")[0] or fecha
    if not date_str:
        last_draw_date = _get_last_draw_date("el-gordo") or ""
        date_str = last_draw_date.strip()[:10]
    if not date_str:
        raise HTTPException(400, detail="Cannot determine draw date for filename.")

    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    coll.update_one(
        {"cutoff_draw_id": cid},
        {
            "$set": {
                "full_wheel_status": "waiting",
                "full_wheel_error": None,
                "full_wheel_draw_date": date_str,
                "full_wheel_file_path": None,
                "full_wheel_total_tickets": 0,
                "full_wheel_good_tickets": 0,
                "full_wheel_bad_tickets": 0,
                "full_wheel_generated_at": None,
                "full_wheel_started_at": now,
            }
        },
        upsert=True,
    )

    def _run_el_gordo_full_wheel() -> None:
        try:
            stats = _generate_el_gordo_full_wheel_file(
                mains_pool=mains_pool,
                clave_pool=clave_pool,
                draw_date=date_str,
            )
            finished_at = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            coll.update_one(
                {"cutoff_draw_id": cid},
                {
                    "$set": {
                        "full_wheel_status": "done",
                        "full_wheel_draw_date": stats["draw_date"],
                        "full_wheel_file_path": stats["file_path"],
                        "full_wheel_total_tickets": stats["total_tickets"],
                        "full_wheel_good_tickets": stats.get("good_tickets", 0),
                        "full_wheel_bad_tickets": stats.get("bad_tickets", 0),
                        "full_wheel_generated_at": finished_at,
                    }
                },
            )
        except Exception as e:
            logging.exception("Error generating El Gordo full wheel: %s", e)
            coll.update_one(
                {"cutoff_draw_id": cid},
                {
                    "$set": {
                        "full_wheel_status": "error",
                        "full_wheel_error": str(e),
                    }
                },
            )

    t = threading.Thread(target=_run_el_gordo_full_wheel, daemon=True)
    t.start()

    return JSONResponse(
        content={
            "status": "started",
            "cutoff_draw_id": cid,
            "draw_date": date_str,
        },
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/api/el-gordo/train/full-wheel-preview")
def api_el_gordo_full_wheel_preview(
    cutoff_draw_id: str = Query(..., description="id_sorteo for this training run."),
    limit: int = Query(20, ge=1, le=200),
):
    """
    Return a small preview of the full-wheeling ticket file for El Gordo (mirror Euromillones).

    - Reads full_wheel_file_path from el_gordo_train_progress.
    - Streams the first `limit` lines from the TXT file.
    - Each line is parsed into { position, mains, clave }.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    coll = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
    doc = coll.find_one({"cutoff_draw_id": cutoff_draw_id.strip()})
    if not doc:
        raise HTTPException(404, detail="Progress not found for this cutoff_draw_id")

    path = (doc.get("full_wheel_file_path") or "").strip()
    if not path:
        return JSONResponse(
            content={"tickets": [], "file_path": None, "total_tickets": doc.get("full_wheel_total_tickets") or 0},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    try:
        tickets = []
        with open(path, "r", encoding="utf-8") as f:
            for _ in range(limit):
                line = f.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    pos_str, mains_str, clave_str = line.split(";")
                    position = int(pos_str)
                    mains = [int(x) for x in mains_str.split(",") if x]
                    clave = int(clave_str)
                except Exception:
                    continue
                tickets.append(
                    {
                        "position": position,
                        "mains": mains,
                        "clave": clave,
                    }
                )
        return JSONResponse(
            content={
                "tickets": tickets,
                "file_path": path,
                "total_tickets": doc.get("full_wheel_total_tickets") or 0,
            },
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    except FileNotFoundError:
        raise HTTPException(404, detail="Full wheel file not found on disk")


@app.get("/api/euromillones/train/full-wheel-preview")
def api_euromillones_full_wheel_preview(
    cutoff_draw_id: str = Query(..., description="id_sorteo for this training run."),
    limit: int = Query(20, ge=1, le=200),
):
    """
    Return a small preview of the full-wheeling ticket file for Euromillones.

    - Reads full_wheel_file_path from euromillones_train_progress.
    - Streams the first `limit` lines from the TXT file.
    - Each line is parsed into { position, mains, stars }.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    coll = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
    doc = coll.find_one({"cutoff_draw_id": cutoff_draw_id.strip()})
    if not doc:
        raise HTTPException(404, detail="Progress not found for this cutoff_draw_id")

    path = (doc.get("full_wheel_file_path") or "").strip()
    if not path:
        return JSONResponse(
            content={"tickets": [], "file_path": None, "total_tickets": doc.get("full_wheel_total_tickets") or 0},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    try:
        tickets = []
        with open(path, "r", encoding="utf-8") as f:
            for _ in range(limit):
                line = f.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    pos_str, mains_str, stars_str = line.split(";")
                    position = int(pos_str)
                    mains = [int(x) for x in mains_str.split(",") if x]
                    stars = [int(x) for x in stars_str.split(",") if x]
                except Exception:
                    continue
                tickets.append(
                    {
                        "position": position,
                        "mains": mains,
                        "stars": stars,
                    }
                )
        return JSONResponse(
            content={
                "tickets": tickets,
                "file_path": path,
                "total_tickets": doc.get("full_wheel_total_tickets") or 0,
            },
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    except FileNotFoundError:
        raise HTTPException(404, detail="Full wheel file not found on disk")


@app.post("/api/euromillones/train/candidate-pool")
def api_euromillones_candidate_pool(
    cutoff_draw_id: str | None = Query(None, description="id_sorteo for this training run."),
    num_tickets: int = Query(3000, ge=100, le=10000),
):
    """
    Step 5: generate candidate ticket pool from Step 4 number pool (20 mains, 4 stars).
    Each ticket: 5 distinct mains from the 20, 2 distinct stars from the 4.
    Saves candidate_pool (list of {mains, stars}) and count to progress.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if not cutoff_draw_id:
        raise HTTPException(400, detail="cutoff_draw_id is required")
    cid = cutoff_draw_id.strip()
    coll = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
    doc = coll.find_one({"cutoff_draw_id": cid})
    if not doc:
        raise HTTPException(404, detail="Progress not found for this cutoff_draw_id")
    filtered_mains = doc.get("filtered_mains_probs") or []
    filtered_stars = doc.get("filtered_stars_probs") or []
    if not filtered_mains or len(filtered_mains) < 5:
        raise HTTPException(400, detail="Run step 4 (rule filters) first to get 20 mains pool.")
    if not filtered_stars or len(filtered_stars) < 2:
        raise HTTPException(400, detail="Run step 4 to get 4 stars pool.")
    main_nums = [int(x.get("number") or 0) for x in filtered_mains if x.get("number") is not None]
    star_nums = [int(x.get("number") or 0) for x in filtered_stars if x.get("number") is not None]
    if len(main_nums) < 5 or len(star_nums) < 2:
        raise HTTPException(400, detail="Pool too small: need at least 5 mains and 2 stars.")

    # Step 5: build a simple wheeling system from the Step 4 pool.
    # Generate mains combinations in a structured way (no duplicates),
    # then pair them with star pairs in round-robin fashion.
    unique_mains_combos = list(combinations(sorted(set(main_nums)), 5))
    random.shuffle(unique_mains_combos)
    max_mains = min(len(unique_mains_combos), num_tickets)
    mains_combos = unique_mains_combos[:max_mains]

    star_pairs = list(combinations(sorted(set(star_nums)), 2))
    if not star_pairs:
        raise HTTPException(400, detail="Pool too small: need at least 2 distinct stars.")
    random.shuffle(star_pairs)

    tickets: List[Dict] = []
    for idx, mains in enumerate(mains_combos):
        pair = star_pairs[idx % len(star_pairs)]
        tickets.append({"mains": list(mains), "stars": list(pair)})

    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    coll.update_one(
        {"cutoff_draw_id": cid},
        {
            "$set": {
                "candidate_pool": tickets,
                "candidate_pool_at": now,
                "candidate_pool_count": len(tickets),
            }
        },
    )


@app.get("/api/la-primitiva/train/full-wheel-preview")
def api_la_primitiva_full_wheel_preview(
    cutoff_draw_id: str = Query(..., description="id_sorteo for this training run."),
    limit: int = Query(20, ge=1, le=200),
):
    """
    Return a small preview of the full-wheeling ticket file for La Primitiva.

    - Reads full_wheel_file_path from la_primitiva_train_progress.
    - Streams the first `limit` lines from the TXT file.
    - Each line is parsed into { position, mains }.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    coll = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
    doc = coll.find_one({"cutoff_draw_id": cutoff_draw_id.strip()})
    if not doc:
        raise HTTPException(404, detail="Progress not found for this cutoff_draw_id")
    path = (doc.get("full_wheel_file_path") or "").strip()
    if not path:
        raise HTTPException(400, detail="Full wheel file not generated for this cutoff_draw_id")
    if not os.path.isfile(path):
        raise HTTPException(404, detail="Full wheel file not found on disk")

    tickets: List[Dict] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    pos_str, mains_str = line.split(";")
                    position = int(pos_str)
                    mains = [int(x) for x in mains_str.split(",") if x]
                except Exception:
                    continue
                tickets.append({"position": position, "mains": mains})
                if len(tickets) >= limit:
                    break
    except FileNotFoundError:
        raise HTTPException(404, detail="Full wheel file not found on disk")

    return JSONResponse(
        content={
            "cutoff_draw_id": cutoff_draw_id.strip(),
            "tickets": tickets,
        }
    )
    return JSONResponse(
        content={"status": "ok", "candidate_pool_count": len(tickets)},
    )


@app.post("/api/euromillones/train/generate-30-mains")
def api_euromillones_generate_30_mains(
    cutoff_draw_id: str | None = Query(
        None,
        description="id_sorteo for this training run.",
    ),
):
    """
    Step 5: generate exactly 30 main numbers from the pool (filtered if step 4 was run,
    else top 30 by probability). Saves generated_30_mains to progress.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if not cutoff_draw_id:
        raise HTTPException(400, detail="cutoff_draw_id is required")
    coll = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
    doc = coll.find_one({"cutoff_draw_id": cutoff_draw_id.strip()})
    if not doc:
        raise HTTPException(404, detail="Progress not found for this cutoff_draw_id")
    mains_probs = doc.get("mains_probs") or []
    filtered = doc.get("filtered_mains_probs") or []
    if not mains_probs:
        raise HTTPException(
            400,
            detail="Run step 3 (compute probabilities) first.",
        )
    # Prefer filtered pool (step 4) if available, else use full mains_probs
    pool = filtered if filtered else mains_probs
    sorted_pool = sorted(pool, key=lambda x: (x.get("p") or 0), reverse=True)
    numbers_so_far: List[int] = []
    for item in sorted_pool:
        n = item.get("number")
        if n is not None and int(n) not in numbers_so_far:
            numbers_so_far.append(int(n))
        if len(numbers_so_far) >= 30:
            break
    # If we have fewer than 30 (e.g. after consecutive filter), fill from mains_probs by prob
    by_num = {int(x.get("number")): x for x in mains_probs if x.get("number") is not None}
    for item in sorted(mains_probs, key=lambda x: (x.get("p") or 0), reverse=True):
        if len(numbers_so_far) >= 30:
            break
        n = int(item.get("number"))
        if n not in numbers_so_far:
            numbers_so_far.append(n)
    generated = numbers_so_far[:30]
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    coll.update_one(
        {"cutoff_draw_id": cutoff_draw_id.strip()},
        {
            "$set": {
                "generated_30_mains": generated,
                "generated_30_mains_at": now,
            }
        },
    )
    return JSONResponse(
        content={"status": "ok", "generated_30_mains": generated},
    )


@app.get("/api/el-gordo/feature-model")
def get_el_gordo_feature_model(
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
    draw_id: str | None = Query(
        None,
        description="Optional: filter by id_sorteo to get one feature row.",
    ),
):
    """
    Return rows from `el_gordo_feature` (new per-draw feature model: 5 mains + clave, frequency/gap arrays).
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["el_gordo_feature"]

    if draw_id:
        cursor = coll.find({"id_sorteo": draw_id})
        docs = [_doc_to_json(doc) for doc in cursor]
        return JSONResponse(content={"features": docs, "total": len(docs)})

    total = coll.count_documents({})
    cursor = coll.find().sort("fecha_sorteo", -1).skip(skip).limit(limit)
    docs = [_doc_to_json(doc) for doc in cursor]
    return JSONResponse(content={"features": docs, "total": total})


@app.get("/api/la-primitiva/feature-model")
def get_la_primitiva_feature_model(
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
    draw_id: str | None = Query(
        None,
        description="Optional: filter by id_sorteo to get one feature row.",
    ),
):
    """
    Return rows from `la_primitiva_feature` (new per-draw feature model: mains + complementario + reintegro).
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["la_primitiva_feature"]

    if draw_id:
        cursor = coll.find({"id_sorteo": draw_id})
        docs = [_doc_to_json(doc) for doc in cursor]
        return JSONResponse(content={"features": docs, "total": len(docs)})

    total = coll.count_documents({})
    cursor = coll.find().sort("fecha_sorteo", -1).skip(skip).limit(limit)
    docs = [_doc_to_json(doc) for doc in cursor]
    return JSONResponse(content={"features": docs, "total": total})


@app.get("/api/la-primitiva/features")
def get_la_primitiva_features(
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
    draw_id: str | None = Query(
        None,
        description="Optional: filter by draw_id to get a specific feature row.",
    ),
):
    """
    Return per-draw La Primitiva feature rows from `la_primitiva_draw_features`.

    Each document contains:
      - main_numbers, complementario, reintegro
      - draw_date, weekday
      - hot/cold numbers
      - frequency arrays
      - previous-draw snapshot fields
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["la_primitiva_draw_features"]

    if draw_id:
        cursor = coll.find({"draw_id": draw_id})
        docs = [_doc_to_json(doc) for doc in cursor]
        total = len(docs)
        return JSONResponse(content={"features": docs, "total": total})

    total = coll.count_documents({})
    cursor = coll.find().sort("draw_date", -1).skip(skip).limit(limit)
    docs = [_doc_to_json(doc) for doc in cursor]

    return JSONResponse(content={"features": docs, "total": total})


@app.get("/api/el-gordo/features")
def get_el_gordo_features(
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
    draw_id: str | None = Query(
        None,
        description="Optional: filter by draw_id to get a specific feature row.",
    ),
):
    """
    Return per-draw El Gordo feature rows from `el_gordo_draw_features`.

    Each document contains:
      - main_numbers (5), clave (0-9)
      - draw_date, weekday
      - hot/cold numbers
      - frequency arrays
      - previous-draw snapshot fields
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["el_gordo_draw_features"]

    if draw_id:
        cursor = coll.find({"draw_id": draw_id})
        docs = [_doc_to_json(doc) for doc in cursor]
        total = len(docs)
        return JSONResponse(content={"features": docs, "total": total})

    total = coll.count_documents({})
    cursor = coll.find().sort("draw_date", -1).skip(skip).limit(limit)
    docs = [_doc_to_json(doc) for doc in cursor]

    return JSONResponse(content={"features": docs, "total": total})


@app.get("/api/euromillones/gaps")
def get_euromillones_gaps(
    type: str = Query("main", pattern="^(main|star)$"),
    end_date: str | None = Query(
        None,
        description="YYYY-MM-DD. If not provided, uses today.",
    ),
    window_days: int = Query(
        31,
        ge=1,
        le=365,
        description="Number of days to include ending at end_date.",
    ),
):
    """
    Return per-number appearance history for Euromillones.

    Response format:
      {
        "points": [
          { "type": "main", "number": 3, "draw_index": 12, "date": "2026-01-24" },
          ...
        ]
      }
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    from datetime import datetime, timedelta

    coll = db["euromillones_number_history"]
    docs = list(coll.find({"type": type}))

    points: list[dict] = []

    # Determine time window
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(400, detail="Invalid end_date format, expected YYYY-MM-DD")
    else:
        end_dt = datetime.utcnow()
    start_dt = end_dt - timedelta(days=window_days)

    for doc in docs:
        number = doc.get("number")
        appearances = doc.get("appearances") or []
        for appo in appearances:
            date_str = (appo.get("date") or "").split(" ")[0]
            if not date_str:
                continue
            try:
                app_dt = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                continue
            if not (start_dt <= app_dt <= end_dt):
                continue
            points.append(
                {
                    "type": type,
                    "number": number,
                    "draw_index": appo.get("draw_index"),
                    "date": date_str,
                }
            )

    # Sort points by date ascending so charts have ordered Y-axis
    points.sort(key=lambda p: p.get("date") or "")

    return JSONResponse(content={"points": points})


@app.get("/api/euromillones/apuestas")
def get_euromillones_apuestas(
    window: str = Query(
        "3m",
        pattern="^(2m|3m|6m|1y|all)$",
        description="Time window: last 2m, 3m, 6m, 1y or all history.",
    ),
):
    """
    Time series for Euromillones apuestas / premios / premio_bote.

    Returns draws ordered ascending by fecha_sorteo. Each point:
      {
        "draw_id": "...",
        "date": "YYYY-MM-DD",
        "apuestas": int | null,
        "premios": float | null,
        "premio_bote": float | null
      }
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    from datetime import datetime, timedelta

    coll = db["euromillones"]

    # Determine date window
    if window == "all":
        query: dict = {}
    else:
        today = datetime.utcnow().date()
        if window == "2m":
            delta_days = 60
        elif window == "3m":
            delta_days = 90
        elif window == "6m":
            delta_days = 180
        else:  # "1y"
            delta_days = 365
        start_date = today - timedelta(days=delta_days)
        query = {
            "fecha_sorteo": {
                "$gte": start_date.strftime("%Y-%m-%d") + " 00:00:00",
                "$lte": today.strftime("%Y-%m-%d") + " 23:59:59",
            }
        }

    cursor = coll.find(
        query,
        projection={
            "id_sorteo": 1,
            "fecha_sorteo": 1,
            "apuestas": 1,
            "aquestas": 1,
            "premio_bote": 1,
            "premios": 1,
        },
    ).sort("fecha_sorteo", 1)

    points: list[dict] = []
    for doc in cursor:
        draw_id = str(doc.get("id_sorteo"))
        fecha_full = (doc.get("fecha_sorteo") or "").strip()
        if not draw_id or not fecha_full:
            continue
        date = fecha_full.split(" ")[0]

        raw_apuestas = doc.get("apuestas")
        if raw_apuestas in (None, ""):
            raw_apuestas = doc.get("aquestas")
        try:
            apuestas = int(str(raw_apuestas).replace(".", "").replace(",", "")) if raw_apuestas not in (None, "") else None
        except Exception:
            apuestas = None

        def _to_float(val):
            if val in (None, ""):
                return None
            try:
                s = str(val).replace(".", "").replace(",", ".")
                return float(s)
            except Exception:
                return None

        premios = _to_float(doc.get("premios"))
        if premios is not None:
            # Valores de premios vienen 100x; normalizar a euros reales
            premios = premios / 100.0
        premio_bote = _to_float(doc.get("premio_bote"))

        points.append(
            {
                "draw_id": draw_id,
                "date": date,
                "apuestas": apuestas,
                "premios": premios,
                "premio_bote": premio_bote,
            }
        )

    return JSONResponse(content={"points": points})


def _apuestas_time_series_for_lottery(lottery_slug: str, window: str):
    """
    Helper to build apuestas / premios / premio_bote time series for a given lottery.
    lottery_slug: 'la-primitiva' | 'el-gordo'
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    from datetime import datetime, timedelta

    game_id = GAME_IDS.get(lottery_slug)
    if not game_id:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery_slug}")
    coll_name = COLLECTIONS.get(game_id)
    if not coll_name:
        raise HTTPException(400, detail=f"No collection for lottery: {lottery_slug}")

    coll = db[coll_name]

    # Determine date window
    if window == "all":
        query: dict = {}
    else:
        today = datetime.utcnow().date()
        if window == "2m":
            delta_days = 60
        elif window == "3m":
            delta_days = 90
        elif window == "6m":
            delta_days = 180
        else:  # "1y"
            delta_days = 365
        start_date = today - timedelta(days=delta_days)
        query = {
            "fecha_sorteo": {
                "$gte": start_date.strftime("%Y-%m-%d") + " 00:00:00",
                "$lte": today.strftime("%Y-%m-%d") + " 23:59:59",
            }
        }

    cursor = coll.find(
        query,
        projection={
            "id_sorteo": 1,
            "fecha_sorteo": 1,
            "apuestas": 1,
            "recaudacion": 1,
            "premio_bote": 1,
            "premios": 1,
        },
    ).sort("fecha_sorteo", 1)

    def _to_float(val):
        if val in (None, ""):
            return None
        try:
            s = str(val).replace(".", "").replace(",", ".")
            return float(s)
        except Exception:
            return None

    points: list[dict] = []
    for doc in cursor:
        draw_id = str(doc.get("id_sorteo"))
        fecha_full = (doc.get("fecha_sorteo") or "").strip()
        if not draw_id or not fecha_full:
            continue
        date = fecha_full.split(" ")[0]

        raw_apuestas = doc.get("apuestas")
        try:
            apuestas = int(str(raw_apuestas).replace(".", "").replace(",", "")) if raw_apuestas not in (None, "") else None
        except Exception:
            apuestas = None

        premios = _to_float(doc.get("premios"))
        if premios is not None:
            premios = premios / 100.0
        premio_bote = _to_float(doc.get("premio_bote"))

        points.append(
            {
                "draw_id": draw_id,
                "date": date,
                "apuestas": apuestas,
                "premios": premios,
                "premio_bote": premio_bote,
            }
        )

    return points


def _money_to_float(val) -> Optional[float]:
    """
    Convert Spanish-formatted money string/number to float.

    Examples:
      "1.234.567,89" -> 1234567.89
      "123.456"      -> 123456.0
    """
    if val in (None, ""):
        return None
    try:
        s = str(val).replace(".", "").replace(",", ".")
        return float(s)
    except Exception:
        return None


def _weekday_name(idx: int) -> str:
    names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    return names[idx] if 0 <= idx < 7 else str(idx)


def _predict_next_funds_for_lottery(lottery_slug: str) -> Dict[str, object]:
    """
    Predict only Premios (total premios) for the next draw.
    Próx. bote is not predicted; it comes from scraping the results page.

    Algorithm:
      - Resolve next_draw_date from scraper_metadata (or from last_draw_date / DB).
      - Take historical draws on the same weekday; build premios distribution.
      - Return median and summary stats for premios only.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    game_id = GAME_IDS.get(lottery_slug)
    coll_name = COLLECTIONS.get(game_id) if game_id else None
    if not game_id or not coll_name:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery_slug}")

    meta = db[METADATA_COLLECTION].find_one(
        {"lottery": lottery_slug}, projection={"last_draw_date": 1, "next_draw_date": 1}
    )
    next_date = (meta or {}).get("next_draw_date")
    last_date = (meta or {}).get("last_draw_date")

    if not next_date:
        if isinstance(last_date, str) and last_date:
            next_date = _compute_next_draw_date(lottery_slug, last_date)
        if not next_date:
            latest = _get_max_draw_date_from_db(game_id)
            if latest:
                next_date = _compute_next_draw_date(lottery_slug, latest)
    if not next_date:
        raise HTTPException(400, detail="No se ha podido determinar el próximo sorteo.")

    try:
        next_dt = dt.strptime(next_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, detail=f"Fecha próxima sorteo inválida: {next_date}")

    target_wd = next_dt.weekday()

    coll = db[coll_name]
    cursor = coll.find(
        {},
        projection={"fecha_sorteo": 1, "premios": 1},
    )

    premios_values: List[float] = []

    for doc in cursor:
        fecha_full = (doc.get("fecha_sorteo") or "").strip()
        if not fecha_full:
            continue
        date_str = fecha_full.split(" ")[0]
        try:
            d_date = dt.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        if d_date.weekday() != target_wd:
            continue

        premios_val = _money_to_float(doc.get("premios"))
        if premios_val is not None:
            premios_val = premios_val / 100.0
            premios_values.append(premios_val)

    if not premios_values:
        raise HTTPException(
            400,
            detail=(
                "No hay históricos suficientes para este día de la semana "
                "para calcular una predicción de premios."
            ),
        )

    def _summary(arr: List[float]) -> Dict[str, float]:
        if not arr:
            return {}
        arr_sorted = sorted(arr)
        n = len(arr_sorted)
        mid = n // 2
        if n % 2 == 1:
            median_val = arr_sorted[mid]
        else:
            median_val = 0.5 * (arr_sorted[mid - 1] + arr_sorted[mid])

        def _percentile(p: float) -> float:
            if n == 1:
                return arr_sorted[0]
            k = (n - 1) * p
            f = int(k)
            c = min(f + 1, n - 1)
            if f == c:
                return arr_sorted[f]
            return arr_sorted[f] + (arr_sorted[c] - arr_sorted[f]) * (k - f)

        return {
            "count": float(n),
            "mean": float(mean(arr_sorted)),
            "median": float(median_val),
            "p10": float(_percentile(0.10)),
            "p25": float(_percentile(0.25)),
            "p75": float(_percentile(0.75)),
            "p90": float(_percentile(0.90)),
        }

    premios_stats = _summary(premios_values)

    return {
        "lottery": lottery_slug,
        "next_draw_date": next_date,
        "weekday_index": target_wd,
        "weekday_name": _weekday_name(target_wd),
        "premios_stats": premios_stats,
    }


def _update_next_funds_metadata(
    lottery_slug: str,
    scraped_bote: Optional[float] = None,
) -> Dict[str, object]:
    """
    Persist next-funds as two values in scraper_metadata: next_bote, next_premios.
    Próx. bote: from scraping only (when provided). Premios: from prediction (median).
    Returns a dict with bote_stats.median and premios_stats.median for API compatibility.
    """
    next_bote: Optional[float] = scraped_bote
    next_premios: Optional[float] = None
    next_draw_date: Optional[str] = None

    if db is not None:
        current = db[METADATA_COLLECTION].find_one(
            {"lottery": lottery_slug},
            projection={"next_bote": 1, "next_premios": 1},
        )
        if next_bote is None and current:
            next_bote = current.get("next_bote")
            if isinstance(next_bote, (int, float)):
                next_bote = float(next_bote)
            else:
                next_bote = None

    try:
        pred = _predict_next_funds_for_lottery(lottery_slug)
        premios_stats = pred.get("premios_stats") or {}
        next_premios = premios_stats.get("median")
        next_draw_date = pred.get("next_draw_date")
        if next_premios is not None:
            next_premios = float(next_premios)
    except HTTPException:
        pass

    if db is not None:
        update_doc: dict = {
            "next_funds_updated_at": dt.utcnow(),
        }
        if next_premios is not None:
            update_doc["next_premios"] = next_premios
        if scraped_bote is not None:
            update_doc["next_bote"] = scraped_bote
        db[METADATA_COLLECTION].update_one(
            {"lottery": lottery_slug},
            {"$set": update_doc},
            upsert=True,
        )

    return {
        "lottery": lottery_slug,
        "next_draw_date": next_draw_date,
        "bote_stats": {"median": next_bote} if next_bote is not None else {},
        "premios_stats": {"median": next_premios} if next_premios is not None else {},
    }


@app.get("/api/la-primitiva/apuestas")
def get_la_primitiva_apuestas(
    window: str = Query(
        "3m",
        pattern="^(2m|3m|6m|1y|all)$",
        description="Time window: last 2m, 3m, 6m, 1y or all history.",
    ),
):
    points = _apuestas_time_series_for_lottery("la-primitiva", window)
    return JSONResponse(content={"points": points})


@app.get("/api/el-gordo/apuestas")
def get_el_gordo_apuestas(
    window: str = Query(
        "3m",
        pattern="^(2m|3m|6m|1y|all)$",
        description="Time window: last 2m, 3m, 6m, 1y or all history.",
    ),
):
    points = _apuestas_time_series_for_lottery("el-gordo", window)
    return JSONResponse(content={"points": points})


@app.get("/api/euromillones/prediction/next-funds")
def predict_euromillones_next_funds():
    """
    Return next-funds metadata: Próx. bote from last scrape (no prediction);
    Premios from weekday-conditioned prediction.
    """
    result = _update_next_funds_metadata("euromillones")
    return JSONResponse(content=_item_to_json(result))


@app.get("/api/la-primitiva/prediction/next-funds")
def predict_la_primitiva_next_funds():
    """
    Return next-funds metadata: Próx. bote from last scrape (no prediction);
    Premios from weekday-conditioned prediction.
    """
    result = _update_next_funds_metadata("la-primitiva")
    return JSONResponse(content=_item_to_json(result))


@app.get("/api/el-gordo/prediction/next-funds")
def predict_el_gordo_next_funds():
    """
    Return next-funds metadata: Próx. bote from last scrape (no prediction);
    Premios from weekday-conditioned prediction.
    """
    result = _update_next_funds_metadata("el-gordo")
    return JSONResponse(content=_item_to_json(result))

@app.get("/api/euromillones/number-history")
def get_euromillones_number_history():
    """
    Return full per-number appearance history for Euromillones, grouped by type.

    Response format:
      {
        "main": [
          { "number": 1, "dates": ["2026-01-13", "2026-01-27", ...] },
          ...
        ],
        "star": [
          { "number": 1, "dates": ["2026-01-20", ...] },
          ...
        ]
      }
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["euromillones_number_history"]

    docs = list(
        coll.find(
            {},
            projection={
                "_id": 0,
                "type": 1,
                "number": 1,
                "appearances.date": 1,
            },
        )
    )

    main: list[dict] = []
    star: list[dict] = []

    for doc in docs:
        t = doc.get("type")
        number = doc.get("number")
        appearances = doc.get("appearances") or []

        dates_set: set[str] = set()
        for appo in appearances:
            date_str = (appo.get("date") or "").split(" ")[0]
            if date_str:
                dates_set.add(date_str)

        dates = sorted(dates_set)

        target = main if t == "main" else star if t == "star" else None
        if target is not None:
            target.append({"number": number, "dates": dates})

    return JSONResponse(content={"main": main, "star": star})


@app.post("/api/euromillones/simulation/frequency/train")
def train_euromillones_frequency_models():
    """
    Train / retrain Euromillones frequency-based models (main + stars).

    This can be triggered from the UI. It runs synchronously and may take
    some seconds depending on history size.
    """
    try:
        train_all_frequency_models()
    except Exception as e:
        raise HTTPException(500, detail=f"Error training frequency models: {e}")
    return {"status": "ok"}


@app.get("/api/euromillones/simulation/frequency")
def simulate_euromillones_frequency(
    cutoff_draw_id: str | None = Query(
        None,
        description="Optional draw_id; if provided, simulate as of the draw after this one.",
    )
):
    """
    Run Euromillones frequency-based simulation for the next draw.

    Returns probability per number for mains (1–50) and stars (1–12),
    sorted descending by probability.
    """
    try:
        scores = predict_next_frequency_scores(cutoff_draw_id=cutoff_draw_id)
        sim_id = save_frequency_simulation_result(scores)
    except RuntimeError as e:
        raise HTTPException(500, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=f"Error running frequency simulation: {e}")

    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["euromillones_simulations"]
    from bson import ObjectId  # type: ignore[import-not-found]

    doc = coll.find_one({"_id": ObjectId(sim_id)})
    if not doc:
        raise HTTPException(500, detail="Saved frequency simulation not found")

    public = _doc_to_json(doc)
    public["simulation_id"] = sim_id
    return JSONResponse(content=public)


@app.get("/api/euromillones/simulation/frequency/history")
def get_euromillones_frequency_simulation_history(
    cutoff_draw_id: str | None = Query(
        None,
        description="Optional draw_id to filter simulations by cutoff_draw_id.",
    ),
    limit: int = Query(
        1,
        ge=1,
        le=50,
        description="Maximum number of simulation records to return.",
    ),
):
    """
    Return saved Euromillones frequency simulations.

    If cutoff_draw_id is provided, only simulations for that draw are returned,
    ordered from newest to oldest.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["euromillones_simulations"]
    query: dict = {}
    if cutoff_draw_id:
        query["cutoff_draw_id"] = cutoff_draw_id

    cursor = coll.find(query).sort("created_at", -1).limit(limit)
    docs = [_doc_to_json(doc) for doc in cursor]
    return JSONResponse(content={"simulations": docs})


@app.post("/api/el-gordo/simulation/frequency/train")
def train_el_gordo_frequency_models():
    """
    Train / retrain El Gordo frequency-based models (main + clave).
    """
    try:
        train_all_el_gordo_frequency_models()
    except Exception as e:
        raise HTTPException(500, detail=f"Error training El Gordo frequency models: {e}")
    return {"status": "ok"}


@app.get("/api/el-gordo/simulation/frequency")
def simulate_el_gordo_frequency(
    cutoff_draw_id: str | None = Query(
        None,
        description=(
            "Optional draw_id; if provided, simulate as of the draw after this one."
        ),
    )
):
    """
    Run El Gordo frequency-based simulation for the next draw.
    """
    try:
        scores = predict_next_el_gordo_frequency_scores(cutoff_draw_id=cutoff_draw_id)
        sim_id = save_el_gordo_frequency_simulation_result(scores)
    except RuntimeError as e:
        raise HTTPException(500, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=f"Error running El Gordo frequency simulation: {e}")

    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["el_gordo_simulations"]
    from bson import ObjectId  # type: ignore[import-not-found]

    doc = coll.find_one({"_id": ObjectId(sim_id)})
    if not doc:
        raise HTTPException(500, detail="Saved El Gordo frequency simulation not found")

    public = _doc_to_json(doc)
    public["simulation_id"] = sim_id
    return JSONResponse(content=public)


@app.get("/api/el-gordo/simulation/frequency/history")
def get_el_gordo_frequency_simulation_history(
    cutoff_draw_id: str | None = Query(
        None,
        description="Optional draw_id to filter El Gordo simulations by cutoff_draw_id.",
    ),
    limit: int = Query(
        1,
        ge=1,
        le=50,
        description="Maximum number of simulation records to return.",
    ),
):
    """
    Return saved El Gordo frequency simulations.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["el_gordo_simulations"]
    query: dict = {}
    if cutoff_draw_id:
        query["cutoff_draw_id"] = cutoff_draw_id

    cursor = coll.find(query).sort("created_at", -1).limit(limit)
    docs = [_doc_to_json(doc) for doc in cursor]
    return JSONResponse(content={"simulations": docs})


@app.get("/api/el-gordo/simulation/simple")
def simulate_el_gordo_simple(
    cutoff_draw_id: str | None = Query(
        None,
        description=(
            "Optional draw_id; if provided, simulate El Gordo as of the draw after this one."
        ),
    )
):
    """
    Run a simple El Gordo simulation (frequency / gap / hot-cold) for the next draw.

    This uses `el_gordo_draw_features` to compute per-number scores and stores the result
    in `el_gordo_simulations` so that the candidate pool and wheeling engines can reuse it.
    """
    try:
        sim_doc = run_el_gordo_simple_simulation(cutoff_draw_id=cutoff_draw_id)
    except RuntimeError as e:
        raise HTTPException(400, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=f"Error running El Gordo simulation: {e}")

    return JSONResponse(content=_doc_to_json(sim_doc))


@app.post("/api/el-gordo/simulation/gap/train")
def train_el_gordo_gap_models():
    """
    Train / retrain El Gordo gap-based models (main + clave).
    """
    try:
        train_all_el_gordo_gap_models()
    except Exception as e:
        raise HTTPException(500, detail=f"Error training El Gordo gap models: {e}")
    return {"status": "ok"}


@app.get("/api/el-gordo/simulation/gap")
def simulate_el_gordo_gap(
    cutoff_draw_id: str | None = Query(
        None,
        description=(
            "Optional draw_id; if provided, simulate as of the draw after this one."
        ),
    )
):
    """
    Run El Gordo gap-based simulation for the next draw.
    """
    try:
        scores = predict_next_el_gordo_gap_scores(cutoff_draw_id=cutoff_draw_id)
        sim_id = save_el_gordo_gap_simulation_result(scores)
    except RuntimeError as e:
        raise HTTPException(500, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=f"Error running El Gordo gap simulation: {e}")

    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["el_gordo_simulations"]
    from bson import ObjectId  # type: ignore[import-not-found]

    doc = coll.find_one({"_id": ObjectId(sim_id)})
    if not doc:
        raise HTTPException(500, detail="Saved El Gordo gap simulation not found")

    public = _doc_to_json(doc)
    public["simulation_id"] = sim_id
    return JSONResponse(content=public)


@app.post("/api/euromillones/simulation/gap/train")
def train_euromillones_gap_models():
    """
    Train / retrain Euromillones gap-based models (main + stars).
    """
    try:
        train_all_gap_models()
    except Exception as e:
        raise HTTPException(500, detail=f"Error training gap models: {e}")
    return {"status": "ok"}


@app.get("/api/euromillones/simulation/gap")
def simulate_euromillones_gap(
    cutoff_draw_id: str | None = Query(
        None,
        description="Optional draw_id; if provided, simulate as of the draw after this one.",
    )
):
    """
    Run Euromillones gap-based simulation for the next draw.
    """
    try:
        scores = predict_next_gap_scores(cutoff_draw_id=cutoff_draw_id)
        sim_id = save_gap_simulation_result(scores)
    except RuntimeError as e:
        raise HTTPException(500, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=f"Error running gap simulation: {e}")

    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["euromillones_simulations"]
    from bson import ObjectId  # type: ignore[import-not-found]

    doc = coll.find_one({"_id": ObjectId(sim_id)})
    if not doc:
        raise HTTPException(500, detail="Saved gap simulation not found")

    public = _doc_to_json(doc)
    public["simulation_id"] = sim_id
    return JSONResponse(content=public)


@app.post("/api/euromillones/simulation/hot/train")
def train_euromillones_hot_models():
    """
    Train / retrain Euromillones hot/cold-based models (main + stars).
    """
    try:
        train_all_hot_models()
    except Exception as e:
        raise HTTPException(500, detail=f"Error training hot/cold models: {e}")
    return {"status": "ok"}


@app.get("/api/euromillones/simulation/hot")
def simulate_euromillones_hot(
    cutoff_draw_id: str | None = Query(
        None,
        description="Optional draw_id; if provided, simulate as of the draw after this one.",
    )
):
    """
    Run Euromillones hot/cold-based simulation for the next draw.
    """
    try:
        scores = predict_next_hot_scores(cutoff_draw_id=cutoff_draw_id)
        sim_id = save_hot_simulation_result(scores)
    except RuntimeError as e:
        raise HTTPException(500, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=f"Error running hot/cold simulation: {e}")

    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["euromillones_simulations"]
    from bson import ObjectId  # type: ignore[import-not-found]

    doc = coll.find_one({"_id": ObjectId(sim_id)})
    if not doc:
        raise HTTPException(500, detail="Saved hot/cold simulation not found")

    public = _doc_to_json(doc)
    public["simulation_id"] = sim_id
    return JSONResponse(content=public)


@app.get("/api/euromillones/simulation/prediction/compare")
def compare_euromillones_prediction_with_result(
    result_draw_id: str = Query(
        ...,
        description=(
            "id_sorteo del sorteo real; se compara contra la simulación de predicción "
            "generada para el sorteo anterior (prev_draw_id)."
        ),
    ),
):
    """
    Comparar la predicción de Euromillones (freq/gap/hot) contra el resultado real.

    Devuelve métricas como número de aciertos en el top-K para mains y stars,
    así como detalles por número para los números que salieron en el sorteo real.
    """
    try:
        result = compare_prediction_with_result(result_draw_id=result_draw_id)
    except RuntimeError as e:
        raise HTTPException(400, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=f"Error comparando predicción de Euromillones: {e}")

    # Ensure datetimes and ObjectIds inside result are JSON serializable
    return JSONResponse(content=_item_to_json(result))


@app.post("/api/el-gordo/simulation/hot/train")
def train_el_gordo_hot_models():
    """
    Train / retrain El Gordo hot/cold-based models (main + clave).
    """
    try:
        train_all_el_gordo_hot_models()
    except Exception as e:
        raise HTTPException(500, detail=f"Error training El Gordo hot/cold models: {e}")
    return {"status": "ok"}


@app.get("/api/el-gordo/simulation/hot")
def simulate_el_gordo_hot(
    cutoff_draw_id: str | None = Query(
        None,
        description=(
            "Optional draw_id; if provided, simulate as of the draw after this one."
        ),
    )
):
    """
    Run El Gordo hot/cold-based simulation for the next draw.
    """
    try:
        scores = predict_next_el_gordo_hot_scores(cutoff_draw_id=cutoff_draw_id)
        sim_id = save_el_gordo_hot_simulation_result(scores)
    except RuntimeError as e:
        raise HTTPException(500, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=f"Error running El Gordo hot/cold simulation: {e}")

    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["el_gordo_simulations"]
    from bson import ObjectId  # type: ignore[import-not-found]

    doc = coll.find_one({"_id": ObjectId(sim_id)})
    if not doc:
        raise HTTPException(500, detail="Saved El Gordo hot/cold simulation not found")

    public = _doc_to_json(doc)
    public["simulation_id"] = sim_id
    return JSONResponse(content=public)


@app.get("/api/euromillones/simulation/candidate-pool")
def get_euromillones_candidate_pool(
    cutoff_draw_id: str | None = Query(
        None,
        description=(
            "Optional draw_id; if provided, candidate pool is built from the "
            "latest simulation document for this cutoff."
        ),
    ),
    k_main: int = Query(
        20,
        ge=1,
        le=50,
        description="Size of main-number candidate pool.",
    ),
    k_star: int = Query(
        6,
        ge=1,
        le=12,
        description="Size of star-number candidate pool.",
    ),
    w_freq_main: float = Query(
        0.4,
        ge=0.0,
        le=1.0,
        description="Weight of frequency probability for main numbers.",
    ),
    w_gap_main: float = Query(
        0.3,
        ge=0.0,
        le=1.0,
        description="Weight of gap probability for main numbers.",
    ),
    w_hot_main: float = Query(
        0.3,
        ge=0.0,
        le=1.0,
        description="Weight of hot/cold probability for main numbers.",
    ),
    w_freq_star: float = Query(
        0.5,
        ge=0.0,
        le=1.0,
        description="Weight of frequency probability for star numbers.",
    ),
    w_gap_star: float = Query(
        0.25,
        ge=0.0,
        le=1.0,
        description="Weight of gap probability for star numbers.",
    ),
    w_hot_star: float = Query(
        0.25,
        ge=0.0,
        le=1.0,
        description="Weight of hot/cold probability for star numbers.",
    ),
):
    """
    Build a Euromillones candidate pool for the wheeling engine.

    Uses the unified `euromillones_simulations` document (freq/gap/hot per number)
    and combines them into a single score per number using the provided weights.

    Default configuration:
      mains: freq=0.4, gap=0.3, hot=0.3, k_main=20
      stars: freq=0.5, gap=0.25, hot=0.25, k_star=6
    """
    try:
        pool = build_candidate_pool(
            cutoff_draw_id=cutoff_draw_id,
            k_main=k_main,
            k_star=k_star,
            main_weights={
                "freq": w_freq_main,
                "gap": w_gap_main,
                "hot": w_hot_main,
            },
            star_weights={
                "freq": w_freq_star,
                "gap": w_gap_star,
                "hot": w_hot_star,
            },
        )
    except ValueError as e:
        raise HTTPException(400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(400, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=f"Error building candidate pool: {e}")

    return JSONResponse(content=pool)


@app.get("/api/el-gordo/simulation/candidate-pool")
def get_el_gordo_candidate_pool(
    cutoff_draw_id: str | None = Query(
        None,
        description=(
            "Optional draw_id; if provided, candidate pool is built from the "
            "latest El Gordo simulation document for this cutoff."
        ),
    ),
    k_main: int = Query(
        20,
        ge=1,
        le=54,
        description="Tamaño del pool de números principales (1–54).",
    ),
    k_clave: int = Query(
        6,
        ge=1,
        le=10,
        description="Tamaño del pool de números clave (0–9).",
    ),
    w_freq_main: float = Query(
        0.4,
        ge=0.0,
        le=1.0,
        description="Peso de frecuencia para números principales.",
    ),
    w_gap_main: float = Query(
        0.3,
        ge=0.0,
        le=1.0,
        description="Peso de gap para números principales.",
    ),
    w_hot_main: float = Query(
        0.3,
        ge=0.0,
        le=1.0,
        description="Peso hot/cold para números principales.",
    ),
    w_freq_clave: float = Query(
        0.4,
        ge=0.0,
        le=1.0,
        description="Peso de frecuencia para número clave.",
    ),
    w_gap_clave: float = Query(
        0.3,
        ge=0.0,
        le=1.0,
        description="Peso de gap para número clave.",
    ),
    w_hot_clave: float = Query(
        0.3,
        ge=0.0,
        le=1.0,
        description="Peso hot/cold para número clave.",
    ),
):
    """
    Build an El Gordo candidate pool for the wheeling engine.
    """
    try:
        pool = build_el_gordo_candidate_pool(
            cutoff_draw_id=cutoff_draw_id,
            k_main=k_main,
            k_clave=k_clave,
            main_weights={
                "freq": w_freq_main,
                "gap": w_gap_main,
                "hot": w_hot_main,
            },
            clave_weights={
                "freq": w_freq_clave,
                "gap": w_gap_clave,
                "hot": w_hot_clave,
            },
        )
    except ValueError as e:
        raise HTTPException(400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(400, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=f"Error building El Gordo candidate pool: {e}")

    return JSONResponse(content=pool)


@app.get("/api/la-primitiva/number-history")
def get_la_primitiva_number_history():
    """
    Return full per-number appearance history for La Primitiva, grouped by type.

    Response format:
      {
        "main": [
          { "number": 1, "dates": ["2026-01-13", ...] },
          ...
        ],
        "complementario": [
          { "number": 1, "dates": ["2026-01-20", ...] },
          ...
        ],
        "reintegro": [
          { "number": 0, "dates": ["2026-01-20", ...] },
          ...
        ]
      }
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["la_primitiva_number_history"]

    docs = list(
        coll.find(
            {},
            projection={
                "_id": 0,
                "type": 1,
                "number": 1,
                "appearances.date": 1,
            },
        )
    )

    main: list[dict] = []
    complementario: list[dict] = []
    reintegro: list[dict] = []

    for doc in docs:
        t = doc.get("type")
        number = doc.get("number")
        appearances = doc.get("appearances") or []

        dates_set: set[str] = set()
        for appo in appearances:
            date_str = (appo.get("date") or "").split(" ")[0]
            if date_str:
                dates_set.add(date_str)

        dates = sorted(dates_set)

        if t == "main":
            main.append({"number": number, "dates": dates})
        elif t == "complementario":
            complementario.append({"number": number, "dates": dates})
        elif t == "reintegro":
            reintegro.append({"number": number, "dates": dates})

    return JSONResponse(
        content={
            "main": main,
            "complementario": complementario,
            "reintegro": reintegro,
        }
    )


@app.get("/api/el-gordo/number-history")
def get_el_gordo_number_history():
    """
    Return full per-number appearance history for El Gordo, grouped by type.

    Response format:
      {
        "main": [ { "number": 1, "dates": ["2026-01-13", ...] }, ... ],
        "clave": [ { "number": 0, "dates": ["2026-01-20", ...] }, ... ]
      }
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["el_gordo_number_history"]

    docs = list(
        coll.find(
            {},
            projection={
                "_id": 0,
                "type": 1,
                "number": 1,
                "appearances.date": 1,
            },
        )
    )

    main: list[dict] = []
    clave: list[dict] = []

    for doc in docs:
        t = doc.get("type")
        number = doc.get("number")
        appearances = doc.get("appearances") or []

        dates_set: set[str] = set()
        for appo in appearances:
            date_str = (appo.get("date") or "").split(" ")[0]
            if date_str:
                dates_set.add(date_str)

        dates = sorted(dates_set)

        if t == "main":
            main.append({"number": number, "dates": dates})
        elif t == "clave":
            clave.append({"number": number, "dates": dates})

    return JSONResponse(content={"main": main, "clave": clave})


@app.post("/api/scrape/daily")
def scrape_daily():
    """
    Daily scrape for all lotteries: from (today - 3 days) to today.
    Call at 00:02 via scheduler or manually. Saves/updates draws; updates last_draw_date.
    """
    from datetime import datetime, timedelta
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    today = datetime.now().strftime("%Y-%m-%d")
    today_yyyymmdd = today.replace("-", "")
    from_d = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
    start_yyyymmdd = from_d.replace("-", "")
    results = []
    for lottery in LOTTERY_DAILY_ORDER:
        game_id = GAME_IDS[lottery]
        api_url = (
            f"{BASE_URL}?game_id={game_id}&celebrados=true"
            f"&fechaInicioInclusiva={start_yyyymmdd}&fechaFinInclusiva={today_yyyymmdd}"
        )
        results_path = RESULTS_PATHS.get(lottery, RESULTS_PATHS["la-primitiva"])
        results_page_url = f"{SITE_ORIGIN}{results_path}"
        try:
            data, proximo_bote_eur = _scrape_with_selenium(api_url, results_page_url)
            if not isinstance(data, list):
                results.append({"lottery": lottery, "saved": 0, "message": "Invalid response"})
                continue
            saved, _ = _save_draws_to_db(data)
            max_date = _max_date_from_draws(data)
            if not max_date:
                max_date = _get_max_draw_date_from_db(game_id)
            if max_date:
                _set_last_draw_date(lottery, max_date)
            try:
                _update_next_funds_metadata(lottery, scraped_bote=proximo_bote_eur)
            except Exception:
                pass
            results.append({
                "lottery": lottery,
                "saved": saved,
                "proximo_bote_eur": proximo_bote_eur,
                "message": f"Saved {saved} draws",
            })
        except Exception as e:
            results.append({"lottery": lottery, "saved": 0, "message": str(e)})
    return {"results": results, "date": today}


@app.post("/api/scrape/import")
async def scrape_import(body: list):
    """
    Save draws from pasted JSON (e.g. when the lottery API returns 403).
    Open the buscadorSorteos URL in your browser, copy the JSON array, paste here.
    """
    if not isinstance(body, list):
        raise HTTPException(400, detail="Body must be a JSON array of draws")
    saved, errors = _save_draws_to_db(body)
    return {
        "saved": saved,
        "total": len(body),
        "message": f"Saved {saved} draws to MongoDB.",
        "errors": errors[:5] if errors else None,
    }
