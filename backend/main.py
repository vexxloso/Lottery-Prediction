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
from typing import Any, Optional, List, Dict, Iterable, Sequence, Tuple
from itertools import combinations

from dotenv import load_dotenv

# Load .env from project root first, then backend dir (backend/.env can override locals)
_load_env_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(os.path.dirname(_load_env_dir), ".env"))
load_dotenv(os.path.join(_load_env_dir, ".env"))

from datetime import datetime as dt, timedelta

from bson import ObjectId
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware 
from fastapi.responses import JSONResponse, StreamingResponse, FileResponse
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, PyMongoError
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# Full-wheel TXT viewer: safe paging by filename (title)
from pathlib import Path

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

# DB-based ticket pool system (new architecture)
import sys as _sys
import os as _os
_backend_dir = _os.path.dirname(_os.path.abspath(__file__))
if _backend_dir not in _sys.path:
    _sys.path.insert(0, _backend_dir)
from ticket_db import (
    bootstrap_euromillones_tickets,
    bootstrap_el_gordo_tickets,
    bootstrap_la_primitiva_tickets,
    update_euromillones_ranking,
    update_el_gordo_ranking,
    update_la_primitiva_ranking,
    compare_euromillones_from_db,
    compare_el_gordo_from_db,
    compare_la_primitiva_from_db,
    probs_list_to_dict,
)

def _optional_wheel_position(t: dict) -> Optional[int]:
    """Parse optional full-wheel line index from a ticket dict (queue / API). Must be >= 1."""
    p = t.get("position")
    if p is None:
        return None
    try:
        ip = int(p)
    except (TypeError, ValueError):
        return None
    return ip if ip >= 1 else None


def _bought_wheel_line_positions(bought_tickets: Optional[list]) -> set[int]:
    """1-based full-wheel line indices from bought_tickets[].position (when present)."""
    out: set[int] = set()
    if not bought_tickets:
        return out
    for t in bought_tickets:
        if not isinstance(t, dict):
            continue
        ip = _optional_wheel_position(t)
        if ip is not None:
            out.add(ip)
    return out


def _enqueue_range_exclude_positions_from_body(body: Dict[str, Any]) -> set[int]:
    """
    Optional JSON body field exclude_positions: list of 1-based wheel line indices to skip.
    Sent by the frontend for tickets already in cesta / buy-queue / guardados so range enqueue
    does not re-add those lines.
    """
    raw = body.get("exclude_positions")
    if not isinstance(raw, list):
        return set()
    out: set[int] = set()
    for x in raw:
        if isinstance(x, bool):
            continue
        if isinstance(x, int) and x >= 1:
            out.add(x)
        elif isinstance(x, float) and x >= 1:
            out.add(int(x))
    return out


def _lottery_max_enqueue_by_count() -> int:
    """Upper bound per enqueue-by-count request; override with LOTTERY_MAX_ENQUEUE_BY_COUNT (default 100000)."""
    raw = os.environ.get("LOTTERY_MAX_ENQUEUE_BY_COUNT", "100000").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 100_000
    return max(1, min(n, 2_000_000))


def _txt_max_line_index_first_column(path: str) -> int:
    """Largest 1-based COMBO position in a full-wheel TXT (supports legacy and id-prefixed lines)."""
    m = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            p = _fw_line_position(raw)
            if p is not None and p > m:
                m = p
    return m

def _append_el_gordo_bought_tickets(
    tickets: list,
    draw_date: str | None = None,
    cutoff_draw_id: str | None = None,
) -> None:
    """Append tickets to bought_tickets in el_gordo_train_progress (merge, dedupe by mains+clave). Preserves optional position."""
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
    merged: List[Dict[str, Any]] = [dict(x) for x in existing if isinstance(x, dict)]
    key_to_idx: Dict[Tuple[Tuple[int, ...], int], int] = {}
    for i, x in enumerate(merged):
        mm = x.get("mains") or []
        if isinstance(mm, list) and len(mm) == 5:
            try:
                k = (tuple(int(v) for v in mm), int(x.get("clave", 0)))
                key_to_idx[k] = i
            except (TypeError, ValueError):
                pass
    for t in tickets:
        if not isinstance(t, dict):
            continue
        mains = t.get("mains") or []
        clave = int(t.get("clave", 0))
        if len(mains) != 5:
            continue
        key = (tuple(int(x) for x in mains), clave)
        pos = _optional_wheel_position(t)
        if key not in key_to_idx:
            entry: Dict[str, Any] = {"mains": list(mains), "clave": clave}
            if pos is not None:
                entry["position"] = pos
            merged.append(entry)
            key_to_idx[key] = len(merged) - 1
        elif pos is not None:
            merged[key_to_idx[key]]["position"] = pos
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
    draw_date_iso: str,
    file_date_compact: str,
    lottery_token: str = "EUROMILLONES",
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
    - Each line: {LOTTERY}_DRAW_{YYYYMMDD}_COMBO_{position};position;mains;stars
    """
    root = Path(_ROOT_DIR)
    out_dir = root / "euromillones_pools"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"euromillones_{file_date_compact}.txt"

    # Positions are global across tiers.
    position = 0
    total_by_tier = [0, 0, 0, 0]

    # Write tiers 0..3 in order.
    with out_path.open("w", encoding="utf-8", buffering=1024 * 1024) as f:
        for tier in range(4):
            # Deterministic shuffling per tier (no cancel support).
            seed = (hash((draw_date_iso, tier)) & 0xFFFFFFFF)
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
                        tid = _full_wheel_ticket_id(lottery_token, file_date_compact, position)
                        f.write(f"{tid};{position};{mains_str};{stars_str}\n")
                        total_by_tier[tier] += 1
                    buffer.clear()

            if buffer:
                rng.shuffle(buffer)
                for m, s in buffer:
                    position += 1
                    mains_str = ",".join(str(n) for n in m)
                    stars_str = ",".join(str(n) for n in s)
                    tid = _full_wheel_ticket_id(lottery_token, file_date_compact, position)
                    f.write(f"{tid};{position};{mains_str};{stars_str}\n")
                    total_by_tier[tier] += 1

    good_count = total_by_tier[0] + total_by_tier[1] + total_by_tier[2]
    bad_count = total_by_tier[3]
    total = position
    return {
        "file_path": str(out_path),
        "draw_date": draw_date_iso,
        "good_tickets": good_count,
        "bad_tickets": bad_count,
        "total_tickets": total,
    }


def _iter_la_primitiva_tickets_from_pool(
    mains_pool: Iterable[int],
) -> Iterable[Tuple[Sequence[int], int]]:
    """
    Iterate all La Primitiva tickets (6 mains + reintegro 0–9) for the given ordered pool
    in manifold order (mirror El Gordo / Euromillones full wheel).

    - mains_pool: ordered mains numbers (expected length 49, already extended
      and prioritized by Step 4).

    We:
    - Deduplicate while preserving order.
    - Build all index combinations of length 6.
    - Deterministically shuffle the mains combo indices and reintegro indices.
    - Traverse the (main_combo_index × reintegro_index) grid in a diagonal pattern
      to avoid long runs of identical mains or identical reintegro.
    """
    mains_list = list(dict.fromkeys(int(x) for x in mains_pool))
    if len(mains_list) < 6:
        raise ValueError(f"Need at least 6 mains, got {len(mains_list)}")

    main_idx_combos = list(combinations(range(len(mains_list)), 6))
    reintegro_list = list(range(10))
    reintegro_indices = list(range(len(reintegro_list)))

    seed = (hash((tuple(mains_list), tuple(reintegro_list))) & 0xFFFFFFFF)
    rng = random.Random(seed)
    rng.shuffle(main_idx_combos)
    rng.shuffle(reintegro_indices)

    nm = len(main_idx_combos)
    nr = len(reintegro_list)
    total = nm * nr
    for k in range(total):
        i = k % nm
        t = k // nm
        j = (t + i) % nr
        mains = [mains_list[idx] for idx in main_idx_combos[i]]
        reintegro = int(reintegro_list[reintegro_indices[j]])
        yield mains, reintegro


def _la_primitiva_ticket_tier(mains: Sequence[int]) -> int:
    """
    Classify a La Primitiva ticket into quality tiers (0 best .. 3 worst),
    reusing the same structural ideas as El Gordo/Euromillones.
    """
    return _el_gordo_ticket_tier(mains)


def _generate_la_primitiva_full_wheel_file(
    mains_pool: Iterable[int],
    draw_date_iso: str,
    file_date_compact: str,
    lottery_token: str = "LA_PRIMITIVA",
) -> dict:
    """
    Generate full La Primitiva wheel from the given mains pool and save to TXT.

    - Uses structural rules (via _la_primitiva_ticket_tier) to group tickets
      into tiers 0..3.
    - Tickets are written tier by tier (0 → 1 → 2 → 3) so structurally
      weaker patterns are pushed toward the end of the file.
    - Each line: {LOTTERY}_DRAW_{YYYYMMDD}_COMBO_{position};position;m1,...,m6;reintegro
    """
    root = Path(_ROOT_DIR)
    out_dir = root / "la_primitiva_pools"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"la_primitiva_{file_date_compact}.txt"
    print(
        f"[la-prim-fullwheel] start generate TXT draw_date={draw_date_iso!r} mains_pool_len={len(list(dict.fromkeys(int(x) for x in mains_pool)))} path={out_path}",
        flush=True,
    )

    position = 0
    total_by_tier = [0, 0, 0, 0]

    with out_path.open("w", encoding="utf-8", buffering=1024 * 1024) as f:
        for tier in range(4):
            seed = (hash((draw_date_iso, "la-primitiva", tier)) & 0xFFFFFFFF)
            rng = random.Random(seed)
            buffer: List[Tuple[List[int], int]] = []
            buffer_size = 1000

            for mains, reintegro in _iter_la_primitiva_tickets_from_pool(mains_pool):
                t = _la_primitiva_ticket_tier(mains)
                if t != tier:
                    continue
                buffer.append((list(mains), int(reintegro)))
                if len(buffer) >= buffer_size:
                    rng.shuffle(buffer)
                    for m, r in buffer:
                        position += 1
                        mains_str = ",".join(str(n) for n in m)
                        tid = _full_wheel_ticket_id(lottery_token, file_date_compact, position)
                        f.write(f"{tid};{position};{mains_str};{r}\n")
                        total_by_tier[tier] += 1
                    buffer.clear()

            if buffer:
                rng.shuffle(buffer)
                for m, r in buffer:
                    position += 1
                    mains_str = ",".join(str(n) for n in m)
                    tid = _full_wheel_ticket_id(lottery_token, file_date_compact, position)
                    f.write(f"{tid};{position};{mains_str};{r}\n")
                    total_by_tier[tier] += 1

    good_count = total_by_tier[0] + total_by_tier[1] + total_by_tier[2]
    bad_count = total_by_tier[3]
    total = position
    print(
        f"[la-prim-fullwheel] finished TXT draw_date={draw_date_iso!r} total={total} good={good_count} bad={bad_count}",
        flush=True,
    )
    return {
        "file_path": str(out_path),
        "draw_date": draw_date_iso,
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
    draw_date_iso: str,
    file_date_compact: str,
    lottery_token: str = "ELGORDO",
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
        f"[el-gordo-fullwheel] START draw_date={draw_date_iso} mains_pool={mains_list_dbg} clave_pool={clave_list_dbg}",
        flush=True,
    )

    # Use local lists for iteration below so we don't re-iterate the debug copies.
    mains_pool = mains_list_dbg
    clave_pool = clave_list_dbg

    root = Path(_ROOT_DIR)
    out_dir = root / "el_gordo_pools"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"el_gordo_{file_date_compact}.txt"

    position = 0
    total_by_tier = [0, 0, 0, 0]
    debug_first_tickets: List[Tuple[List[int], int]] = []

    with out_path.open("w", encoding="utf-8", buffering=1024 * 1024) as f:
        for tier in range(4):
            seed = (hash(("el_gordo_full_wheel", draw_date_iso, tier)) & 0xFFFFFFFF)
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
                        tid = _full_wheel_ticket_id(lottery_token, file_date_compact, position)
                        f.write(f"{tid};{position};{mains_str};{c}\n")
                        total_by_tier[tier] += 1
                    buffer.clear()

            if buffer:
                rng.shuffle(buffer)
                for m, c in buffer:
                    position += 1
                    mains_str = ",".join(str(n) for n in m)
                    tid = _full_wheel_ticket_id(lottery_token, file_date_compact, position)
                    f.write(f"{tid};{position};{mains_str};{c}\n")
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
                    sp = _fw_split_el_gordo_line(line)
                    if not sp:
                        continue
                    pos, mains_str, clave_str = sp
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
        "draw_date": draw_date_iso,
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

EL_GORDO_BUY_QUEUE_COLLECTION = "el_gordo_buy_queue"
LA_PRIMITIVA_BUY_QUEUE_COLLECTION = "la_primitiva_buy_queue"
EUROMILLONES_BUY_QUEUE_COLLECTION = "euromillones_buy_queue"
BOT_CREDENTIALS_COLLECTION = "bot_credentials"

# ── DB-based ticket pool collections (new architecture) ──────────────────────
# Permanent ticket combinations (never change after bootstrap)
EUROMILLONES_TICKETS_COLLECTION = "euromillones_tickets"
EL_GORDO_TICKETS_COLLECTION     = "el_gordo_tickets"
LA_PRIMITIVA_TICKETS_COLLECTION = "la_primitiva_tickets"
# Per-draw probability snapshots (tiny — ~2KB per draw)
EUROMILLONES_DRAW_PROBS_COLLECTION = "euromillones_draw_probs"
EL_GORDO_DRAW_PROBS_COLLECTION     = "el_gordo_draw_probs"
LA_PRIMITIVA_DRAW_PROBS_COLLECTION = "la_primitiva_draw_probs"

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
    for _compare_coll in (
        EUROMILLONES_COMPARE_RESULTS_COLLECTION,
        EL_GORDO_COMPARE_RESULTS_COLLECTION,
        LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION,
    ):
        db[_compare_coll].create_index([("updated_at", -1), ("date", -1), ("current_id", -1)])
    db["model_feedback_log"].create_index([("lottery", 1), ("draw_id", 1)], unique=True)
    db["model_feedback_log"].create_index([("lottery", 1), ("updated_at", -1), ("draw_date", -1)])
    db["model_orc_snapshots"].create_index([("lottery", 1), ("draw_id", 1)], unique=True)
    db[EL_GORDO_BUY_QUEUE_COLLECTION].create_index([("status", 1), ("created_at", 1)])
    # DB-based ticket pool indexes (new architecture)
    for _tc in [EUROMILLONES_TICKETS_COLLECTION, EL_GORDO_TICKETS_COLLECTION, LA_PRIMITIVA_TICKETS_COLLECTION]:
        db[_tc].create_index([("lottery", 1), ("position", 1)], unique=True)
        db[_tc].create_index([("lottery", 1), ("tier", 1)])
    # Per-draw probability snapshot indexes (tiny — ~2KB per draw)
    for _rc in [EUROMILLONES_DRAW_PROBS_COLLECTION, EL_GORDO_DRAW_PROBS_COLLECTION, LA_PRIMITIVA_DRAW_PROBS_COLLECTION]:
        db[_rc].create_index("draw_id", unique=True)
        db[_rc].create_index("draw_date")
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


def _parse_el_gordo_mains_clave(doc: dict) -> tuple[list[int], int | None]:
    """
    Parse El Gordo draw: 5 mains (1–54) and clave (0–9).
    Uses numbers + reintegro, then combinacion / combinacion_acta (incl. R(n)).
    """
    mains: list[int] = []
    clave: int | None = None
    raw_numbers = doc.get("numbers") or []
    if isinstance(raw_numbers, list) and len(raw_numbers) >= 5:
        try:
            mains = [int(x) for x in raw_numbers[:5]]
        except (TypeError, ValueError):
            mains = []
    raw_reintegro = doc.get("reintegro")
    try:
        if raw_reintegro is not None and str(raw_reintegro).strip() != "":
            clave = int(raw_reintegro)
    except (TypeError, ValueError):
        clave = None

    if len(mains) == 5 and clave is not None and 0 <= clave <= 9:
        return mains, clave

    for field in ("combinacion", "combinacion_acta"):
        text = (doc.get(field) or "").strip()
        if not text:
            continue
        parsed = parse_combinacion(text)
        if len(parsed.get("numbers") or []) >= 5:
            try:
                mains = [int(x) for x in parsed["numbers"][:5]]
            except (TypeError, ValueError):
                mains = []
        if parsed.get("reintegro") is not None:
            try:
                clave = int(parsed["reintegro"])
            except (TypeError, ValueError):
                pass
        if len(mains) == 5 and clave is not None:
            return mains, clave
        match_r = re.search(r"R\s*\(\s*(\d+)\s*\)", text, re.I)
        if match_r:
            try:
                clave = int(match_r.group(1))
            except (TypeError, ValueError):
                pass
        main_part = re.split(r"\s+R\s*\(", text, flags=re.I)[0].strip()
        parts = re.split(r"[\s\-]+", main_part)
        fallback_mains = [int(p) for p in parts if p.isdigit()][:5]
        if len(fallback_mains) == 5:
            mains = fallback_mains
        if len(mains) == 5 and clave is not None:
            return mains, clave

    if len(mains) == 5 and clave is not None and 0 <= clave <= 9:
        return mains, clave
    return mains if len(mains) == 5 else [], clave if clave is not None and 0 <= clave <= 9 else None


def normalize_draw(draw: dict) -> dict:
    """Add parsed numbers, C, R, and joker_combinacion; keep all original fields."""
    out = dict(draw)
    combinacion = draw.get("combinacion") or draw.get("combinacion_acta") or ""
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
        "http://213.165.78.107:5173"
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


@app.get("/api/health/db")
def health_db():
    """Check MongoDB connectivity. Use this to debug 'can't access VPS DB' issues."""
    if db is None:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unavailable",
                "database": "not_initialized",
                "message": "App not started or lifespan not run yet.",
            },
        )
    try:
        # ping forces a round-trip to the server
        db.client.admin.command("ping")
        return {
            "status": "ok",
            "database": "connected",
            "db_name": MONGO_DB,
            "host_configured": "***" if MONGO_URI != "mongodb://localhost:27017" else "localhost",
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "degraded",
                "database": "error",
                "message": str(e),
                "hint": "Check MONGO_URI in .env, VPS firewall (port 27017), and that MongoDB is running and bound to 0.0.0.0 if connecting remotely.",
            },
        )


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
    if doc is None:
        return None
    return (doc.get("last_draw_date") or "").strip() or None


def _get_next_draw_date(lottery: str) -> str | None:
    """Get next_draw_date for a lottery from scraper_metadata."""
    if db is None:
        return None
    doc = db[METADATA_COLLECTION].find_one({"lottery": lottery}, projection=["next_draw_date"])
    if doc is None:
        return None
    return (doc.get("next_draw_date") or "").strip() or None


def _fw_normalize_draw_date_to_iso(raw: str) -> str:
    """Normalize a draw date string to YYYY-MM-DD (first 10 chars if already ISO)."""
    s = (raw or "").strip()
    if not s:
        raise ValueError("empty draw date")
    s = s.split()[0].replace("/", "-")
    if len(s) >= 10 and s[4:5] == "-" and s[7:8] == "-":
        return s[:10]
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) >= 8:
        d8 = digits[:8]
        return f"{d8[:4]}-{d8[4:6]}-{d8[6:8]}"
    raise ValueError(f"unrecognized draw date: {raw!r}")


def _fw_compact_from_iso(iso_yyyy_mm_dd: str) -> str:
    return (iso_yyyy_mm_dd or "").replace("-", "").strip()[:8]


def _full_wheel_ticket_id(lottery_token: str, date_compact: str, position: int) -> str:
    return f"{lottery_token}_DRAW_{date_compact}_COMBO_{position}"


def _resolve_full_wheel_dates_for_api(lottery_slug: str, draw_date_override: str | None) -> tuple[str, str]:
    """
    (iso_yyyy_mm_dd, compact_yyyymmdd) for full wheel TXT filename and COMBO ids.
    Optional draw_date query overrides; otherwise uses scraper_metadata.next_draw_date.
    """
    o = (draw_date_override or "").strip()
    if o:
        iso = _fw_normalize_draw_date_to_iso(o)
        return iso, _fw_compact_from_iso(iso)
    nd = _get_next_draw_date(lottery_slug)
    if not nd or not str(nd).strip():
        raise HTTPException(
            400,
            detail=(
                f"next_draw_date missing in scraper_metadata for {lottery_slug}; "
                "set it in metadata or pass draw_date=YYYY-MM-DD (or YYYYMMDD)."
            ),
        )
    iso = _fw_normalize_draw_date_to_iso(str(nd))
    return iso, _fw_compact_from_iso(iso)


def _fw_line_position(raw: str) -> Optional[int]:
    """1-based COMBO position from a full-wheel TXT line (legacy or id-prefixed)."""
    p = [x.strip() for x in raw.split(";")]
    if len(p) >= 4:
        try:
            return int(p[1])
        except (TypeError, ValueError):
            return None
    if len(p) == 3:
        if "_COMBO_" in p[0]:
            try:
                return int(p[1])
            except (TypeError, ValueError):
                return None
        try:
            return int(p[0])
        except (TypeError, ValueError):
            return None
    if len(p) == 2:
        try:
            return int(p[0])
        except (TypeError, ValueError):
            return None
    return None


def _fw_split_euromillones_line(raw: str) -> Optional[tuple[int, str, str]]:
    """Return (position, mains_str, stars_str) or None."""
    parts = raw.split(";")
    if len(parts) >= 4:
        try:
            return int(parts[1]), parts[2], parts[3]
        except (TypeError, ValueError):
            return None
    if len(parts) >= 3:
        try:
            return int(parts[0]), parts[1], parts[2]
        except (TypeError, ValueError):
            return None
    return None


def _fw_split_el_gordo_line(raw: str) -> Optional[tuple[int, str, str]]:
    """Return (position, mains_str, clave_str) or None."""
    parts = raw.split(";")
    if len(parts) >= 4:
        try:
            return int(parts[1]), parts[2], parts[3]
        except (TypeError, ValueError):
            return None
    if len(parts) >= 3:
        try:
            return int(parts[0]), parts[1], parts[2]
        except (TypeError, ValueError):
            return None
    return None


def _fw_split_la_primitiva_line(raw: str) -> Optional[tuple[int, str, Optional[str]]]:
    """Return (position, mains_str, reintegro_str_or_none) or None."""
    parts = raw.split(";")
    if len(parts) >= 4 and "_COMBO_" in (parts[0] or ""):
        try:
            return int(parts[1]), parts[2], parts[3]
        except (TypeError, ValueError):
            return None
    if len(parts) >= 3 and "_COMBO_" in (parts[0] or ""):
        try:
            return int(parts[1]), parts[2], None
        except (TypeError, ValueError):
            return None
    if len(parts) >= 3:
        try:
            return int(parts[0]), parts[1], parts[2]
        except (TypeError, ValueError):
            return None
    if len(parts) >= 2:
        try:
            return int(parts[0]), parts[1], None
        except (TypeError, ValueError):
            return None
    return None


def _fw_token_and_compact_from_path(path: str) -> tuple[str, str]:
    """Derive {EUROMILLONES|ELGORDO|LA_PRIMITIVA} and YYYYMMDD from pool filename or first line id."""
    base = os.path.basename(path)
    m = re.match(r"^(euromillones|el_gordo|la_primitiva)_(\d{8})\.txt$", base, re.I)
    if m:
        pref = m.group(1).lower()
        tok = {"euromillones": "EUROMILLONES", "el_gordo": "ELGORDO", "la_primitiva": "LA_PRIMITIVA"}[pref]
        return tok, m.group(2)
    m2 = re.match(r"^(euromillones|el_gordo|la_primitiva)_(\d{4}-\d{2}-\d{2})\.txt$", base, re.I)
    if m2:
        pref = m2.group(1).lower()
        tok = {"euromillones": "EUROMILLONES", "el_gordo": "ELGORDO", "la_primitiva": "LA_PRIMITIVA"}[pref]
        return tok, m2.group(2).replace("-", "")
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            parts = raw.split(";")
            if len(parts) >= 4 or (len(parts) >= 3 and "_COMBO_" in (parts[0] or "")):
                tid = parts[0]
                m3 = re.match(r"^(.+)_DRAW_(\d{8})_COMBO_\d+$", tid)
                if m3:
                    return m3.group(1), m3.group(2)
            break
    raise ValueError(f"Cannot derive lottery id prefix / date from {path!r}")


def _fw_export_max_lines() -> int:
    """Upper bound lines per export request (default 200000)."""
    raw = os.environ.get("FULL_WHEEL_EXPORT_MAX_LINES", "200000").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 200_000
    return max(1, min(n, 5_000_000))


def _fw_export_range(start_position: int, end_position: Optional[int], total: int) -> tuple[int, int]:
    if start_position < 1:
        raise HTTPException(400, detail="start_position must be >= 1")
    if total > 0 and start_position > total:
        raise HTTPException(400, detail=f"start_position must be <= total tickets ({total})")
    max_lines = _fw_export_max_lines()
    if end_position is None or end_position <= 0:
        end = start_position + max_lines - 1
        if total > 0:
            end = min(end, total)
        return start_position, end
    if end_position < start_position:
        raise HTTPException(400, detail="end_position must be >= start_position")
    if total > 0 and end_position > total:
        raise HTTPException(400, detail=f"end_position must be <= total tickets ({total})")
    if (end_position - start_position + 1) > max_lines:
        raise HTTPException(
            400,
            detail=f"Requested range too large. Max lines per export is {max_lines} (set FULL_WHEEL_EXPORT_MAX_LINES to raise).",
        )
    return start_position, end_position


def _fw_csv_path_for_full_wheel(txt_path: str) -> str:
    folder = os.path.dirname(txt_path) or "."
    base = os.path.splitext(os.path.basename(txt_path))[0]
    return os.path.join(folder, f"{base}.csv")


def _fw_generate_full_csv_if_needed(txt_path: str, lottery_slug: str) -> str:
    """
    Create a full CSV file next to the full-wheel TXT (streaming read/write).
    If already exists and is newer than TXT, reuse it.
    """
    csv_path = _fw_csv_path_for_full_wheel(txt_path)
    try:
        if os.path.isfile(csv_path) and os.path.getmtime(csv_path) >= os.path.getmtime(txt_path):
            return csv_path
    except OSError:
        pass

    token, compact = _fw_token_and_compact_from_path(txt_path)
    tmp_path = csv_path + ".tmp"
    header = (
        "ticket_id;position;mains;stars\n"
        if lottery_slug == "euromillones"
        else "ticket_id;position;mains;clave\n"
        if lottery_slug == "el-gordo"
        else "ticket_id;position;mains;reintegro\n"
    )

    with open(tmp_path, "w", encoding="utf-8", newline="") as out:
        out.write(header)
        with open(txt_path, "r", encoding="utf-8") as f:
            for line in f:
                raw = line.rstrip("\n")
                if not raw:
                    continue
                if lottery_slug == "euromillones":
                    sp = _fw_split_euromillones_line(raw)
                    if not sp:
                        continue
                    p, mains_str, stars_str = sp
                    tid = raw.split(";", 1)[0]
                    if "_COMBO_" not in tid:
                        tid = _full_wheel_ticket_id(token, compact, p)
                    out.write(f"{tid};{p};{mains_str};{stars_str}\n")
                elif lottery_slug == "el-gordo":
                    sp = _fw_split_el_gordo_line(raw)
                    if not sp:
                        continue
                    p, mains_str, clave_str = sp
                    tid = raw.split(";", 1)[0]
                    if "_COMBO_" not in tid:
                        tid = _full_wheel_ticket_id(token, compact, p)
                    out.write(f"{tid};{p};{mains_str};{clave_str}\n")
                else:
                    sp = _fw_split_la_primitiva_line(raw)
                    if not sp:
                        continue
                    p, mains_str, rein_str = sp
                    tid = raw.split(";", 1)[0]
                    if "_COMBO_" not in tid:
                        tid = _full_wheel_ticket_id(token, compact, p)
                    r_out = (rein_str or "").strip()
                    if r_out == "":
                        r_out = "0"
                    out.write(f"{tid};{p};{mains_str};{r_out}\n")

    os.replace(tmp_path, csv_path)
    return csv_path


def _fw_safe_title_to_fullwheel_path(title: str) -> str:
    """
    Resolve a *filename only* (no directories) to a full-wheel TXT path inside known folders.
    Prevents path traversal. Title examples: 'el_gordo_20260308.txt'
    """
    t = (title or "").strip()
    if not t:
        raise HTTPException(400, detail="title is required")
    if len(t) > 200:
        raise HTTPException(400, detail="title too long")
    # Disallow any path separators or traversal.
    if any(x in t for x in ("/", "\\", ":", "..")):
        raise HTTPException(400, detail="invalid title")
    if not re.match(r"^[A-Za-z0-9_.-]+\.txt$", t):
        raise HTTPException(400, detail="invalid title")

    root = Path(_ROOT_DIR)
    allowed_dirs = [
        root / "euromillones_pools",
        root / "el_gordo_pools",
        root / "la_primitiva_pools",
    ]
    for d in allowed_dirs:
        try:
            p = (d / t).resolve()
            # Ensure resolved path stays within the directory.
            d_res = d.resolve()
            if d_res not in p.parents:
                continue
            if p.is_file():
                return str(p)
        except Exception:
            continue
    raise HTTPException(404, detail="file not found")


# --- CSV preparation jobs (for UI progress while server generates CSV) ---
_csv_prepare_jobs_lock = threading.Lock()
_csv_prepare_jobs: dict[str, dict] = {}  # job_id -> {status, progress, error, txt_path, csv_path, lottery, started_at, updated_at}


def _resolve_full_wheel_txt_path_for_export(lottery_slug: str, cutoff_draw_id: str | None) -> str:
    """Resolve full-wheel TXT path for latest train_progress (or explicit cutoff_draw_id)."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    slug = (lottery_slug or "").strip().lower()
    coll_name = (
        EUROMILLONES_TRAIN_PROGRESS_COLLECTION
        if slug == "euromillones"
        else EL_GORDO_TRAIN_PROGRESS_COLLECTION
        if slug == "el-gordo"
        else LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION
        if slug == "la-primitiva"
        else None
    )
    if not coll_name:
        raise HTTPException(400, detail="Invalid lottery")
    coll = db[coll_name]
    cid = (cutoff_draw_id or "").strip()
    doc = None
    if cid:
        doc = coll.find_one({"cutoff_draw_id": cid}) or (coll.find_one({"cutoff_draw_id": int(cid)}) if cid.isdigit() else None)
    if doc is None:
        doc = _resolve_latest_train_progress_doc(slug)
    if not doc:
        raise HTTPException(404, detail=f"No train_progress found for {slug} (last_draw_date)")
    path = (doc.get("full_wheel_file_path") or "").strip()
    if not path or not os.path.isfile(path):
        raise HTTPException(404, detail="Full wheel file not found on disk")
    return path


def _csv_prepare_job_worker(job_id: str) -> None:
    """Background worker to generate CSV with progress updates."""
    with _csv_prepare_jobs_lock:
        job = dict(_csv_prepare_jobs.get(job_id) or {})
    if not job:
        return
    txt_path = job["txt_path"]
    lottery = job["lottery"]
    try:
        size = os.path.getsize(txt_path) if os.path.isfile(txt_path) else 0
        with _csv_prepare_jobs_lock:
            _csv_prepare_jobs[job_id]["status"] = "running"
            _csv_prepare_jobs[job_id]["progress"] = 1
            _csv_prepare_jobs[job_id]["updated_at"] = time.time()

        csv_path = _fw_csv_path_for_full_wheel(txt_path)
        token, compact = _fw_token_and_compact_from_path(txt_path)
        tmp_path = csv_path + ".tmp"
        header = (
            "ticket_id;position;mains;stars\n"
            if lottery == "euromillones"
            else "ticket_id;position;mains;clave\n"
            if lottery == "el-gordo"
            else "ticket_id;position;mains\n"
        )

        last_update = 0.0
        with open(tmp_path, "w", encoding="utf-8", newline="") as out:
            out.write(header)
            with open(txt_path, "r", encoding="utf-8") as f:
                for line in f:
                    with _csv_prepare_jobs_lock:
                        if _csv_prepare_jobs.get(job_id, {}).get("cancel_requested"):
                            raise RuntimeError("__CANCELLED__")
                    raw = line.rstrip("\n")
                    if not raw:
                        continue
                    if lottery == "euromillones":
                        sp = _fw_split_euromillones_line(raw)
                        if not sp:
                            continue
                        p, mains_str, stars_str = sp
                        tid = raw.split(";", 1)[0]
                        if "_COMBO_" not in tid:
                            tid = _full_wheel_ticket_id(token, compact, p)
                        out.write(f"{tid};{p};{mains_str};{stars_str}\n")
                    elif lottery == "el-gordo":
                        sp = _fw_split_el_gordo_line(raw)
                        if not sp:
                            continue
                        p, mains_str, clave_str = sp
                        tid = raw.split(";", 1)[0]
                        if "_COMBO_" not in tid:
                            tid = _full_wheel_ticket_id(token, compact, p)
                        out.write(f"{tid};{p};{mains_str};{clave_str}\n")
                    else:
                        sp = _fw_split_la_primitiva_line(raw)
                        if not sp:
                            continue
                        p, mains_str, rein_str = sp
                        tid = raw.split(";", 1)[0]
                        if "_COMBO_" not in tid:
                            tid = _full_wheel_ticket_id(token, compact, p)
                        r_out = (rein_str or "").strip()
                        if r_out == "":
                            r_out = "0"
                        out.write(f"{tid};{p};{mains_str};{r_out}\n")

                    # Progress update (based on bytes read)
                    now = time.time()
                    if now - last_update >= 0.2:
                        last_update = now
                        try:
                            read_bytes = f.tell()
                        except Exception:
                            read_bytes = 0
                        pct = 1
                        if size > 0 and read_bytes > 0:
                            pct = int((read_bytes / size) * 100)
                            pct = max(1, min(pct, 99))
                        with _csv_prepare_jobs_lock:
                            if job_id in _csv_prepare_jobs:
                                _csv_prepare_jobs[job_id]["progress"] = pct
                                _csv_prepare_jobs[job_id]["updated_at"] = now

        os.replace(tmp_path, csv_path)
        with _csv_prepare_jobs_lock:
            if job_id in _csv_prepare_jobs:
                _csv_prepare_jobs[job_id]["status"] = "done"
                _csv_prepare_jobs[job_id]["progress"] = 100
                _csv_prepare_jobs[job_id]["csv_path"] = csv_path
                _csv_prepare_jobs[job_id]["updated_at"] = time.time()
    except Exception as e:
        if str(e) == "__CANCELLED__":
            try:
                if os.path.isfile(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
            with _csv_prepare_jobs_lock:
                if job_id in _csv_prepare_jobs:
                    _csv_prepare_jobs[job_id]["status"] = "cancelled"
                    _csv_prepare_jobs[job_id]["error"] = ""
                    _csv_prepare_jobs[job_id]["updated_at"] = time.time()
            return
        with _csv_prepare_jobs_lock:
            if job_id in _csv_prepare_jobs:
                _csv_prepare_jobs[job_id]["status"] = "error"
                _csv_prepare_jobs[job_id]["error"] = str(e)
                _csv_prepare_jobs[job_id]["updated_at"] = time.time()


def _resolve_latest_train_progress_doc(lottery_slug: str):
    """
    Resolve the latest train_progress document for a lottery for scraper_metadata.last_draw_date.
    Primary lookup uses probs_fecha_sorteo == last_draw_date[:10]. Fallback: newest by pipeline_started_at.
    """
    if db is None:
        return None
    slug = (lottery_slug or "").strip().lower()
    coll_name = (
        EUROMILLONES_TRAIN_PROGRESS_COLLECTION
        if slug == "euromillones"
        else EL_GORDO_TRAIN_PROGRESS_COLLECTION
        if slug == "el-gordo"
        else LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION
        if slug == "la-primitiva"
        else None
    )
    if not coll_name:
        return None
    coll = db[coll_name]
    last = (_get_last_draw_date(slug) or "").strip()[:10]
    doc = None
    if last:
        doc = coll.find_one({"probs_fecha_sorteo": last}) or coll.find_one({"full_wheel_draw_date": last})
    if doc is None:
        doc = coll.find_one(sort=[("pipeline_started_at", -1)]) or coll.find_one(sort=[("full_wheel_started_at", -1)])
    return doc


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


@app.get("/api/fullwheel/page")
def api_fullwheel_page(
    title: str = Query(..., description="Full-wheel TXT filename only, e.g. el_gordo_20260308.txt"),
    skip: int = Query(0, ge=0, description="0-based number of non-empty lines to skip"),
    limit: int = Query(200, ge=1, le=2000, description="Max lines to return (1..2000)"),
):
    """
    Simple paging for very large full-wheel TXT files.
    The desktop app sends only the file title (filename), plus skip/limit.
    """
    path = _fw_safe_title_to_fullwheel_path(title)
    items: list[dict] = []
    idx = 0
    has_more = False
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            raw = line.rstrip("\n")
            if not raw.strip():
                continue
            if idx < skip:
                idx += 1
                continue
            if len(items) >= limit:
                has_more = True
                break
            items.append({"line_no": idx + 1, "raw": raw})
            idx += 1

    return JSONResponse(
        content={
            "title": os.path.basename(path),
            "skip": skip,
            "limit": limit,
            "count": len(items),
            "items": items,
            "has_more": has_more,
        }
    )


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
    # Automation bot public endpoints (compare + prediction pipeline + full wheel)
    "/api/euromillones/feature-model",
    "/api/el-gordo/feature-model",
    "/api/la-primitiva/feature-model",
    "/api/euromillones/train/progress",
    "/api/el-gordo/train/progress",
    "/api/la-primitiva/train/progress",
    "/api/euromillones/train/run-pipeline",
    "/api/el-gordo/train/run-pipeline",
    "/api/la-primitiva/train/run-pipeline",
    "/api/euromillones/train/full-wheel",
    "/api/el-gordo/train/full-wheel",
    "/api/la-primitiva/train/full-wheel",
    "/api/euromillones/compare/full-wheel",
    "/api/el-gordo/compare/full-wheel",
    "/api/el-gordo/compare/cached",
    "/api/euromillones/compare/cached",
    "/api/la-primitiva/compare/cached",
    "/api/la-primitiva/compare/full-wheel",
    # Online learning + train reset (daily automation / backfill scripts)
    "/api/online-learning/pre-draw",
    "/api/online-learning/post-draw",
    "/api/online-learning/validate-hashes",
    "/api/validation/rebuild-metrics",
    "/api/online-learning/history",
    "/api/online-learning/sync-pending",
    "/api/online-learning/repair-feedback",
    "/api/online-learning/orc-snapshots",
    "/api/train/reset",
    # DB-based reorder/compare endpoints (new architecture)
    "/api/euromillones/compare/full-wheel/reorder",
    "/api/el-gordo/compare/full-wheel/reorder",
    "/api/la-primitiva/compare/full-wheel/reorder",
    "/api/euromillones/tickets/status",
    "/api/el-gordo/tickets/status",
    "/api/la-primitiva/tickets/status",
    "/api/euromillones/tickets/bootstrap",
    "/api/el-gordo/tickets/bootstrap",
    "/api/la-primitiva/tickets/bootstrap",
    "/api/tickets/bootstrap-status",
    # Ranking dashboard endpoints
    "/api/ranking/draws",
    "/api/ranking/chart",
    "/api/ranking/top-tickets",
    "/api/ranking/compute-top-tickets",
    "/api/ranking/study-progress",
    "/api/ranking/study-summary",
    "/api/ranking/save-draw-probs",
    # Public download endpoints for native browser download (Chrome progress UI).
    "/api/euromillones/full-wheel/export",
    "/api/el-gordo/full-wheel/export",
    "/api/la-primitiva/full-wheel/export",
    "/api/euromillones/full-wheel/prepare-csv",
    "/api/el-gordo/full-wheel/prepare-csv",
    "/api/la-primitiva/full-wheel/prepare-csv",
    "/api/euromillones/full-wheel/prepare-csv/status",
    "/api/el-gordo/full-wheel/prepare-csv/status",
    "/api/la-primitiva/full-wheel/prepare-csv/status",
    "/api/euromillones/full-wheel/prepare-csv/cancel",
    "/api/el-gordo/full-wheel/prepare-csv/cancel",
    "/api/la-primitiva/full-wheel/prepare-csv/cancel",
    # Public TXT viewer paging endpoint (desktop app)
    "/api/fullwheel/page",
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


@app.post("/api/euromillones/train/progress")
def api_euromillones_train_progress_post(
    cutoff_draw_id: str | None = Query(None, description="id_sorteo for this training run."),
):
    """POST variant of euromillones/train/progress (same as GET, for polling status)."""
    return api_euromillones_train_progress(cutoff_draw_id=cutoff_draw_id)


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


@app.post("/api/el-gordo/train/progress")
def api_el_gordo_train_progress_post(
    cutoff_draw_id: str | None = Query(None, description="id_sorteo for this training run (El Gordo)."),
):
    """POST variant of el-gordo/train/progress (same as GET, for polling status)."""
    return api_el_gordo_train_progress(cutoff_draw_id=cutoff_draw_id)


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
                sp = _fw_split_el_gordo_line(line)
                if not sp:
                    continue
                position, mains_str, clave_str = sp
                try:
                    mains = [int(x) for x in mains_str.split(",") if x]
                    clave = int(clave_str)
                except Exception:
                    continue
                parts = line.split(";")
                tid = parts[0] if len(parts) >= 4 and "_COMBO_" in (parts[0] or "") else None
                row: Dict[str, Any] = {"position": position, "mains": mains, "clave": clave}
                if tid:
                    row["ticket_id"] = tid
                tickets_list.append(row)
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
        item: Dict[str, Any] = {"mains": [int(m) for m in mains], "clave": clave}
        pos = _optional_wheel_position(t)
        if pos is not None:
            item["position"] = pos
        normalized.append(item)
    draw_date = (body.get("draw_date") or "").strip()[:10] or None
    cutoff_draw_id = (body.get("cutoff_draw_id") or "").strip() or None
    if db is None:
        raise HTTPException(503, detail="Database not available")
    if not draw_date:
        coll_progress = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
        doc_progress = None
        if cutoff_draw_id:
            doc_progress = coll_progress.find_one({"cutoff_draw_id": cutoff_draw_id})
            if doc_progress is None and cutoff_draw_id.isdigit():
                doc_progress = coll_progress.find_one({"cutoff_draw_id": int(cutoff_draw_id)})
        if doc_progress is not None:
            draw_date = (doc_progress.get("probs_fecha_sorteo") or "").strip()[:10] or None
        if not draw_date:
            draw_date = (_get_last_draw_date("el-gordo") or "").strip()[:10] or None
    if not draw_date:
        raise HTTPException(400, detail="No draw_date resolved; provide draw_date or cutoff_draw_id")
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


EL_GORDO_BUCKET_SIZE = 6


@app.post("/api/el-gordo/betting/enqueue-by-count")
async def api_el_gordo_betting_enqueue_by_count(request: Request):
    """
    Create buy-queue items by ticket count. Body: { "count": int, "draw_date"?, "cutoff_draw_id"? }.
    Reads from full_wheel file, splits into buckets of 6 (one queue item per bucket).
    Capped by LOTTERY_MAX_ENQUEUE_BY_COUNT (default 100000). Inserts each bucket immediately to limit memory.
    """
    try:
        body = await request.json() or {}
    except Exception:
        body = {}
    count = body.get("count")
    if not isinstance(count, int) or count < 1:
        raise HTTPException(400, detail="Body must contain 'count' (positive integer)")
    max_n = _lottery_max_enqueue_by_count()
    if count > max_n:
        raise HTTPException(
            400,
            detail=f"count must be <= {max_n} per request (raise LOTTERY_MAX_ENQUEUE_BY_COUNT or split into multiple requests)",
        )
    draw_date = (body.get("draw_date") or "").strip()[:10] or None
    cutoff_draw_id = (body.get("cutoff_draw_id") or "").strip() or None
    if db is None:
        raise HTTPException(503, detail="Database not available")
    coll_progress = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
    coll_queue = db[EL_GORDO_BUY_QUEUE_COLLECTION]
    date_str = draw_date
    cutoff = cutoff_draw_id
    if not date_str and not cutoff:
        last = (_get_last_draw_date("el-gordo") or "").strip()
        if last:
            date_str = last[:10]
        if not date_str:
            raise HTTPException(400, detail="No last_draw_date for el-gordo; provide draw_date or cutoff_draw_id")
    doc = None
    if cutoff:
        doc = coll_progress.find_one({"cutoff_draw_id": cutoff})
        if doc is None and cutoff.isdigit():
            doc = coll_progress.find_one({"cutoff_draw_id": int(cutoff)})
    if doc is None and date_str:
        doc = coll_progress.find_one({"full_wheel_draw_date": date_str}) or coll_progress.find_one({"probs_fecha_sorteo": date_str})
    if doc is None:
        raise HTTPException(404, detail="No train progress found for draw_date or cutoff_draw_id")
    path = (doc.get("full_wheel_file_path") or "").strip()
    total_in_file = int(doc.get("full_wheel_total_tickets") or 0)
    if not path or total_in_file <= 0:
        raise HTTPException(404, detail="Full wheel file not found or empty for this draw")
    date_str = (doc.get("full_wheel_draw_date") or doc.get("probs_fecha_sorteo") or date_str or "").strip()[:10] or None
    cutoff = str(doc.get("cutoff_draw_id") or "") or cutoff
    excluded: set = set()
    for d in coll_queue.find({"status": {"$in": ["waiting", "in_progress"]}}, projection={"tickets": 1}):
        d_date = (d.get("draw_date") or "").strip()
        d_cutoff = (d.get("cutoff_draw_id") or "").strip()
        if d_date != date_str and d_cutoff != cutoff:
            continue
        for t in (d.get("tickets") or []):
            mains = t.get("mains")
            clave = t.get("clave", 0)
            if isinstance(mains, list) and len(mains) == 5 and 0 <= int(clave) <= 9:
                excluded.add((tuple(sorted(mains)), int(clave)))
    for t in (doc.get("bought_tickets") or []):
        mains = t.get("mains")
        clave = t.get("clave", 0)
        if isinstance(mains, list) and len(mains) == 5 and 0 <= int(clave) <= 9:
            excluded.add((tuple(sorted(mains)), int(clave)))
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    inserted_ids: List[str] = []
    bucket_buf: List[Dict] = []
    total_collected = 0
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if total_collected >= count:
                    break
                line = line.strip()
                if not line:
                    continue
                sp = _fw_split_el_gordo_line(line)
                if not sp:
                    continue
                position, mains_str, clave_str = sp
                try:
                    mains = [int(x) for x in mains_str.split(",") if x]
                    clave = int(clave_str)
                except Exception:
                    continue
                if len(mains) != 5 or not (0 <= clave <= 9):
                    continue
                key = (tuple(sorted(mains)), clave)
                if key in excluded:
                    continue
                excluded.add(key)
                bucket_buf.append({"mains": mains, "clave": clave, "position": position})
                total_collected += 1
                if len(bucket_buf) >= EL_GORDO_BUCKET_SIZE:
                    chunk = bucket_buf[:EL_GORDO_BUCKET_SIZE]
                    del bucket_buf[:EL_GORDO_BUCKET_SIZE]
                    doc_queue = {
                        "lottery": "el-gordo",
                        "tickets": chunk,
                        "tickets_count": len(chunk),
                        "draw_date": date_str,
                        "cutoff_draw_id": cutoff,
                        "status": "waiting",
                        "created_at": now,
                    }
                    ins = coll_queue.insert_one(doc_queue)
                    inserted_ids.append(str(ins.inserted_id))
                if total_collected >= count:
                    break
    except FileNotFoundError:
        raise HTTPException(404, detail="Full wheel file not found on disk")
    if bucket_buf:
        chunk = bucket_buf
        doc_queue = {
            "lottery": "el-gordo",
            "tickets": chunk,
            "tickets_count": len(chunk),
            "draw_date": date_str,
            "cutoff_draw_id": cutoff,
            "status": "waiting",
            "created_at": now,
        }
        ins = coll_queue.insert_one(doc_queue)
        inserted_ids.append(str(ins.inserted_id))
    if total_collected == 0:
        raise HTTPException(400, detail="No tickets available (all already in queue or bought)")
    return JSONResponse(
        content={
            "status": "ok",
            "queued": len(inserted_ids),
            "total_tickets": total_collected,
            "message": f"{len(inserted_ids)} colas creadas ({total_collected} boletos).",
        },
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.post("/api/el-gordo/betting/enqueue-by-range")
async def api_el_gordo_betting_enqueue_by_range(request: Request):
    """
    Create buy-queue items by position range from the El Gordo full-wheeling pool TXT file.

    Body: { "start_position": int, "end_position": int, "draw_date"?, "cutoff_draw_id"?, "exclude_positions"?: int[] }
      - Range is inclusive on both ends: positions start_position..end_position.
      - end must be >= start.
      - end must be <= total_in_file.
      - Optional exclude_positions: wheel line indices to skip (cesta/cola/guardados from UI).
      - Tickets already in queue (waiting/in_progress/bought) or bought are skipped by combo.
    """
    try:
        body = await request.json() or {}
    except Exception:
        body = {}

    start_position = body.get("start_position")
    end_position = body.get("end_position")
    if not isinstance(start_position, int) or not isinstance(end_position, int):
        raise HTTPException(400, detail="Body must contain 'start_position' and 'end_position' (integers)")
    if start_position < 1:
        raise HTTPException(400, detail="start_position must be >= 1")
    if end_position < start_position:
        raise HTTPException(400, detail="end_position must be >= start_position")

    draw_date = (body.get("draw_date") or "").strip()[:10] or None
    cutoff_draw_id = (body.get("cutoff_draw_id") or "").strip() or None

    if db is None:
        raise HTTPException(503, detail="Database not available")

    coll_progress = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
    coll_queue = db[EL_GORDO_BUY_QUEUE_COLLECTION]

    date_str = draw_date
    cutoff = cutoff_draw_id
    if not date_str and not cutoff:
        last = (_get_last_draw_date("el-gordo") or "").strip()
        if last:
            date_str = last[:10]
        if not date_str:
            raise HTTPException(400, detail="No last_draw_date for el-gordo; provide draw_date or cutoff_draw_id")

    doc = None
    if cutoff:
        doc = coll_progress.find_one({"cutoff_draw_id": cutoff})
        if doc is None and cutoff.isdigit():
            doc = coll_progress.find_one({"cutoff_draw_id": int(cutoff)})
    if doc is None and date_str:
        doc = coll_progress.find_one({"full_wheel_draw_date": date_str}) or coll_progress.find_one({"probs_fecha_sorteo": date_str})
    if doc is None:
        raise HTTPException(404, detail="No train progress found for draw_date or cutoff_draw_id")

    path = (doc.get("full_wheel_file_path") or "").strip()
    total_in_file = int(doc.get("full_wheel_total_tickets") or 0)
    if not path or total_in_file <= 0:
        raise HTTPException(404, detail="Full wheel file not found or empty for this draw")

    if start_position > total_in_file:
        raise HTTPException(400, detail=f"start_position must be <= total tickets ({total_in_file})")
    if end_position > total_in_file:
        raise HTTPException(400, detail=f"end_position must be <= total tickets ({total_in_file})")

    exclude_pos = _enqueue_range_exclude_positions_from_body(body)

    # Exclusions
    excluded: set[tuple[tuple[int, ...], int]] = set()
    for d in coll_queue.find({"status": {"$in": ["waiting", "in_progress", "bought"]}}, projection={"tickets": 1}):
        d_date = (d.get("draw_date") or "").strip()
        d_cutoff = (d.get("cutoff_draw_id") or "").strip()
        if d_date != date_str and d_cutoff != cutoff:
            continue
        for t in (d.get("tickets") or []):
            mains = t.get("mains")
            clave = t.get("clave", 0)
            if isinstance(mains, list) and len(mains) == 5 and 0 <= int(clave) <= 9:
                excluded.add((tuple(sorted(mains)), int(clave)))

    for t in (doc.get("bought_tickets") or []):
        mains = t.get("mains")
        clave = t.get("clave", 0)
        if isinstance(mains, list) and len(mains) == 5 and 0 <= int(clave) <= 9:
            excluded.add((tuple(sorted(mains)), int(clave)))

    collected: List[Dict] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if not line:
                    continue
                line = line.strip()
                if not line:
                    continue
                sp = _fw_split_el_gordo_line(line)
                if not sp:
                    continue
                position, mains_str, clave_str = sp

                if position < start_position:
                    continue
                if position > end_position:
                    break
                if position in exclude_pos:
                    continue

                try:
                    mains = [int(x) for x in mains_str.split(",") if x]
                    clave = int(clave_str)
                except Exception:
                    continue

                if len(mains) != 5 or not (0 <= clave <= 9):
                    continue

                key = (tuple(sorted(mains)), clave)
                if key in excluded:
                    continue
                excluded.add(key)
                collected.append({"mains": mains, "clave": clave, "position": position})
    except FileNotFoundError:
        raise HTTPException(404, detail="Full wheel file not found on disk")

    if len(collected) == 0:
        raise HTTPException(400, detail="No tickets available in this range (all already in queue or bought)")

    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    inserted_ids = []
    for i in range(0, len(collected), EL_GORDO_BUCKET_SIZE):
        chunk = collected[i : i + EL_GORDO_BUCKET_SIZE]
        doc_queue = {
            "lottery": "el-gordo",
            "tickets": chunk,
            "tickets_count": len(chunk),
            "draw_date": date_str,
            "cutoff_draw_id": cutoff,
            "status": "waiting",
            "created_at": now,
        }
        ins = coll_queue.insert_one(doc_queue)
        inserted_ids.append(str(ins.inserted_id))

    requested_count = end_position - start_position + 1
    return JSONResponse(
        content={
            "status": "ok",
            "requested_range": {"start_position": start_position, "end_position": end_position},
            "requested_tickets_count": requested_count,
            "collected_tickets_count": len(collected),
            "queued_items": len(inserted_ids),
            "queued_tickets_count": len(collected),
            "message": f"{len(inserted_ids)} colas creadas ({len(collected)} boletos).",
        },
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/api/el-gordo/betting/buy-queue")
def api_el_gordo_betting_buy_queue(limit: int = Query(50, ge=1, le=5000)):
    """Return recent El Gordo buy-queue items for scraper_metadata last_draw_date (draw_date on each item must match)."""
    if db is None:
        return JSONResponse(
            content={"items": [], "last_draw_date": None, "next_draw_date": _get_next_draw_date("el-gordo")},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    coll = db[EL_GORDO_BUY_QUEUE_COLLECTION]
    last_draw_date = _get_last_draw_date("el-gordo")
    next_draw_date = _get_next_draw_date("el-gordo")
    queue_filter: Dict[str, Any] = {}
    if isinstance(last_draw_date, str) and last_draw_date.strip():
        queue_filter["draw_date"] = last_draw_date.strip()[:10]
    else:
        if next_draw_date:
            next_draw_date = next_draw_date.strip()[:10]
        return JSONResponse(
            content={"items": [], "last_draw_date": None, "next_draw_date": next_draw_date},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    cursor = coll.find(queue_filter).sort("created_at", -1).limit(limit)
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
    if isinstance(last_draw_date, str):
        last_draw_date = last_draw_date.strip()[:10]
    if isinstance(next_draw_date, str):
        next_draw_date = next_draw_date.strip()[:10]
    return JSONResponse(
        content={"items": items, "last_draw_date": last_draw_date, "next_draw_date": next_draw_date},
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


@app.post("/api/el-gordo/betting/save-queue-after-print")
async def api_el_gordo_betting_save_queue_after_print(request: Request):
    """Print buy queue → sync into el_gordo_train_progress.bought_tickets; promote waiting/failed with tickets."""
    if db is None:
        return JSONResponse(
            content={"status": "ok", "saved_count": 0, "promoted_count": 0},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    coll = db[EL_GORDO_BUY_QUEUE_COLLECTION]
    try:
        body = await request.json() or {}
    except Exception:
        body = {}
    selected_oids: List[ObjectId] = []
    for qid in (body.get("queue_ids") or []):
        try:
            selected_oids.append(ObjectId(str(qid)))
        except Exception:
            continue
    id_filter: Dict[str, Any] = {"_id": {"$in": selected_oids}} if selected_oids else {}
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    saved_count = 0
    promoted_count = 0
    cursor = coll.find({
        **id_filter,
        "status": "bought",
        "$or": [{"saved_status": {"$ne": True}}, {"saved_status": {"$exists": False}}],
    })
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
    for d in coll.find({**id_filter, "status": {"$in": ["waiting", "failed"]}}):
        tickets = d.get("tickets") or []
        if not tickets:
            continue
        oid = d["_id"]
        draw_date = (d.get("draw_date") or "").strip()[:10] or None
        cutoff_draw_id = (d.get("cutoff_draw_id") or "").strip() or None
        _append_el_gordo_bought_tickets(tickets, draw_date=draw_date, cutoff_draw_id=cutoff_draw_id)
        coll.update_one(
            {"_id": oid},
            {"$set": {"status": "bought", "saved_status": True, "finished_at": now, "error": None}},
        )
        promoted_count += 1
    return JSONResponse(
        content={"status": "ok", "saved_count": saved_count, "promoted_count": promoted_count},
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


@app.delete("/api/el-gordo/betting/buy-queue/waiting")
def api_el_gordo_betting_buy_queue_delete_all_waiting(draw_date: Optional[str] = Query(None)):
    """Remove waiting queue rows, optionally only for one draw_date."""
    if db is None:
        raise HTTPException(status_code=500, detail="Database not connected")
    coll = db[EL_GORDO_BUY_QUEUE_COLLECTION]
    q: Dict[str, Any] = {"status": "waiting"}
    draw_date_clean = (draw_date or "").strip()[:10]
    if draw_date_clean:
        q["draw_date"] = draw_date_clean
    result = coll.delete_many(q)
    return JSONResponse(
        content={"status": "ok", "deleted_count": result.deleted_count},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.post("/api/el-gordo/betting/buy-queue/{queue_id}/repair")
def api_el_gordo_betting_buy_queue_repair(queue_id: str):
    """Set failed/in_progress queue item back to waiting so bot can retry."""
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
    if st not in ("in_progress", "failed"):
        raise HTTPException(status_code=400, detail="Solo se puede reparar cuando está Comprando (in_progress) o Error (failed)")
    coll.update_one(
        {"_id": oid, "status": {"$in": ["in_progress", "failed"]}},
        {"$set": {"status": "waiting", "started_at": None, "finished_at": None, "error": None}},
    )
    return JSONResponse(content={"status": "ok", "message": "Cola reparada y reenviada a waiting"}, headers={"Cache-Control": "no-store, no-cache, must-revalidate"})


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
                sp = _fw_split_euromillones_line(line)
                if not sp:
                    continue
                position, mains_str, stars_str = sp
                try:
                    mains = [int(x) for x in mains_str.split(",") if x]
                    stars = [int(x) for x in stars_str.split(",") if x]
                except Exception:
                    continue
                parts = line.split(";")
                tid = parts[0] if len(parts) >= 4 and "_COMBO_" in (parts[0] or "") else None
                row: Dict[str, Any] = {"position": position, "mains": mains, "stars": stars}
                if tid:
                    row["ticket_id"] = tid
                tickets.append(row)
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


def _euromillones_read_line_payloads(path: str, positions: set[int]) -> Dict[int, Tuple[str, str]]:
    """Read (mains_str, stars_str) for given 1-based line indices; scan whole file if needed."""
    found: Dict[int, Tuple[str, str]] = {}
    if not positions:
        return found
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            raw = line.rstrip("\n")
            if not raw:
                continue
            sp = _fw_split_euromillones_line(raw)
            if not sp:
                continue
            position, mains_str, stars_str = sp
            if position in positions:
                found[position] = (mains_str, stars_str)
    return found


def _euromillones_resolve_ranks_to_physical(
    planned: List[Tuple[int, int]],
    bought_lines: set[int],
    max_line: int,
) -> Tuple[List[Tuple[int, int]], List[Tuple[int, int]]]:
    """
    planned: (old_line, new_rank) where new_rank is 1-based index among non-bought lines.
    Returns (resolved_pairs, unresolved_planned).
    """
    if not planned or max_line < 1:
        return [], list(planned)
    pending = list(planned)
    resolved: List[Tuple[int, int]] = []
    c = 0
    for i in range(1, max_line + 1):
        if i in bought_lines:
            continue
        c += 1
        next_pending: List[Tuple[int, int]] = []
        for old_pos, new_rank in pending:
            if new_rank == c:
                resolved.append((old_pos, i))
            else:
                next_pending.append((old_pos, new_rank))
        pending = next_pending
        if not pending:
            break
    return resolved, pending


def _el_gordo_read_line_payloads(path: str, positions: set[int]) -> Dict[int, Tuple[str, str]]:
    """Read (mains_str, clave_str) for given 1-based line indices."""
    found: Dict[int, Tuple[str, str]] = {}
    if not positions:
        return found
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            raw = line.rstrip("\n")
            if not raw:
                continue
            sp = _fw_split_el_gordo_line(raw)
            if not sp:
                continue
            position, mains_str, clave_str = sp
            if position in positions:
                found[position] = (mains_str, clave_str)
    return found

def _la_primitiva_read_line_payloads(path: str, positions: set[int]) -> Dict[int, str]:
    """Read payload for given 1-based line indices (La Primitiva TXT: ...;position;mains[;reintegro])."""
    found: Dict[int, str] = {}
    if not positions:
        return found
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            raw = line.rstrip("\n")
            if not raw:
                continue
            sp = _fw_split_la_primitiva_line(raw)
            if not sp:
                continue
            position, mains_str, rein_str = sp
            if position in positions:
                r = (rein_str or "").strip()
                found[position] = f"{mains_str};{r}" if r != "" else mains_str
    return found


def _euromillones_full_wheel_compare(
    current_id: str,
    pre_id: str,
    db_instance,
) -> Dict:
    """
    Compare full wheel TXT against draw result: stream TXT until jackpot (5+2), then
    aggregate prize counts and earnings per category using escrutinio premio per category.
    Save to euromillones_compare_results.

    NOTE: Compare GET endpoints return cached or TXT-based results only (no synthetic fallback).
    by the public API endpoint; this helper always attempts to compute a fresh
    compare result from TXT and may raise if the TXT/progress is missing.
    """
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
                sp = _fw_split_euromillones_line(line)
                if not sp:
                    continue
                position, mains_str, stars_str = sp
                try:
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
    _save_compare_result(db_instance, "euromillones", result)
    return result


def _draw_id_variants(value: Any) -> List[Any]:
    """String and int forms for Mongo keys (id_sorteo stored both ways)."""
    s = str(value or "").strip()
    if not s:
        return []
    out: List[Any] = [s]
    if s.isdigit():
        out.append(int(s))
    return out


def _compare_pair_keys(current_id: str, pre_id: str) -> List[Tuple[Any, Any]]:
    keys: List[Tuple[Any, Any]] = []
    seen: set = set()
    for ck in _draw_id_variants(current_id):
        for pk in _draw_id_variants(pre_id):
            pair = (ck, pk)
            if pair not in seen:
                seen.add(pair)
                keys.append(pair)
    return keys


def _find_compare_doc(coll, current_id: str, pre_id: str) -> Optional[Dict[str, Any]]:
    for ck, pk in _compare_pair_keys(current_id, pre_id):
        doc = coll.find_one({"current_id": ck, "pre_id": pk})
        if doc:
            return doc
    return None


def _compare_jackpot_value(doc: Optional[Dict[str, Any]]) -> Optional[int]:
    if not doc:
        return None
    for key in ("jackpot_position", "special_position"):
        raw = doc.get(key)
        if raw is None:
            continue
        try:
            val = int(raw)
            if val > 0:
                return val
        except (TypeError, ValueError):
            continue
    return None


def _compare_result_ready(doc: Optional[Dict[str, Any]]) -> bool:
    return _compare_jackpot_value(doc) is not None


def _delete_compare_pair_variants(coll, current_id: str, pre_id: str) -> int:
    deleted = 0
    for ck, pk in _compare_pair_keys(current_id, pre_id):
        res = coll.delete_many({"current_id": ck, "pre_id": pk})
        deleted += res.deleted_count
    return deleted


def _prepare_reorder_compare_cache(
    coll,
    current_id: str,
    pre_id: str,
    *,
    force: bool,
) -> Optional[Dict[str, Any]]:
    """Drop invalid/stale cache; return ready doc unless ``force`` recalculate."""
    existing = _find_compare_doc(coll, current_id, pre_id)
    if existing and not _compare_result_ready(existing):
        _delete_compare_pair_variants(coll, current_id, pre_id)
        return None
    if existing and force:
        _delete_compare_pair_variants(coll, current_id, pre_id)
        return None
    if existing and _compare_result_ready(existing):
        return existing
    return None


_compare_slots_busy: set[str] = set()
_compare_slot_lock = threading.Lock()


def _compare_slot_busy(lottery_api: str) -> bool:
    key = lottery_api.replace("-", "_")
    with _compare_slot_lock:
        return key in _compare_slots_busy


def _try_acquire_compare_slot(lottery_api: str) -> bool:
    key = lottery_api.replace("-", "_")
    with _compare_slot_lock:
        if key in _compare_slots_busy:
            return False
        _compare_slots_busy.add(key)
        return True


def _release_compare_slot(lottery_api: str) -> None:
    key = lottery_api.replace("-", "_")
    with _compare_slot_lock:
        _compare_slots_busy.discard(key)


def _full_wheel_path_for_pre_id(
    db_instance, progress_collection: str, pre_id: str
) -> Optional[str]:
    coll = db_instance[progress_collection]
    progress_doc = None
    for variant in _draw_id_variants(pre_id):
        progress_doc = coll.find_one({"cutoff_draw_id": variant})
        if progress_doc:
            break
        progress_doc = coll.find_one({"probs_draw_id": variant})
        if progress_doc:
            break
    if not progress_doc:
        return None
    path = (progress_doc.get("full_wheel_file_path") or "").strip()
    if path and os.path.isfile(path):
        return path
    return None


def _find_el_gordo_compare_doc(
    db_instance, current_id: str, pre_id: str
) -> Optional[Dict[str, Any]]:
    """Load saved compare from MongoDB (string or int id_sorteo keys)."""
    coll = db_instance[EL_GORDO_COMPARE_RESULTS_COLLECTION]
    doc = _find_compare_doc(coll, current_id.strip(), pre_id.strip())
    return doc if _compare_result_ready(doc) else None


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
    current_id_clean = current_id.strip()
    existing = _find_el_gordo_compare_doc(db_instance, current_id_clean, pre_id_clean)
    if existing:
        out = {k: v for k, v in existing.items() if k != "_id"}
        return _item_to_json(out)

    coll_draws = db_instance["el_gordo"]
    doc = coll_draws.find_one({"id_sorteo": current_id})
    if doc is None and current_id.isdigit():
        doc = coll_draws.find_one({"id_sorteo": int(current_id)})
    if not doc:
        raise HTTPException(404, detail="El Gordo draw not found")
    draw = _build_draw(doc, "ELGR")
    main_draw, clave_draw = _parse_el_gordo_mains_clave(doc)
    print(
        f"[el-gordo-compare] current_id={current_id!r} mains={main_draw} clave={clave_draw}",
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
    # All official tiers through 8ª + reintegro (must scan past jackpot — lower tiers appear later in TXT).
    _tier_keys: Tuple[Tuple[int, int], ...] = (
        (5, 1), (5, 0), (4, 1), (4, 0), (3, 1), (3, 0), (2, 1), (2, 0), (0, 1),
    )

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            sp = _fw_split_el_gordo_line(line)
            if not sp:
                continue
            position, mains_str, clave_str = sp
            try:
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
            if hits_main == 5 and hits_clave == 1 and jackpot_position is None:
                jackpot_position = position
            elif hits_main == 5 and hits_clave == 0 and second_first is None:
                second_first = position
            elif hits_main == 4 and hits_clave == 1 and third_first is None:
                third_first = position
            elif hits_main == 4 and hits_clave == 0 and fourth_first is None:
                fourth_first = position
            if jackpot_position is not None and all(k in category_first_pos for k in _tier_keys):
                break

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

    fifth_first = category_first_pos.get((3, 1))
    sixth_first = category_first_pos.get((3, 0))

    draw_date_norm = (doc.get("fecha_sorteo") or draw.get("fecha_sorteo") or "")[:10] or None
    result = {
        "current_id": current_id,
        "date": draw_date_norm,
        "pre_id": pre_id_clean,
        "jackpot_position": jackpot_position,
        "pos_2th": second_first,
        "pos_3th": third_first,
        "pos_4th": fourth_first,
        "pos_5th": fifth_first,
        "pos_6th": sixth_first,
        "categories": categories_out,
        "total_categories": len(categories_out),
    }
    _save_compare_result(db_instance, "el-gordo", result)
    print(f"[el-gordo-compare] saved result for current_id={current_id} pre_id={pre_id_clean} jackpot_position={jackpot_position}", flush=True)
    return result


def _el_gordo_category_first_position(
    categories: List[Dict[str, Any]], main_hits: int, clave_hit: int
) -> Optional[int]:
    for c in categories or []:
        if not isinstance(c, dict):
            continue
        if int(c.get("main_hits") or 0) == main_hits and int(c.get("clave_hit") or 0) == clave_hit:
            fp = int(c.get("first_position") or 0)
            return fp if fp > 0 else None
    return None


def _el_gordo_analysis_row_from_compare_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Map el_gordo_compare_results doc to analysis table fields (1ª–8ª tiers)."""
    date_str = (doc.get("date") or "")[:10]
    cats: List[Dict[str, Any]] = []
    for c in doc.get("categories") or []:
        if not isinstance(c, dict):
            continue
        cats.append(
            {
                "category": str(c.get("category") or ""),
                "main_hits": int(c.get("main_hits") or 0),
                "clave_hit": int(c.get("clave_hit") or 0),
                "first_position": int(c.get("first_position") or 0),
                "count": int(c.get("count") or 0),
            }
        )

    def _pos(field: str, hm: int, hc: int) -> Optional[int]:
        raw = doc.get(field)
        if raw is not None:
            try:
                v = int(raw)
                if v > 0:
                    return v
            except (TypeError, ValueError):
                pass
        return _el_gordo_category_first_position(cats, hm, hc)

    # pos_1th..pos_8th = official 1ª..8ª (5+1 through 2+0); no separate "special" tier.
    pos_1th = _pos("jackpot_position", 5, 1)
    pos_2th = _pos("pos_2th", 5, 0)
    pos_3th = _pos("pos_3th", 4, 1)
    pos_4th = _pos("pos_4th", 4, 0)
    pos_5th = _pos("pos_5th", 3, 1)
    pos_6th = _pos("pos_6th", 3, 0)
    pos_7th = _el_gordo_category_first_position(cats, 2, 1)
    pos_8th = _el_gordo_category_first_position(cats, 2, 0)
    return {
        "date": date_str,
        "current_id": str(doc.get("current_id") or ""),
        "pre_id": str(doc.get("pre_id") or ""),
        "jackpot_position": pos_1th,
        "special_position": pos_1th,
        "pos_1th": pos_1th,
        "pos_2th": pos_2th,
        "pos_3th": pos_3th,
        "pos_4th": pos_4th,
        "pos_5th": pos_5th,
        "pos_6th": pos_6th,
        "pos_7th": pos_7th,
        "pos_8th": pos_8th,
        "categories": cats,
    }


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
    - Also record special_position (6 mains + reintegro), and first positions for
      1st(6 hits), 2nd(5+C), 3rd(5 hits), 4th(4 hits), 5th(3 hits).
    - Stop scanning once special (6+R) and early prizes are found
      (when complementario is set, wait for 2nd too).
    - Save summary to la_primitiva_compare_results.

    NOTE: Compare GET endpoints return cached or TXT-based results only (no synthetic fallback).
    by the public API endpoint; this helper always attempts to compute a fresh
    compare result from TXT and may raise if the TXT/progress is missing.
    """
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

    current_id_clean = current_id.strip()
    pre_id_clean = pre_id.strip()

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

    # We care about the official La Primitiva categories plus Especial (6 + R):
    # Especial: (6,1) -> 6 aciertos + reintegro
    # 1ª: (6,0) -> 6 aciertos
    # 2ª: (5,1) -> 5 + C
    # 3ª: (5,0) -> 5 aciertos
    # 4ª: (4,0) -> 4 aciertos
    # 5ª: (3,0) -> 3 aciertos
    category_first_pos: Dict[Tuple[int, int], int] = {}
    category_counts: Dict[Tuple[int, int], int] = {}
    special_position: Optional[int] = None  # Especial (6 + R)
    jackpot_position: Optional[int] = None  # 1ª (6 mains)
    second_first: Optional[int] = None   # 2ª (5+C)
    third_first: Optional[int] = None   # 3ª (5 aciertos)
    fourth_first: Optional[int] = None  # 4ª (4 aciertos)
    fifth_first: Optional[int] = None  # 5ª (3 aciertos)

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            sp = _fw_split_la_primitiva_line(line)
            if not sp:
                continue
            position, mains_str, rein_str = sp
            try:
                mains = [int(x) for x in mains_str.split(",") if x]
            except Exception:
                continue
            if len(mains) != 6:
                continue
            rein_val: Optional[int] = None
            if rein_str is not None and str(rein_str).strip() != "":
                try:
                    rein_val = int(str(rein_str).strip())
                except Exception:
                    rein_val = None
            hits_main = sum(1 for n in mains if n in main_set)
            has_complementario = bool(
                complementario_set and any(n in complementario_set for n in mains)
            )

            # Map to one of the 5 official categories (or ignore if not matching any).
            key: Optional[Tuple[int, int]] = None
            if hits_main == 6 and reintegro_draw is not None and rein_val is not None and rein_val == int(reintegro_draw):
                key = (6, 1)  # Especial (6 + R)
            elif hits_main == 6:
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

            if hits_main == 6 and reintegro_draw is not None and rein_val is not None and rein_val == int(reintegro_draw):
                if special_position is None:
                    special_position = position
            if hits_main == 6:
                if jackpot_position is None:
                    jackpot_position = position
            elif hits_main == 5 and has_complementario:
                if second_first is None:
                    second_first = position
            elif hits_main == 5:
                if third_first is None:
                    third_first = position
            elif hits_main == 4:
                if fourth_first is None:
                    fourth_first = position
            elif hits_main == 3:
                if fifth_first is None:
                    fifth_first = position

            # Stop once we have special (6+R) and early prizes.
            if special_position is not None and third_first is not None and (
                complementario_draw is None or second_first is not None
            ):
                break

    if special_position is None:
        raise HTTPException(
            404,
            detail="Special jackpot (6 mains + reintegro) not found in La Primitiva full wheel file; cannot compute compare result",
        )

    coll_compare = db_instance[LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION]

    # Build categories array in fixed canonical order.
    ordered_keys: List[Tuple[Tuple[int, int], str]] = [
        ((6, 1), "Especial (6 aciertos + R)"),
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
        "current_id": current_id_clean,
        "date": draw_date,
        "pre_id": pre_id_clean,
        "jackpot_position": special_position,
        "special_position": special_position,
        "pos_1th": jackpot_position,
        "pos_2th": second_first,
        "pos_3th": third_first,
        "pos_4th": fourth_first,
        "pos_5th": fifth_first,
        "categories": categories_out,
        "total_categories": len(categories_out),
    }
    _save_compare_result(db_instance, "la-primitiva", result)
    print(
        f"[la-prim-compare] saved result for current_id={current_id_clean} pre_id={pre_id_clean} special_position={special_position}",
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

    current_id_clean = current_id.strip()
    pre_id_clean = pre_id.strip()
    coll_compare = db[EUROMILLONES_COMPARE_RESULTS_COLLECTION]

    existing = _find_compare_doc(coll_compare, current_id_clean, pre_id_clean)
    if existing and _compare_result_ready(existing):
        out = {k: v for k, v in existing.items() if k != "_id"}
        return JSONResponse(content=_item_to_json(out))

    # 2) Compute from full-wheel TXT for pre_id (no synthetic fallback).
    try:
        result = _euromillones_full_wheel_compare(current_id_clean, pre_id_clean, db)
        return JSONResponse(content=_item_to_json(result))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=f"Euromillones compare failed: {e}")


@app.get("/api/euromillones/compare/cached")
def api_euromillones_compare_cached(
    current_id: str = Query(..., description="id_sorteo of the draw (result)."),
    pre_id: str = Query(..., description="cutoff_draw_id for the full wheel run."),
):
    """Return saved compare only (no TXT scan). Used by prediction UI polling."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    doc = _find_compare_doc(
        db[EUROMILLONES_COMPARE_RESULTS_COLLECTION],
        current_id.strip(),
        pre_id.strip(),
    )
    if not _compare_result_ready(doc):
        raise HTTPException(
            404,
            detail="No cached Euromillones compare for this draw pair; run reorder first",
        )
    out = {k: v for k, v in doc.items() if k != "_id"}
    return JSONResponse(content=_item_to_json(out))


@app.get("/api/la-primitiva/compare/cached")
def api_la_primitiva_compare_cached(
    current_id: str = Query(..., description="id_sorteo of the La Primitiva draw (result)."),
    pre_id: str = Query(..., description="cutoff_draw_id for the full wheel run."),
):
    """Return saved compare only (no TXT scan). Used by prediction UI polling."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    doc = _find_compare_doc(
        db[LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION],
        current_id.strip(),
        pre_id.strip(),
    )
    if not _compare_result_ready(doc):
        raise HTTPException(
            404,
            detail="No cached La Primitiva compare for this draw pair; run reorder first",
        )
    out = {k: v for k, v in doc.items() if k != "_id"}
    return JSONResponse(content=_item_to_json(out))


@app.get("/api/el-gordo/compare/cached")
def api_el_gordo_compare_cached(
    current_id: str = Query(..., description="id_sorteo of the El Gordo draw (result)."),
    pre_id: str = Query(..., description="cutoff_draw_id for the full wheel run."),
):
    """Return saved compare only (no TXT scan). Fast path for the simulation UI."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    doc = _find_el_gordo_compare_doc(db, current_id.strip(), pre_id.strip())
    if not doc:
        raise HTTPException(
            404,
            detail="No cached El Gordo compare for this draw pair; run full-wheel compare first",
        )
    out = {k: v for k, v in doc.items() if k != "_id"}
    return JSONResponse(content=_item_to_json(out))


@app.get("/api/la-primitiva/compare/full-wheel")
def api_la_primitiva_compare_full_wheel(
    current_id: str = Query(..., description="id_sorteo of the La Primitiva draw to evaluate (result)."),
    pre_id: str = Query(..., description="cutoff_draw_id or probs_draw_id for the La Primitiva full wheel run."),
):
    """
    Compare La Primitiva full wheel TXT (from pre_id) against draw result (current_id).
    Returns cached (current_id, pre_id) or computes from TXT. No synthetic fallback.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    current_id_clean = current_id.strip()
    pre_id_clean = pre_id.strip()
    coll_compare = db[LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION]

    existing = _find_compare_doc(coll_compare, current_id_clean, pre_id_clean)
    if existing and _compare_result_ready(existing):
        out = {k: v for k, v in existing.items() if k != "_id"}
        return JSONResponse(content=_item_to_json(out))

    # 2) Compute from full-wheel TXT for pre_id (no synthetic fallback).
    try:
        result = _la_primitiva_full_wheel_compare(current_id_clean, pre_id_clean, db)
        return JSONResponse(content=_item_to_json(result))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=f"La Primitiva compare failed: {e}")

@app.get("/api/el-gordo/compare/full-wheel")
def api_el_gordo_compare_full_wheel(
    current_id: str = Query(..., description="id_sorteo of the El Gordo draw to evaluate (result)."),
    pre_id: str = Query(..., description="cutoff_draw_id or probs_draw_id for the El Gordo full wheel run."),
):
    """
    Compare El Gordo full wheel TXT (from pre_id) against draw result (current_id).
    Returns cached (current_id, pre_id) when present; otherwise scans TXT (slow).
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    current_id_clean = current_id.strip()
    pre_id_clean = pre_id.strip()
    existing = _find_el_gordo_compare_doc(db, current_id_clean, pre_id_clean)
    if existing:
        out = {k: v for k, v in existing.items() if k != "_id"}
        return JSONResponse(content=_item_to_json(out))
    try:
        result = _el_gordo_full_wheel_compare(current_id_clean, pre_id_clean, db)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=f"El Gordo compare failed: {e}")
    return JSONResponse(content=_item_to_json(result))


# ── DB-based compare/full-wheel/reorder endpoints (new architecture) ─────────
#
# Strategy:
#   1. Check if tickets already exist in DB for this lottery.
#   2. If YES (not first draw): update scores from latest probabilities, then compare from DB.
#   3. If NO (first draw): generate ALL tickets, store in DB, then compare from DB.
#   4. Always save result to compare_results collection for caching.
#   5. Fall back to TXT-based compare if DB approach fails.

def _probs_list_to_dict(probs: list) -> dict:
    """Convert [{number: n, p: p}, ...] to {n: p}. Delegates to ticket_db."""
    return probs_list_to_dict(probs)


@app.post("/api/euromillones/compare/full-wheel/reorder")
def api_euromillones_compare_reorder(
    current_id: str = Query(..., description="id_sorteo of the draw to evaluate (result)."),
    pre_id: str = Query(..., description="cutoff_draw_id for the training run used to rank tickets."),
    force: bool = Query(False, description="Delete cached compare and recalculate"),
):
    """
    DB-based compare: rank existing tickets by model score, find jackpot position.

    Runs in a background thread (202). Uses full-wheel TXT when available (low RAM),
    otherwise streams the ticket pool from Mongo without loading all tickets into memory.
    Bootstrap tickets separately via POST /api/euromillones/tickets/bootstrap.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    current_id_clean = current_id.strip()
    pre_id_clean = pre_id.strip()

    # 1) Return cached result if available
    coll_compare = db[EUROMILLONES_COMPARE_RESULTS_COLLECTION]
    existing = _prepare_reorder_compare_cache(
        coll_compare, current_id_clean, pre_id_clean, force=force
    )
    if existing:
        out = {k: v for k, v in existing.items() if k != "_id"}
        return JSONResponse(content=_item_to_json(out))

    # 2) Load draw result
    coll_draws = db["euromillones"]
    draw_doc = coll_draws.find_one({"id_sorteo": current_id_clean})
    if draw_doc is None and current_id_clean.isdigit():
        draw_doc = coll_draws.find_one({"id_sorteo": int(current_id_clean)})
    if not draw_doc:
        raise HTTPException(404, detail="Draw not found")

    draw = _build_draw(draw_doc, "EMIL")
    numbers = draw.get("numbers") or []
    if len(numbers) >= 7:
        main_draw = [int(x) for x in numbers[:5]]
        star_draw = [int(x) for x in numbers[5:7]]
    else:
        parts = re.split(r"[\s\-]+", str(draw.get("combinacion_acta") or ""))
        nums = [int(p) for p in parts if p.isdigit()]
        main_draw = nums[:5] if len(nums) >= 5 else []
        star_draw = nums[5:7] if len(nums) >= 7 else []
    if len(main_draw) != 5 or len(star_draw) != 2:
        raise HTTPException(400, detail="Draw main/star numbers missing or invalid")

    escrutinio = draw.get("escrutinio") or []
    prize_map = _build_escrutinio_prize_map(escrutinio)
    premio_bote = _parse_euro_premio(draw.get("premio_bote"))
    if premio_bote >= 0:
        prize_map[(5, 2)] = premio_bote
    draw_date = (draw.get("fecha_sorteo") or "")[:10] or None

    # 3) Load probabilities — check draw_probs collection first (backfill), then train_progress
    mains_probs: Dict[str, Any] = {}
    stars_probs: Dict[str, Any] = {}

    draw_probs_doc = db[EUROMILLONES_DRAW_PROBS_COLLECTION].find_one({"draw_id": pre_id_clean})
    if draw_probs_doc:
        mains_probs = {int(k): float(v) for k, v in (draw_probs_doc.get("mains_probs") or {}).items()}
        stars_probs = {int(k): float(v) for k, v in (draw_probs_doc.get("stars_probs") or {}).items()}

    if not mains_probs:
        # Fall back to train_progress
        coll_progress = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
        progress_doc = coll_progress.find_one({"cutoff_draw_id": pre_id_clean})
        if not progress_doc and pre_id_clean.isdigit():
            progress_doc = coll_progress.find_one({"cutoff_draw_id": int(pre_id_clean)})
        if not progress_doc:
            progress_doc = coll_progress.find_one({"probs_draw_id": pre_id_clean})
        if progress_doc:
            mains_probs = _probs_list_to_dict(progress_doc.get("mains_probs") or [])
            stars_probs = _probs_list_to_dict(progress_doc.get("stars_probs") or [])

    if not mains_probs or not stars_probs:
        try:
            result = _euromillones_full_wheel_compare(current_id_clean, pre_id_clean, db)
            return JSONResponse(content=_item_to_json(result))
        except Exception as e:
            raise HTTPException(400, detail=f"Probabilities not found and TXT fallback failed: {e}")

    if _compare_slot_busy("euromillones"):
        raise HTTPException(
            503,
            detail="Compare already running for euromillones. Poll GET /api/euromillones/compare/cached.",
        )

    def _run_compare():
        if not _try_acquire_compare_slot("euromillones"):
            logging.warning("[reorder/euromillones] compare slot already taken")
            return
        try:
            if _full_wheel_path_for_pre_id(db, EUROMILLONES_TRAIN_PROGRESS_COLLECTION, pre_id_clean):
                try:
                    result = _euromillones_full_wheel_compare(current_id_clean, pre_id_clean, db)
                    logging.info(
                        "[reorder/euromillones] TXT compare done jackpot_position=%s",
                        result.get("jackpot_position"),
                    )
                    return
                except Exception as e:
                    logging.warning("[reorder/euromillones] TXT compare failed, trying DB: %s", e)

            tickets_coll = db[EUROMILLONES_TICKETS_COLLECTION]
            if tickets_coll.count_documents({"lottery": "euromillones"}) == 0:
                logging.error(
                    "[reorder/euromillones] ticket pool empty — bootstrap via "
                    "POST /api/euromillones/tickets/bootstrap (skipped during compare to avoid OOM)"
                )
                return

            update_euromillones_ranking(db, pre_id_clean, draw_date, mains_probs, stars_probs)
            result = compare_euromillones_from_db(
                db=db,
                current_id=current_id_clean,
                pre_id=pre_id_clean,
                main_draw=main_draw,
                star_draw=star_draw,
                prize_map=prize_map,
                draw_date=draw_date,
                ticket_cost_eur=EUROMILLONES_TICKET_COST_EUR,
            )
            _save_compare_result(db, "euromillones", result)
            logging.info(
                "[reorder/euromillones] DB compare done jackpot_position=%s",
                result.get("jackpot_position"),
            )
        except Exception as e:
            logging.exception("[reorder/euromillones] background compare failed: %s", e)
        finally:
            _release_compare_slot("euromillones")

    threading.Thread(target=_run_compare, daemon=True).start()
    return JSONResponse(
        status_code=202,
        content={"status": "started", "current_id": current_id_clean, "pre_id": pre_id_clean,
                 "message": "Compare running in background. Poll GET /compare/full-wheel for result."},
    )


@app.post("/api/el-gordo/compare/full-wheel/reorder")
def api_el_gordo_compare_reorder(
    current_id: str = Query(..., description="id_sorteo of the El Gordo draw to evaluate (result)."),
    pre_id: str = Query(..., description="cutoff_draw_id for the training run used to rank tickets."),
    force: bool = Query(False, description="Delete cached compare and recalculate"),
):
    """
    DB-based compare for El Gordo: rank existing tickets by model score, find jackpot position.

    Runs in a background thread (202). Uses full-wheel TXT when available (low RAM),
    otherwise streams the ticket pool from Mongo without loading all tickets into memory.
    Bootstrap tickets separately via POST /api/el-gordo/tickets/bootstrap.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    current_id_clean = current_id.strip()
    pre_id_clean = pre_id.strip()

    coll_compare = db[EL_GORDO_COMPARE_RESULTS_COLLECTION]
    existing = _prepare_reorder_compare_cache(
        coll_compare, current_id_clean, pre_id_clean, force=force
    )
    if existing:
        out = {k: v for k, v in existing.items() if k != "_id"}
        return JSONResponse(content=_item_to_json(out))

    # 2) Load draw result
    coll_draws = db["el_gordo"]
    draw_doc = coll_draws.find_one({"id_sorteo": current_id_clean})
    if draw_doc is None and current_id_clean.isdigit():
        draw_doc = coll_draws.find_one({"id_sorteo": int(current_id_clean)})
    if not draw_doc:
        raise HTTPException(404, detail="El Gordo draw not found")

    main_draw, clave_draw = _parse_el_gordo_mains_clave(draw_doc)

    if len(main_draw) != 5 or clave_draw is None:
        raise HTTPException(400, detail="El Gordo draw main/clave numbers missing or invalid")

    draw_date = (draw_doc.get("fecha_sorteo") or "")[:10] or None

    # 3) Load probabilities from training progress
    coll_progress = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
    # 3) Load probabilities — check draw_probs collection first, then train_progress
    mains_probs: Dict[str, Any] = {}
    clave_probs: Dict[str, Any] = {}

    draw_probs_doc = db[EL_GORDO_DRAW_PROBS_COLLECTION].find_one({"draw_id": pre_id_clean})
    if draw_probs_doc:
        mains_probs = {int(k): float(v) for k, v in (draw_probs_doc.get("mains_probs") or {}).items()}
        clave_probs = {int(k): float(v) for k, v in (draw_probs_doc.get("clave_probs") or {}).items()}

    if not mains_probs:
        coll_progress = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
        progress_doc = coll_progress.find_one({"cutoff_draw_id": pre_id_clean})
        if not progress_doc and pre_id_clean.isdigit():
            progress_doc = coll_progress.find_one({"cutoff_draw_id": int(pre_id_clean)})
        if not progress_doc:
            progress_doc = coll_progress.find_one({"probs_draw_id": pre_id_clean})
        if progress_doc:
            mains_probs = _probs_list_to_dict(progress_doc.get("mains_probs") or [])
            clave_probs = _probs_list_to_dict(progress_doc.get("clave_probs") or [])

    if not mains_probs:
        try:
            result = _el_gordo_full_wheel_compare(current_id_clean, pre_id_clean, db)
            return JSONResponse(content=_item_to_json(result))
        except Exception as e:
            raise HTTPException(400, detail=f"Probabilities not found and TXT fallback failed: {e}")

    if _compare_slot_busy("el-gordo"):
        raise HTTPException(
            503,
            detail="Compare already running for el-gordo. Poll GET /api/el-gordo/compare/cached.",
        )

    def _run_compare():
        if not _try_acquire_compare_slot("el-gordo"):
            logging.warning("[reorder/el-gordo] compare slot already taken")
            return
        try:
            if _full_wheel_path_for_pre_id(db, EL_GORDO_TRAIN_PROGRESS_COLLECTION, pre_id_clean):
                try:
                    result = _el_gordo_full_wheel_compare(current_id_clean, pre_id_clean, db)
                    logging.info(
                        "[reorder/el-gordo] TXT compare done jackpot_position=%s",
                        result.get("jackpot_position"),
                    )
                    return
                except Exception as e:
                    logging.warning("[reorder/el-gordo] TXT compare failed, trying DB: %s", e)

            tickets_coll = db[EL_GORDO_TICKETS_COLLECTION]
            if tickets_coll.count_documents({"lottery": "el_gordo"}) == 0:
                logging.error(
                    "[reorder/el-gordo] ticket pool empty — bootstrap via "
                    "POST /api/el-gordo/tickets/bootstrap (skipped during compare to avoid OOM)"
                )
                return

            update_el_gordo_ranking(db, pre_id_clean, draw_date, mains_probs, clave_probs)
            result = compare_el_gordo_from_db(
                db=db,
                current_id=current_id_clean,
                pre_id=pre_id_clean,
                main_draw=main_draw,
                clave_draw=clave_draw,
                draw_date=draw_date,
            )
            _save_compare_result(db, "el-gordo", result)
            logging.info(
                "[reorder/el-gordo] DB compare done jackpot_position=%s",
                result.get("jackpot_position"),
            )
        except Exception as e:
            logging.exception("[reorder/el-gordo] background compare failed: %s", e)
        finally:
            _release_compare_slot("el-gordo")

    threading.Thread(target=_run_compare, daemon=True).start()
    return JSONResponse(
        status_code=202,
        content={"status": "started", "current_id": current_id_clean, "pre_id": pre_id_clean,
                 "message": "Compare running in background. Poll GET /compare/full-wheel for result."},
    )


@app.post("/api/la-primitiva/compare/full-wheel/reorder")
def api_la_primitiva_compare_reorder(
    current_id: str = Query(..., description="id_sorteo of the La Primitiva draw to evaluate (result)."),
    pre_id: str = Query(..., description="cutoff_draw_id for the training run used to rank tickets."),
    force: bool = Query(False, description="Delete cached compare and recalculate"),
):
    """
    DB-based compare for La Primitiva: rank existing tickets by model score, find jackpot position.

    Runs in a background thread (202). Uses full-wheel TXT when available (low RAM),
    otherwise streams the ticket pool from Mongo without loading all tickets into memory.
    Bootstrap tickets separately via POST /api/la-primitiva/tickets/bootstrap.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    current_id_clean = current_id.strip()
    pre_id_clean = pre_id.strip()

    # 1) Return cached result if available
    coll_compare = db[LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION]
    existing = _prepare_reorder_compare_cache(
        coll_compare, current_id_clean, pre_id_clean, force=force
    )
    if existing:
        out = {k: v for k, v in existing.items() if k != "_id"}
        return JSONResponse(content=_item_to_json(out))

    # 2) Load draw result
    coll_draws = db["la_primitiva"]
    draw_doc = coll_draws.find_one({"id_sorteo": current_id_clean})
    if draw_doc is None and current_id_clean.isdigit():
        draw_doc = coll_draws.find_one({"id_sorteo": int(current_id_clean)})
    if not draw_doc:
        raise HTTPException(404, detail="La Primitiva draw not found")

    draw = _build_draw(draw_doc, "LAPR")
    numbers = draw.get("numbers") or []
    combinacion = draw.get("combinacion_acta") or draw.get("combinacion") or ""
    if len(numbers) >= 6:
        main_draw = [int(x) for x in numbers[:6]]
        complementario = int(numbers[6]) if len(numbers) >= 7 else None
        reintegro_raw = numbers[7] if len(numbers) >= 8 else None
        reintegro_draw = int(reintegro_raw) if reintegro_raw is not None else None
    else:
        parts = re.split(r"[\s\-]+", str(combinacion))
        nums = [int(p) for p in parts if p.isdigit()]
        main_draw = nums[:6] if len(nums) >= 6 else []
        complementario = nums[6] if len(nums) >= 7 else None
        reintegro_draw = nums[7] if len(nums) >= 8 else None

    # Try to get reintegro from dedicated field if not parsed from numbers
    if reintegro_draw is None:
        r_raw = draw_doc.get("reintegro") or draw_doc.get("R")
        if r_raw is not None:
            try:
                reintegro_draw = int(r_raw)
            except (TypeError, ValueError):
                reintegro_draw = None

    if len(main_draw) != 6:
        raise HTTPException(400, detail="La Primitiva draw main numbers missing or invalid")
    if reintegro_draw is None:
        reintegro_draw = 0  # safe default

    draw_date = (draw.get("fecha_sorteo") or "")[:10] or None

    # 3) Load probabilities — draw_probs first, then train_progress
    mains_probs: Dict[str, Any] = {}
    reintegro_probs: Dict[str, Any] = {}

    draw_probs_doc = db[LA_PRIMITIVA_DRAW_PROBS_COLLECTION].find_one({"draw_id": pre_id_clean})
    if draw_probs_doc:
        mains_probs     = {int(k): float(v) for k, v in (draw_probs_doc.get("mains_probs") or {}).items()}
        reintegro_probs = {int(k): float(v) for k, v in (draw_probs_doc.get("rein_probs") or {}).items()}

    if not mains_probs:
        coll_progress = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
        progress_doc = coll_progress.find_one({"cutoff_draw_id": pre_id_clean})
        if not progress_doc and pre_id_clean.isdigit():
            progress_doc = coll_progress.find_one({"cutoff_draw_id": int(pre_id_clean)})
        if not progress_doc:
            progress_doc = coll_progress.find_one({"probs_draw_id": pre_id_clean})
        if progress_doc:
            mains_probs     = _probs_list_to_dict(progress_doc.get("mains_probs") or [])
            reintegro_probs = _probs_list_to_dict(progress_doc.get("reintegros_probs") or progress_doc.get("reintegro_probs") or [])

    if not mains_probs:
        try:
            result = _la_primitiva_full_wheel_compare(current_id_clean, pre_id_clean, db)
            return JSONResponse(content=_item_to_json(result))
        except Exception as e:
            raise HTTPException(400, detail=f"Probabilities not found and TXT fallback failed: {e}")

    if _compare_slot_busy("la-primitiva"):
        raise HTTPException(
            503,
            detail="Compare already running for la-primitiva. Poll GET /api/la-primitiva/compare/cached.",
        )

    def _run_compare():
        if not _try_acquire_compare_slot("la-primitiva"):
            logging.warning("[reorder/la-primitiva] compare slot already taken")
            return
        try:
            if _full_wheel_path_for_pre_id(db, LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION, pre_id_clean):
                try:
                    result = _la_primitiva_full_wheel_compare(current_id_clean, pre_id_clean, db)
                    logging.info(
                        "[reorder/la-primitiva] TXT compare done jackpot_position=%s",
                        result.get("jackpot_position") or result.get("special_position"),
                    )
                    return
                except Exception as e:
                    logging.warning("[reorder/la-primitiva] TXT compare failed, trying DB: %s", e)

            tickets_coll = db[LA_PRIMITIVA_TICKETS_COLLECTION]
            if tickets_coll.count_documents({"lottery": "la_primitiva"}) == 0:
                logging.error(
                    "[reorder/la-primitiva] ticket pool empty — bootstrap via "
                    "POST /api/la-primitiva/tickets/bootstrap (skipped during compare to avoid OOM)"
                )
                return

            update_la_primitiva_ranking(db, pre_id_clean, draw_date, mains_probs, reintegro_probs)
            result = compare_la_primitiva_from_db(
                db=db,
                current_id=current_id_clean,
                pre_id=pre_id_clean,
                main_draw=main_draw,
                reintegro_draw=reintegro_draw,
                complementario_draw=complementario,
                draw_date=draw_date,
            )
            _save_compare_result(db, "la-primitiva", result)
            logging.info(
                "[reorder/la-primitiva] DB compare done jackpot_position=%s",
                result.get("jackpot_position") or result.get("special_position"),
            )
        except Exception as e:
            logging.exception("[reorder/la-primitiva] background compare failed: %s", e)
        finally:
            _release_compare_slot("la-primitiva")

    threading.Thread(target=_run_compare, daemon=True).start()
    return JSONResponse(
        status_code=202,
        content={"status": "started", "current_id": current_id_clean, "pre_id": pre_id_clean,
                 "message": "Compare running in background. Poll GET /compare/full-wheel for result."},
    )


# ── Ticket pool status endpoints ──────────────────────────────────────────────

@app.get("/api/euromillones/tickets/status")
def api_euromillones_tickets_status():
    """Return count and sample of stored Euromillones tickets in DB."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    coll = db[EUROMILLONES_TICKETS_COLLECTION]
    total = coll.count_documents({"lottery": "euromillones"})
    sample = list(coll.find(
        {"lottery": "euromillones"},
        projection={"_id": 0, "position": 1, "mains": 1, "stars": 1, "tier": 1, "score": 1},
        sort=[("score", -1)],
        limit=5,
    ))
    return JSONResponse(content={"total_tickets": total, "top_5": sample})


@app.get("/api/el-gordo/tickets/status")
def api_el_gordo_tickets_status():
    """Return count and sample of stored El Gordo tickets in DB."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    coll = db[EL_GORDO_TICKETS_COLLECTION]
    total = coll.count_documents({"lottery": "el_gordo"})
    sample = list(coll.find(
        {"lottery": "el_gordo"},
        projection={"_id": 0, "position": 1, "mains": 1, "clave": 1, "tier": 1, "score": 1},
        sort=[("score", -1)],
        limit=5,
    ))
    return JSONResponse(content={"total_tickets": total, "top_5": sample})


@app.get("/api/la-primitiva/tickets/status")
def api_la_primitiva_tickets_status():
    """Return count and sample of stored La Primitiva tickets in DB."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    coll = db[LA_PRIMITIVA_TICKETS_COLLECTION]
    total = coll.count_documents({"lottery": "la_primitiva"})
    sample = list(coll.find(
        {"lottery": "la_primitiva"},
        projection={"_id": 0, "position": 1, "mains": 1, "reintegro": 1, "tier": 1, "score": 1},
        sort=[("score", -1)],
        limit=5,
    ))
    return JSONResponse(content={"total_tickets": total, "top_5": sample})


# ── Bootstrap endpoints (one-time, run once before any draw) ──────────────────
#
# Call these ONCE before running any draw pipeline.
# They generate the complete ticket universe and store it permanently in DB.
# After that, only scores are updated — no regeneration ever.
# Safe to call again if interrupted (skips already-inserted tickets).

_bootstrap_status: Dict[str, Any] = {}  # in-memory progress tracker


@app.post("/api/euromillones/tickets/bootstrap")
def api_euromillones_tickets_bootstrap():
    """
    One-time: generate ALL C(50,5)×C(12,2) ≈ 139M Euromillones tickets and store in DB.
    Runs in background. Poll GET /api/euromillones/tickets/status to track progress.
    Safe to call again if interrupted — already-inserted tickets are skipped.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    lottery = "euromillones"
    if _bootstrap_status.get(lottery) == "running":
        return JSONResponse(content={"status": "running", "message": "Bootstrap already in progress."})

    def _run():
        _bootstrap_status[lottery] = "running"
        try:
            def progress_cb(n: int):
                _bootstrap_status[f"{lottery}_count"] = n
            result = bootstrap_euromillones_tickets(db, progress_cb=progress_cb)
            _bootstrap_status[lottery] = "done"
            _bootstrap_status[f"{lottery}_result"] = result
            logging.info("[bootstrap/euromillones] done: %s", result)
        except Exception as e:
            _bootstrap_status[lottery] = "error"
            _bootstrap_status[f"{lottery}_error"] = str(e)
            logging.exception("[bootstrap/euromillones] error: %s", e)

    threading.Thread(target=_run, daemon=True).start()
    return JSONResponse(content={"status": "started", "message": "Bootstrap running in background. Poll /api/euromillones/tickets/status."})


@app.post("/api/el-gordo/tickets/bootstrap")
def api_el_gordo_tickets_bootstrap():
    """
    One-time: generate ALL C(54,5)×10 ≈ 31M El Gordo tickets and store in DB.
    Runs in background. Poll GET /api/el-gordo/tickets/status to track progress.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    lottery = "el_gordo"
    if _bootstrap_status.get(lottery) == "running":
        return JSONResponse(content={"status": "running", "message": "Bootstrap already in progress."})

    def _run():
        _bootstrap_status[lottery] = "running"
        try:
            def progress_cb(n: int):
                _bootstrap_status[f"{lottery}_count"] = n
            result = bootstrap_el_gordo_tickets(db, progress_cb=progress_cb)
            _bootstrap_status[lottery] = "done"
            _bootstrap_status[f"{lottery}_result"] = result
            logging.info("[bootstrap/el-gordo] done: %s", result)
        except Exception as e:
            _bootstrap_status[lottery] = "error"
            _bootstrap_status[f"{lottery}_error"] = str(e)
            logging.exception("[bootstrap/el-gordo] error: %s", e)

    threading.Thread(target=_run, daemon=True).start()
    return JSONResponse(content={"status": "started", "message": "Bootstrap running in background. Poll /api/el-gordo/tickets/status."})


@app.post("/api/la-primitiva/tickets/bootstrap")
def api_la_primitiva_tickets_bootstrap():
    """
    One-time: generate ALL C(49,6)×10 ≈ 139M La Primitiva tickets and store in DB.
    Runs in background. Poll GET /api/la-primitiva/tickets/status to track progress.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    lottery = "la_primitiva"
    if _bootstrap_status.get(lottery) == "running":
        return JSONResponse(content={"status": "running", "message": "Bootstrap already in progress."})

    def _run():
        _bootstrap_status[lottery] = "running"
        try:
            def progress_cb(n: int):
                _bootstrap_status[f"{lottery}_count"] = n
            result = bootstrap_la_primitiva_tickets(db, progress_cb=progress_cb)
            _bootstrap_status[lottery] = "done"
            _bootstrap_status[f"{lottery}_result"] = result
            logging.info("[bootstrap/la-primitiva] done: %s", result)
        except Exception as e:
            _bootstrap_status[lottery] = "error"
            _bootstrap_status[f"{lottery}_error"] = str(e)
            logging.exception("[bootstrap/la-primitiva] error: %s", e)

    threading.Thread(target=_run, daemon=True).start()
    return JSONResponse(content={"status": "started", "message": "Bootstrap running in background. Poll /api/la-primitiva/tickets/status."})


@app.get("/api/tickets/bootstrap-status")
def api_tickets_bootstrap_status():
    """Return current bootstrap progress for all lotteries."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    return JSONResponse(content={
        "euromillones": {
            "status":         _bootstrap_status.get("euromillones", "idle"),
            "tickets_so_far": _bootstrap_status.get("euromillones_count", 0),
            "result":         _bootstrap_status.get("euromillones_result"),
            "error":          _bootstrap_status.get("euromillones_error"),
            "db_count":       db[EUROMILLONES_TICKETS_COLLECTION].count_documents({"lottery": "euromillones"}),
        },
        "el_gordo": {
            "status":         _bootstrap_status.get("el_gordo", "idle"),
            "tickets_so_far": _bootstrap_status.get("el_gordo_count", 0),
            "result":         _bootstrap_status.get("el_gordo_result"),
            "error":          _bootstrap_status.get("el_gordo_error"),
            "db_count":       db[EL_GORDO_TICKETS_COLLECTION].count_documents({"lottery": "el_gordo"}),
        },
        "la_primitiva": {
            "status":         _bootstrap_status.get("la_primitiva", "idle"),
            "tickets_so_far": _bootstrap_status.get("la_primitiva_count", 0),
            "result":         _bootstrap_status.get("la_primitiva_result"),
            "error":          _bootstrap_status.get("la_primitiva_error"),
            "db_count":       db[LA_PRIMITIVA_TICKETS_COLLECTION].count_documents({"lottery": "la_primitiva"}),
        },
    })


@app.delete("/api/euromillones/tickets")
def api_euromillones_tickets_reset():
    """Delete all stored Euromillones tickets (forces re-bootstrap on next call)."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    result = db[EUROMILLONES_TICKETS_COLLECTION].delete_many({"lottery": "euromillones"})
    _bootstrap_status.pop("euromillones", None)
    return JSONResponse(content={"deleted": result.deleted_count})


@app.delete("/api/el-gordo/tickets")
def api_el_gordo_tickets_reset():
    """Delete all stored El Gordo tickets (forces re-bootstrap on next call)."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    result = db[EL_GORDO_TICKETS_COLLECTION].delete_many({"lottery": "el_gordo"})
    _bootstrap_status.pop("el_gordo", None)
    return JSONResponse(content={"deleted": result.deleted_count})


@app.delete("/api/la-primitiva/tickets")
def api_la_primitiva_tickets_reset():
    """Delete all stored La Primitiva tickets (forces re-bootstrap on next call)."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    result = db[LA_PRIMITIVA_TICKETS_COLLECTION].delete_many({"lottery": "la_primitiva"})
    _bootstrap_status.pop("la_primitiva", None)
    return JSONResponse(content={"deleted": result.deleted_count})


@app.get("/api/euromillones/compare/analysis")
def api_euromillones_compare_analysis(
    skip: int = Query(0, ge=0, description="Number of rows to skip (for pagination)"),
    limit: int = Query(100, ge=1, le=1000, description="Max rows to return, sorted by date desc"),
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
    total = coll.count_documents({})
    cursor = (
        coll.find(
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
        )
        .sort("date", -1)
        .skip(skip)
        .limit(limit)
    )
    rows = [_euromillones_analysis_row_from_doc(doc) for doc in cursor]
    rows = _annotate_tier1_differences_via_pre_id(
        db,
        EUROMILLONES_COMPARE_RESULTS_COLLECTION,
        rows,
        map_doc_to_row=_euromillones_analysis_row_from_doc,
        tier1_from_row=_tier1_special_position_from_row,
    )
    return JSONResponse(content={"rows": rows, "total": total, "skip": skip, "limit": limit})


@app.get("/api/euromillones/compare/analysis-graph")
def api_euromillones_compare_analysis_graph(
    max_points: int = Query(100, ge=10, le=200, description="Maximum number of points for graph (default 100)"),
):
    """
    Compact analysis for Euromillones full-wheel compares, for graphing from 2004 to now.

    - Reads ALL documents from euromillones_compare_results sorted by date asc.
    - If total <= max_points: return all.
    - If total > max_points: pick approximately one every `step = ceil(total / max_points)` documents,
      so the output has at most `max_points` rows uniformly sampled over time.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    coll = db[EUROMILLONES_COMPARE_RESULTS_COLLECTION]
    total = coll.count_documents({})
    if total == 0:
        return JSONResponse(content={"rows": [], "total": 0})

    # Load all documents in ascending date order so we can group by year.
    docs: List[Dict[str, Any]] = []
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
    ).sort("date", 1)
    for doc in cursor:
        docs.append(doc)

    if not docs:
        return JSONResponse(content={"rows": [], "total": 0})

    # Group docs by year, ensuring we cover from 2004 up to latest year that has data.
    from datetime import datetime as _dt

    by_year: Dict[int, List[Dict[str, Any]]] = {}
    for doc in docs:
        date_str = (doc.get("date") or "")[:10]
        try:
            year = _dt.strptime(date_str, "%Y-%m-%d").year
        except Exception:
            continue
        if year < 2004:
            continue
        by_year.setdefault(year, []).append(doc)

    if not by_year:
        return JSONResponse(content={"rows": [], "total": 0})

    years_sorted = sorted(by_year.keys())
    num_years = len(years_sorted)

    # We want at least 1 point per year, and at most max_points in total.
    max_points = max(max_points, num_years)  # ensure capacity for one per year if needed
    total_in_range = sum(len(by_year[y]) for y in years_sorted)
    target_points = min(max_points, total_in_range)

    # Initial allocation proportional to year sizes, with minimum 1 per year.
    alloc: Dict[int, int] = {}
    remaining = target_points
    for y in years_sorted:
        size = len(by_year[y])
        if total_in_range == 0:
            k = 1
        else:
            k = max(1, int(round(size / total_in_range * target_points)))
        k = min(k, size)  # cannot pick more than we have
        alloc[y] = k
        remaining -= k

    # If we allocated too many (sum > target_points), reduce from years that have >1.
    while remaining < 0:
        for y in reversed(years_sorted):
            if remaining == 0:
                break
            if alloc[y] > 1:
                alloc[y] -= 1
                remaining += 1
        if all(alloc[y] <= 1 for y in years_sorted):
            break

    # If we allocated too few (sum < target_points), add extra points to larger years when possible.
    while remaining > 0:
        for y in years_sorted:
            if remaining == 0:
                break
            if alloc[y] < len(by_year[y]):
                alloc[y] += 1
                remaining -= 1

    # Now sample within each year evenly across its docs.
    out_rows: List[Dict[str, Any]] = []
    for y in years_sorted:
        year_docs = by_year[y]
        k = alloc.get(y, 0)
        if k <= 0:
            continue
        n = len(year_docs)
        if k >= n:
            chosen = year_docs
        else:
            step = n / float(k)
            indices = [int(i * step) for i in range(k)]
            # Ensure indices are within bounds and unique but keep order.
            seen_idx = set()
            chosen = []
            for idx in indices:
                if idx >= n:
                    idx = n - 1
                if idx in seen_idx:
                    continue
                seen_idx.add(idx)
                chosen.append(year_docs[idx])

        for doc in chosen:
            date_str = (doc.get("date") or "")[:10]
            second_positions = doc.get("second_positions") or []
            third_positions = doc.get("third_positions") or []
            fourth_positions = doc.get("fourth_positions") or []
            out_rows.append(
                {
                    "date": date_str,
                    "current_id": str(doc.get("current_id") or ""),
                    "pre_id": str(doc.get("pre_id") or ""),
                    "special_position": int(doc.get("jackpot_position") or 0),
                    "pos_1th": int(doc.get("jackpot_position") or 0),
                    "pos_2th": int(second_positions[0]) if second_positions else None,
                    "pos_3th": int(third_positions[0]) if third_positions else None,
                    "pos_4th": int(fourth_positions[0]) if fourth_positions else None,
                }
            )

    # Ensure we always include the very last date in the dataset.
    last_doc = docs[-1]
    last_date_str = (last_doc.get("date") or "")[:10]
    last_second_positions = last_doc.get("second_positions") or []
    last_third_positions = last_doc.get("third_positions") or []
    last_fourth_positions = last_doc.get("fourth_positions") or []
    already_has_last = any(
        (row.get("date") or "")[:10] == last_date_str
        and str(row.get("current_id") or "") == str(last_doc.get("current_id") or "")
        for row in out_rows
    )
    if not already_has_last:
        out_rows.append(
            {
                "date": last_date_str,
                "current_id": str(last_doc.get("current_id") or ""),
                "pre_id": str(last_doc.get("pre_id") or ""),
                "special_position": int(last_doc.get("jackpot_position") or 0),
                "pos_1th": int(last_doc.get("jackpot_position") or 0),
                "pos_2th": int(last_second_positions[0]) if last_second_positions else None,
                "pos_3th": int(last_third_positions[0]) if last_third_positions else None,
                "pos_4th": int(last_fourth_positions[0]) if last_fourth_positions else None,
            }
        )

    # Ensure final rows are sorted chronologically.
    out_rows.sort(key=lambda r: (r.get("date") or "", r.get("current_id") or ""))
    out_rows = _annotate_tier1_differences_via_pre_id(
        db,
        EUROMILLONES_COMPARE_RESULTS_COLLECTION,
        out_rows,
        map_doc_to_row=_euromillones_analysis_row_from_doc,
        tier1_from_row=_tier1_special_position_from_row,
    )

    return JSONResponse(content={"rows": out_rows, "total": total})


@app.get("/api/el-gordo/compare/analysis")
def api_el_gordo_compare_analysis(
    skip: int = Query(0, ge=0, description="Number of rows to skip (for pagination)"),
    limit: int = Query(100, ge=1, le=1000, description="Max rows to return, sorted by date desc"),
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
    total = coll.count_documents({})
    cursor = (
        coll.find(
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
                "pos_5th": 1,
                "pos_6th": 1,
                "categories": 1,
            },
        )
        .sort("date", -1)
        .skip(skip)
        .limit(limit)
    )
    rows: List[Dict[str, Any]] = []
    for doc in cursor:
        rows.append(_el_gordo_analysis_row_from_compare_doc(doc))
    rows = _annotate_tier1_differences_via_pre_id(
        db,
        EL_GORDO_COMPARE_RESULTS_COLLECTION,
        rows,
        map_doc_to_row=_el_gordo_analysis_row_from_compare_doc,
    )
    return JSONResponse(content={"rows": rows, "total": total, "skip": skip, "limit": limit})


@app.get("/api/el-gordo/compare/analysis-graph")
def api_el_gordo_compare_analysis_graph(
    max_points: int = Query(100, ge=10, le=200, description="Maximum number of points for graph (default 100)"),
):
    """
    Compact analysis for El Gordo full-wheel compares, for graphing from 2004 to now.

    - Reads ALL documents from el_gordo_compare_results sorted by date asc.
    - Ensures each year (>=2004) with data contributes at least one point.
    - Samples within each year so total points <= max_points.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db[EL_GORDO_COMPARE_RESULTS_COLLECTION]
    total = coll.count_documents({})
    if total == 0:
        return JSONResponse(content={"rows": [], "total": 0})

    # Load all docs in ascending date order to group by year.
    docs: List[Dict[str, Any]] = []
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
    ).sort("date", 1)
    for d in cursor:
        docs.append(d)

    if not docs:
        return JSONResponse(content={"rows": [], "total": 0})

    from datetime import datetime as _dt

    by_year: Dict[int, List[Dict[str, Any]]] = {}
    for doc in docs:
        date_str = (doc.get("date") or "")[:10]
        try:
            year = _dt.strptime(date_str, "%Y-%m-%d").year
        except Exception:
            continue
        if year < 2004:
            continue
        by_year.setdefault(year, []).append(doc)

    if not by_year:
        return JSONResponse(content={"rows": [], "total": 0})

    years_sorted = sorted(by_year.keys())
    num_years = len(years_sorted)

    max_points = max(max_points, num_years)
    total_in_range = sum(len(by_year[y]) for y in years_sorted)
    target_points = min(max_points, total_in_range)

    alloc: Dict[int, int] = {}
    remaining = target_points
    for y in years_sorted:
        size = len(by_year[y])
        if total_in_range == 0:
            k = 1
        else:
            k = max(1, int(round(size / total_in_range * target_points)))
        k = min(k, size)
        alloc[y] = k
        remaining -= k

    while remaining < 0:
        for y in reversed(years_sorted):
            if remaining == 0:
                break
            if alloc[y] > 1:
                alloc[y] -= 1
                remaining += 1
        if all(alloc[y] <= 1 for y in years_sorted):
            break

    while remaining > 0:
        for y in years_sorted:
            if remaining == 0:
                break
            if alloc[y] < len(by_year[y]):
                alloc[y] += 1
                remaining -= 1

    out_rows: List[Dict[str, Any]] = []
    for y in years_sorted:
        year_docs = by_year[y]
        k = alloc.get(y, 0)
        if k <= 0:
            continue
        n = len(year_docs)
        if k >= n:
            chosen = year_docs
        else:
            step = n / float(k)
            indices = [int(i * step) for i in range(k)]
            seen_idx = set()
            chosen = []
            for idx in indices:
                if idx >= n:
                    idx = n - 1
                if idx in seen_idx:
                    continue
                seen_idx.add(idx)
                chosen.append(year_docs[idx])

        for doc in chosen:
            out_rows.append(_el_gordo_analysis_row_from_compare_doc(doc))

    last_doc = docs[-1]
    last_row = _el_gordo_analysis_row_from_compare_doc(last_doc)
    already_has_last = any(
        (row.get("date") or "")[:10] == (last_row.get("date") or "")[:10]
        and str(row.get("current_id") or "") == str(last_row.get("current_id") or "")
        for row in out_rows
    )
    if not already_has_last:
        out_rows.append(last_row)

    out_rows.sort(key=lambda r: (r.get("date") or "", r.get("current_id") or ""))
    out_rows = _annotate_tier1_differences_via_pre_id(
        db,
        EL_GORDO_COMPARE_RESULTS_COLLECTION,
        out_rows,
        map_doc_to_row=_el_gordo_analysis_row_from_compare_doc,
    )

    return JSONResponse(content={"rows": out_rows, "total": total})


# Canonical (main_hits, reintegro_hit) keys for La Primitiva analysis: 1ª=6-0, 2ª=5+C, 3ª=5-0, 4ª=4-0, 5ª=3-0
_LA_PRIMITIVA_ANALYSIS_KEYS = [(6, 0), (5, 1), (5, 0), (4, 0), (3, 0)]


@app.get("/api/la-primitiva/compare/analysis")
def api_la_primitiva_compare_analysis(
    skip: int = Query(0, ge=0, description="Number of rows to skip (for pagination)"),
    limit: int = Query(100, ge=1, le=1000, description="Max rows to return, sorted by date desc"),
):
    """
    Analysis of La Primitiva full-wheel compares.

    Reads la_primitiva_compare_results and returns rows sorted by date desc.
    All five positions (pos_1th–pos_5th) are taken from the categories array by matching
    (main_hits, reintegro_hit): 1ª=6-0, 2ª=5+C, 3ª=5-0, 4ª=4-0, 5ª=3-0.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    coll = db[LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION]
    total = coll.count_documents({})
    cursor = (
        coll.find(
            {},
            projection={
                "_id": 0,
                "date": 1,
                "current_id": 1,
                "pre_id": 1,
                "jackpot_position": 1,
                "special_position": 1,
                "categories": 1,
            },
        )
        .sort("date", -1)
        .skip(skip)
        .limit(limit)
    )
    rows = [_la_primitiva_analysis_row_from_doc(doc) for doc in cursor]
    rows = _annotate_tier1_differences_via_pre_id(
        db,
        LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION,
        rows,
        map_doc_to_row=_la_primitiva_analysis_row_from_doc,
        tier1_from_row=_tier1_special_position_from_row,
    )
    return JSONResponse(content={"rows": rows, "total": total, "skip": skip, "limit": limit})


@app.get("/api/la-primitiva/compare/analysis-graph")
def api_la_primitiva_compare_analysis_graph(
    max_points: int = Query(100, ge=10, le=200, description="Maximum number of points for graph (default 100)"),
):
    """
    Compact analysis for La Primitiva full-wheel compares, for graphing from 2004 to now.

    - Reads ALL documents from la_primitiva_compare_results sorted by date asc.
    - Ensures each year (>=2004) with data contributes at least one point.
    - Samples within each year so total points <= max_points.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db[LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION]
    total = coll.count_documents({})
    if total == 0:
        return JSONResponse(content={"rows": [], "total": 0})

    # Load all docs in ascending date order to group by year.
    docs: List[Dict[str, Any]] = []
    cursor = coll.find(
        {},
        projection={
            "_id": 0,
            "date": 1,
            "current_id": 1,
            "pre_id": 1,
            "jackpot_position": 1,
            "special_position": 1,
            "categories": 1,
        },
    ).sort("date", 1)
    for d in cursor:
        docs.append(d)

    if not docs:
        return JSONResponse(content={"rows": [], "total": 0})

    from datetime import datetime as _dt

    by_year: Dict[int, List[Dict[str, Any]]] = {}
    for doc in docs:
        date_str = (doc.get("date") or "")[:10]
        try:
            year = _dt.strptime(date_str, "%Y-%m-%d").year
        except Exception:
            continue
        if year < 2004:
            continue
        by_year.setdefault(year, []).append(doc)

    if not by_year:
        return JSONResponse(content={"rows": [], "total": 0})

    years_sorted = sorted(by_year.keys())
    num_years = len(years_sorted)

    max_points = max(max_points, num_years)
    total_in_range = sum(len(by_year[y]) for y in years_sorted)
    target_points = min(max_points, total_in_range)

    alloc: Dict[int, int] = {}
    remaining = target_points
    for y in years_sorted:
        size = len(by_year[y])
        if total_in_range == 0:
            k = 1
        else:
            k = max(1, int(round(size / total_in_range * target_points)))
        k = min(k, size)
        alloc[y] = k
        remaining -= k

    while remaining < 0:
        for y in reversed(years_sorted):
            if remaining == 0:
                break
            if alloc[y] > 1:
                alloc[y] -= 1
                remaining += 1
        if all(alloc[y] <= 1 for y in years_sorted):
            break

    while remaining > 0:
        for y in years_sorted:
            if remaining == 0:
                break
            if alloc[y] < len(by_year[y]):
                alloc[y] += 1
                remaining -= 1

    def _extract_positions(doc: Dict[str, Any]) -> Dict[str, Any]:
        date_str = (doc.get("date") or "")[:10]
        cats = doc.get("categories") or []
        cat_by_key: Dict[Tuple[int, int], int] = {}
        if isinstance(cats, list):
            for c in cats:
                if not isinstance(c, dict):
                    continue
                hm = int(c.get("main_hits") or 0)
                rh = int(c.get("reintegro_hit") or 0)
                fp = int(c.get("first_position") or 0)
                cat_by_key[(hm, rh)] = fp
        positions = [cat_by_key.get(key) for key in _LA_PRIMITIVA_ANALYSIS_KEYS]
        special_pos = int(doc.get("special_position") or 0) or None
        return {
            "date": date_str,
            "current_id": str(doc.get("current_id") or ""),
            "pre_id": str(doc.get("pre_id") or ""),
            "special_position": special_pos,
            "pos_1th": positions[0] if positions[0] is not None and positions[0] > 0 else 0,
            "pos_2th": positions[1] if positions[1] and positions[1] > 0 else None,
            "pos_3th": positions[2] if positions[2] and positions[2] > 0 else None,
            "pos_4th": positions[3] if positions[3] and positions[3] > 0 else None,
            "pos_5th": positions[4] if positions[4] and positions[4] > 0 else None,
        }

    out_rows: List[Dict[str, Any]] = []
    for y in years_sorted:
        year_docs = by_year[y]
        k = alloc.get(y, 0)
        if k <= 0:
            continue
        n = len(year_docs)
        if k >= n:
            chosen = year_docs
        else:
            step = n / float(k)
            indices = [int(i * step) for i in range(k)]
            seen_idx = set()
            chosen = []
            for idx in indices:
                if idx >= n:
                    idx = n - 1
                if idx in seen_idx:
                    continue
                seen_idx.add(idx)
                chosen.append(year_docs[idx])

        for doc in chosen:
            out_rows.append(_extract_positions(doc))

    # Always include the very last date in the dataset.
    last_doc = docs[-1]
    last_row = _extract_positions(last_doc)
    already_has_last = any(
        (row.get("date") or "")[:10] == (last_row.get("date") or "")[:10]
        and str(row.get("current_id") or "") == str(last_row.get("current_id") or "")
        for row in out_rows
    )
    if not already_has_last:
        out_rows.append(last_row)

    out_rows.sort(key=lambda r: (r.get("date") or "", r.get("current_id") or ""))
    out_rows = _annotate_tier1_differences_via_pre_id(
        db,
        LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION,
        out_rows,
        map_doc_to_row=_la_primitiva_analysis_row_from_doc,
        tier1_from_row=_tier1_special_position_from_row,
    )

    return JSONResponse(content={"rows": out_rows, "total": total})


@app.get("/api/euromillones/compare/full-wheel/tickets")
def api_euromillones_compare_full_wheel_tickets(
    current_id: str = Query(..., description="id_sorteo of the draw (result)."),
    pre_id: str = Query(..., description="cutoff_draw_id or probs_draw_id for the full wheel run."),
    skip: int = Query(0, ge=0, description="Number of tickets to skip from the start (0‑based)."),
    limit: int = Query(100, ge=1, le=100, description="Page size (max 100)."),
):
   
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
                p = _fw_line_position(line)
                if p is not None and p > last_pos:
                    last_pos = p
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
            sp = _fw_split_euromillones_line(line)
            if not sp:
                continue
            position, mains_str, stars_str = sp
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
            parts = line.split(";")
            tid = parts[0] if len(parts) >= 4 and "_COMBO_" in (parts[0] or "") else None
            row: Dict[str, Any] = {
                "position": position,
                "mains": mains,
                "stars": stars,
                "first_main": mains[0],
                "category": label_for(hits_main, hits_star),
                "main_hits": hits_main,
                "star_hits": hits_star,
            }
            if tid:
                row["ticket_id"] = tid
            items.append(row)
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

    main_draw, clave_draw = _parse_el_gordo_mains_clave(doc)
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
            p = _fw_line_position(line)
            if p is not None and p > last_pos:
                last_pos = p
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
            sp = _fw_split_el_gordo_line(line)
            if not sp:
                continue
            position, mains_str, clave_str = sp
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
            parts = line.split(";")
            tid = parts[0] if len(parts) >= 4 and "_COMBO_" in (parts[0] or "") else None
            row: Dict[str, Any] = {
                "position": position,
                "mains": mains,
                "clave": clave,
                "first_main": mains[0],
                "category": label_for_el_gordo(hits_main, hits_clave),
                "main_hits": hits_main,
                "clave_hit": hits_clave,
            }
            if tid:
                row["ticket_id"] = tid
            items.append(row)
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
            p = _fw_line_position(line)
            if p is not None and p > last_pos:
                last_pos = p
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
            sp = _fw_split_la_primitiva_line(line)
            if not sp:
                continue
            position, mains_str, rein_str = sp
            if position < start_pos:
                continue
            if position > end_pos:
                break
            mains = [int(x) for x in mains_str.split(",") if x]
            if len(mains) != 6:
                continue
            parts = line.split(";")
            tid = parts[0] if len(parts) >= 3 and "_COMBO_" in (parts[0] or "") else None
            row: Dict[str, Any] = {"position": position, "mains": mains}
            if tid:
                row["ticket_id"] = tid
            if rein_str is not None and str(rein_str).strip() != "":
                try:
                    row["reintegro"] = int(str(rein_str).strip())
                except Exception:
                    pass
            items.append(row)
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


@app.get("/api/euromillones/full-wheel/export")
def api_euromillones_full_wheel_export(
    cutoff_draw_id: str | None = Query(None, description="cutoff_draw_id that generated the full wheel file (optional)."),
    fmt: str = Query("txt", description="txt or csv"),
):
    """
    Export Euromillones full wheel:
    - fmt=txt: download the full TXT file already generated.
    - fmt=csv: generate a full CSV file on server (if needed) then download it.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    coll = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
    cid = (cutoff_draw_id or "").strip()
    doc = None
    if cid:
        doc = coll.find_one({"cutoff_draw_id": cid}) or (coll.find_one({"cutoff_draw_id": int(cid)}) if cid.isdigit() else None)
    if doc is None:
        doc = _resolve_latest_train_progress_doc("euromillones")
    if not doc:
        raise HTTPException(404, detail="No train_progress found for euromillones (last_draw_date)")
    path = (doc.get("full_wheel_file_path") or "").strip()
    total = int(doc.get("full_wheel_total_tickets") or 0)
    if not path or not os.path.isfile(path):
        raise HTTPException(404, detail="Full wheel file not found on disk")

    fmt2 = (fmt or "txt").strip().lower()

    if fmt2 not in ("txt", "csv"):
        raise HTTPException(400, detail="fmt must be txt or csv")

    if fmt2 == "txt":
        base = os.path.basename(path)
        return FileResponse(path, media_type="text/plain", filename=base)

    csv_path = _fw_generate_full_csv_if_needed(path, "euromillones")
    base = os.path.basename(csv_path)
    return FileResponse(csv_path, media_type="text/csv", filename=base)


@app.get("/api/el-gordo/full-wheel/export")
def api_el_gordo_full_wheel_export(
    cutoff_draw_id: str | None = Query(None, description="cutoff_draw_id that generated the full wheel file (optional)."),
    fmt: str = Query("txt", description="txt or csv"),
):
    """Export El Gordo full wheel: txt downloads existing file; csv generated server-side if needed."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    coll = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
    cid = (cutoff_draw_id or "").strip()
    doc = None
    if cid:
        doc = coll.find_one({"cutoff_draw_id": cid}) or (coll.find_one({"cutoff_draw_id": int(cid)}) if cid.isdigit() else None)
    if doc is None:
        doc = _resolve_latest_train_progress_doc("el-gordo")
    if not doc:
        raise HTTPException(404, detail="No train_progress found for el-gordo (last_draw_date)")
    path = (doc.get("full_wheel_file_path") or "").strip()
    total = int(doc.get("full_wheel_total_tickets") or 0)
    if not path or not os.path.isfile(path):
        raise HTTPException(404, detail="Full wheel file not found on disk")

    fmt2 = (fmt or "txt").strip().lower()

    if fmt2 not in ("txt", "csv"):
        raise HTTPException(400, detail="fmt must be txt or csv")

    if fmt2 == "txt":
        base = os.path.basename(path)
        return FileResponse(path, media_type="text/plain", filename=base)

    csv_path = _fw_generate_full_csv_if_needed(path, "el-gordo")
    base = os.path.basename(csv_path)
    return FileResponse(csv_path, media_type="text/csv", filename=base)


@app.get("/api/la-primitiva/full-wheel/export")
def api_la_primitiva_full_wheel_export(
    cutoff_draw_id: str | None = Query(None, description="cutoff_draw_id that generated the full wheel file (optional)."),
    fmt: str = Query("txt", description="txt or csv"),
):
    """Export La Primitiva full wheel: txt downloads existing file; csv generated server-side if needed."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    coll = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
    cid = (cutoff_draw_id or "").strip()
    doc = None
    if cid:
        doc = coll.find_one({"cutoff_draw_id": cid}) or (coll.find_one({"cutoff_draw_id": int(cid)}) if cid.isdigit() else None)
    if doc is None:
        doc = _resolve_latest_train_progress_doc("la-primitiva")
    if not doc:
        raise HTTPException(404, detail="No train_progress found for la-primitiva (last_draw_date)")
    path = (doc.get("full_wheel_file_path") or "").strip()
    total = int(doc.get("full_wheel_total_tickets") or 0)
    if not path or not os.path.isfile(path):
        raise HTTPException(404, detail="Full wheel file not found on disk")

    fmt2 = (fmt or "txt").strip().lower()

    if fmt2 not in ("txt", "csv"):
        raise HTTPException(400, detail="fmt must be txt or csv")

    if fmt2 == "txt":
        base = os.path.basename(path)
        return FileResponse(path, media_type="text/plain", filename=base)

    csv_path = _fw_generate_full_csv_if_needed(path, "la-primitiva")
    base = os.path.basename(csv_path)
    return FileResponse(csv_path, media_type="text/csv", filename=base)


def _prepare_csv_job(lottery_slug: str, cutoff_draw_id: str | None) -> dict:
    """Create or reuse a CSV preparation job for the current TXT."""
    txt_path = _resolve_full_wheel_txt_path_for_export(lottery_slug, cutoff_draw_id)
    csv_path = _fw_csv_path_for_full_wheel(txt_path)
    try:
        if os.path.isfile(csv_path) and os.path.getmtime(csv_path) >= os.path.getmtime(txt_path):
            return {"status": "done", "progress": 100, "job_id": None}
    except OSError:
        pass

    sig = f"{lottery_slug}:{txt_path}:{os.path.getmtime(txt_path) if os.path.isfile(txt_path) else 0}"
    with _csv_prepare_jobs_lock:
        # Reuse existing running job with same signature
        for jid, job in _csv_prepare_jobs.items():
            if job.get("signature") == sig and job.get("status") in ("queued", "running"):
                return {"status": job.get("status"), "progress": int(job.get("progress") or 0), "job_id": jid}

        job_id = secrets.token_urlsafe(12)
        _csv_prepare_jobs[job_id] = {
            "signature": sig,
            "status": "queued",
            "progress": 0,
            "error": "",
            "cancel_requested": False,
            "txt_path": txt_path,
            "csv_path": "",
            "lottery": lottery_slug,
            "started_at": time.time(),
            "updated_at": time.time(),
        }

    t = threading.Thread(target=_csv_prepare_job_worker, args=(job_id,), daemon=True)
    t.start()
    return {"status": "queued", "progress": 0, "job_id": job_id}


@app.post("/api/euromillones/full-wheel/prepare-csv")
def api_euromillones_prepare_csv(
    cutoff_draw_id: str | None = Query(None, description="Optional cutoff_draw_id to prepare CSV for."),
):
    """Public: prepare CSV on server and report job_id for progress polling."""
    out = _prepare_csv_job("euromillones", cutoff_draw_id)
    return JSONResponse(content=out)


@app.post("/api/el-gordo/full-wheel/prepare-csv")
def api_el_gordo_prepare_csv(
    cutoff_draw_id: str | None = Query(None, description="Optional cutoff_draw_id to prepare CSV for."),
):
    """Public: prepare CSV on server and report job_id for progress polling."""
    out = _prepare_csv_job("el-gordo", cutoff_draw_id)
    return JSONResponse(content=out)


@app.post("/api/la-primitiva/full-wheel/prepare-csv")
def api_la_primitiva_prepare_csv(
    cutoff_draw_id: str | None = Query(None, description="Optional cutoff_draw_id to prepare CSV for."),
):
    """Public: prepare CSV on server and report job_id for progress polling."""
    out = _prepare_csv_job("la-primitiva", cutoff_draw_id)
    return JSONResponse(content=out)


@app.get("/api/euromillones/full-wheel/prepare-csv/status")
def api_euromillones_prepare_csv_status(job_id: str = Query(..., description="Job id from prepare-csv.")):
    with _csv_prepare_jobs_lock:
        job = dict(_csv_prepare_jobs.get(job_id) or {})
    if not job:
        raise HTTPException(404, detail="Job not found")
    return JSONResponse(
        content={
            "status": job.get("status"),
            "progress": int(job.get("progress") or 0),
            "error": job.get("error") or "",
        }
    )


@app.get("/api/el-gordo/full-wheel/prepare-csv/status")
def api_el_gordo_prepare_csv_status(job_id: str = Query(..., description="Job id from prepare-csv.")):
    with _csv_prepare_jobs_lock:
        job = dict(_csv_prepare_jobs.get(job_id) or {})
    if not job:
        raise HTTPException(404, detail="Job not found")
    return JSONResponse(
        content={
            "status": job.get("status"),
            "progress": int(job.get("progress") or 0),
            "error": job.get("error") or "",
        }
    )


@app.get("/api/la-primitiva/full-wheel/prepare-csv/status")
def api_la_primitiva_prepare_csv_status(job_id: str = Query(..., description="Job id from prepare-csv.")):
    with _csv_prepare_jobs_lock:
        job = dict(_csv_prepare_jobs.get(job_id) or {})
    if not job:
        raise HTTPException(404, detail="Job not found")
    return JSONResponse(
        content={
            "status": job.get("status"),
            "progress": int(job.get("progress") or 0),
            "error": job.get("error") or "",
        }
    )


def _cancel_csv_job(job_id: str) -> dict:
    with _csv_prepare_jobs_lock:
        job = _csv_prepare_jobs.get(job_id)
        if not job:
            raise HTTPException(404, detail="Job not found")
        st = job.get("status")
        if st in ("done", "error", "cancelled"):
            return {"status": st, "progress": int(job.get("progress") or 0)}
        job["cancel_requested"] = True
        job["updated_at"] = time.time()
        return {"status": "cancelling", "progress": int(job.get("progress") or 0)}


@app.post("/api/euromillones/full-wheel/prepare-csv/cancel")
def api_euromillones_prepare_csv_cancel(job_id: str = Query(..., description="Job id to cancel.")):
    """Public: request cancellation of a CSV preparation job."""
    return JSONResponse(content=_cancel_csv_job(job_id))


@app.post("/api/el-gordo/full-wheel/prepare-csv/cancel")
def api_el_gordo_prepare_csv_cancel(job_id: str = Query(..., description="Job id to cancel.")):
    """Public: request cancellation of a CSV preparation job."""
    return JSONResponse(content=_cancel_csv_job(job_id))


@app.post("/api/la-primitiva/full-wheel/prepare-csv/cancel")
def api_la_primitiva_prepare_csv_cancel(job_id: str = Query(..., description="Job id to cancel.")):
    """Public: request cancellation of a CSV preparation job."""
    return JSONResponse(content=_cancel_csv_job(job_id))


@app.post("/api/train/reset")
def api_train_reset(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
    cutoff_draw_id: str | None = Query(None, description="cutoff_draw_id to delete from train_progress (optional; defaults to last_draw_date run)"),
    delete_files: bool = Query(True, description="Delete generated full-wheel TXT/CSV files on disk when possible."),
    delete_compare: bool = Query(True, description="Delete cached compare results for this pre_id (cutoff_draw_id)."),
):
    """
    Reset training state so the user can retrain/regenerate.

    This deletes the corresponding *_train_progress document for cutoff_draw_id.
    Optionally deletes generated full-wheel files and cached compare results rows that use pre_id.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    lot = (lottery or "").strip().lower()
    cid = (cutoff_draw_id or "").strip()
    if lot not in ("euromillones", "el-gordo", "la-primitiva"):
        raise HTTPException(400, detail="lottery must be one of: euromillones, el-gordo, la-primitiva")

    progress_coll_name = (
        EUROMILLONES_TRAIN_PROGRESS_COLLECTION
        if lot == "euromillones"
        else EL_GORDO_TRAIN_PROGRESS_COLLECTION
        if lot == "el-gordo"
        else LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION
    )
    compare_coll_name = (
        EUROMILLONES_COMPARE_RESULTS_COLLECTION
        if lot == "euromillones"
        else EL_GORDO_COMPARE_RESULTS_COLLECTION
        if lot == "el-gordo"
        else LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION
    )

    coll_progress = db[progress_coll_name]
    doc = None
    if cid:
        doc = coll_progress.find_one({"cutoff_draw_id": cid}) or (
            coll_progress.find_one({"cutoff_draw_id": int(cid)}) if cid.isdigit() else None
        )
    if doc is None:
        doc = _resolve_latest_train_progress_doc(lot)
        cid = str((doc or {}).get("cutoff_draw_id") or "").strip() or cid
    if not doc:
        return JSONResponse(content={"status": "ok", "deleted": 0, "message": "No train_progress document found for this lottery/last_draw_date."})

    deleted_files: List[str] = []
    file_errors: List[str] = []
    if delete_files:
        txt_path = (doc.get("full_wheel_file_path") or "").strip()
        paths = []
        if txt_path:
            paths.append(txt_path)
            paths.append(_fw_csv_path_for_full_wheel(txt_path))
        for p in paths:
            try:
                if p and os.path.isfile(p):
                    os.unlink(p)
                    deleted_files.append(p)
            except Exception as e:
                file_errors.append(f"{p}: {e}")

    deleted_compare = 0
    if delete_compare:
        try:
            deleted_compare = int(db[compare_coll_name].delete_many({"pre_id": cid}).deleted_count)
        except Exception:
            deleted_compare = 0

    res = coll_progress.delete_one({"_id": doc.get("_id")})
    return JSONResponse(
        content={
            "status": "ok",
            "lottery": lot,
            "cutoff_draw_id": cid,
            "deleted": int(res.deleted_count or 0),
            "deleted_compare": deleted_compare,
            "deleted_files": deleted_files,
            "file_errors": file_errors[:10],
        }
    )


@app.get("/api/train/latest")
def api_train_latest(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
):
    """Return latest train_progress for scraper_metadata.last_draw_date (if any)."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    lot = (lottery or "").strip().lower()
    if lot not in ("euromillones", "el-gordo", "la-primitiva"):
        raise HTTPException(400, detail="lottery must be one of: euromillones, el-gordo, la-primitiva")
    last = (_get_last_draw_date(lot) or "").strip()[:10] or None
    doc = _resolve_latest_train_progress_doc(lot)
    if not doc:
        return JSONResponse(content={"lottery": lot, "last_draw_date": last, "exists": False, "progress": None})
    progress = {
        "cutoff_draw_id": str(doc.get("cutoff_draw_id") or ""),
        "probs_fecha_sorteo": (doc.get("probs_fecha_sorteo") or ""),
        "pipeline_status": doc.get("pipeline_status"),
        "rules_applied": doc.get("rules_applied"),
        "full_wheel_status": doc.get("full_wheel_status"),
        "full_wheel_file_path": doc.get("full_wheel_file_path"),
        "full_wheel_total_tickets": doc.get("full_wheel_total_tickets"),
        "full_wheel_draw_date": doc.get("full_wheel_draw_date"),
    }
    return JSONResponse(content={"lottery": lot, "last_draw_date": last, "exists": True, "progress": progress})

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
        item_e: Dict[str, Any] = {"mains": [int(m) for m in mains], "stars": [int(s) for s in stars]}
        pos_e = _optional_wheel_position(t)
        if pos_e is not None:
            item_e["position"] = pos_e
        normalized.append(item_e)
    draw_date = (body.get("draw_date") or "").strip()[:10] or None
    cutoff_draw_id = (body.get("cutoff_draw_id") or "").strip() or None
    if db is None:
        raise HTTPException(503, detail="Database not available")
    if not draw_date:
        coll_progress = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
        doc_progress = None
        if cutoff_draw_id:
            doc_progress = coll_progress.find_one({"cutoff_draw_id": cutoff_draw_id})
            if doc_progress is None and cutoff_draw_id.isdigit():
                doc_progress = coll_progress.find_one({"cutoff_draw_id": int(cutoff_draw_id)})
        if doc_progress is not None:
            draw_date = (doc_progress.get("probs_fecha_sorteo") or "").strip()[:10] or None
        if not draw_date:
            draw_date = (_get_last_draw_date("euromillones") or "").strip()[:10] or None
    if not draw_date:
        raise HTTPException(400, detail="No draw_date resolved; provide draw_date or cutoff_draw_id")
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


EUROMILLONES_BUCKET_SIZE = 5


@app.post("/api/euromillones/betting/enqueue-by-count")
async def api_euromillones_betting_enqueue_by_count(request: Request):
    """
    Create buy-queue items by ticket count. Body: { "count": int, "draw_date"?, "cutoff_draw_id"? }.
    Reads from full_wheel file, splits into buckets of 5 (one queue item per bucket).
    Capped by LOTTERY_MAX_ENQUEUE_BY_COUNT (default 100000). Inserts each bucket immediately to limit memory.
    """
    try:
        body = await request.json() or {}
    except Exception:
        body = {}
    count = body.get("count")
    if not isinstance(count, int) or count < 1:
        raise HTTPException(400, detail="Body must contain 'count' (positive integer)")
    max_n = _lottery_max_enqueue_by_count()
    if count > max_n:
        raise HTTPException(
            400,
            detail=f"count must be <= {max_n} per request (raise LOTTERY_MAX_ENQUEUE_BY_COUNT or split into multiple requests)",
        )
    draw_date = (body.get("draw_date") or "").strip()[:10] or None
    cutoff_draw_id = (body.get("cutoff_draw_id") or "").strip() or None
    if db is None:
        raise HTTPException(503, detail="Database not available")
    coll_progress = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
    coll_queue = db[EUROMILLONES_BUY_QUEUE_COLLECTION]
    date_str = draw_date
    cutoff = cutoff_draw_id
    if not date_str and not cutoff:
        last = (_get_last_draw_date("euromillones") or "").strip()
        if last:
            date_str = last[:10]
        if not date_str:
            raise HTTPException(400, detail="No last_draw_date for euromillones; provide draw_date or cutoff_draw_id")
    doc = None
    if cutoff:
        doc = coll_progress.find_one({"cutoff_draw_id": cutoff})
        if doc is None and cutoff.isdigit():
            doc = coll_progress.find_one({"cutoff_draw_id": int(cutoff)})
    if doc is None and date_str:
        doc = coll_progress.find_one({"full_wheel_draw_date": date_str}) or coll_progress.find_one({"probs_fecha_sorteo": date_str})
    if doc is None:
        raise HTTPException(404, detail="No train progress found for draw_date or cutoff_draw_id")
    path = (doc.get("full_wheel_file_path") or "").strip()
    total_in_file = int(doc.get("full_wheel_total_tickets") or 0)
    if not path or total_in_file <= 0:
        raise HTTPException(404, detail="Full wheel file not found or empty for this draw")
    date_str = (doc.get("full_wheel_draw_date") or doc.get("probs_fecha_sorteo") or date_str or "").strip()[:10] or None
    cutoff = str(doc.get("cutoff_draw_id") or "") or cutoff
    excluded: set = set()
    for d in coll_queue.find({"status": {"$in": ["waiting", "in_progress"]}}, projection={"tickets": 1}):
        d_date = (d.get("draw_date") or "").strip()
        d_cutoff = (d.get("cutoff_draw_id") or "").strip()
        if d_date != date_str and d_cutoff != cutoff:
            continue
        for t in (d.get("tickets") or []):
            mains = t.get("mains")
            stars = t.get("stars")
            if isinstance(mains, list) and len(mains) == 5 and isinstance(stars, list) and len(stars) == 2:
                excluded.add((tuple(sorted(mains)), tuple(sorted(stars))))
    for t in (doc.get("bought_tickets") or []):
        mains = t.get("mains")
        stars = t.get("stars")
        if isinstance(mains, list) and len(mains) == 5 and isinstance(stars, list) and len(stars) == 2:
            excluded.add((tuple(sorted(mains)), tuple(sorted(stars))))
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    inserted_ids: List[str] = []
    bucket_buf: List[Dict] = []
    total_collected = 0
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if total_collected >= count:
                    break
                line = line.strip()
                if not line:
                    continue
                sp = _fw_split_euromillones_line(line)
                if not sp:
                    continue
                position, mains_str, stars_str = sp
                try:
                    mains = [int(x) for x in mains_str.split(",") if x]
                    stars = [int(x) for x in stars_str.split(",") if x]
                except Exception:
                    continue
                if len(mains) != 5 or len(stars) != 2:
                    continue
                key = (tuple(sorted(mains)), tuple(sorted(stars)))
                if key in excluded:
                    continue
                excluded.add(key)
                bucket_buf.append({"mains": mains, "stars": stars, "position": position})
                total_collected += 1
                if len(bucket_buf) >= EUROMILLONES_BUCKET_SIZE:
                    chunk = bucket_buf[:EUROMILLONES_BUCKET_SIZE]
                    del bucket_buf[:EUROMILLONES_BUCKET_SIZE]
                    doc_queue = {
                        "lottery": "euromillones",
                        "tickets": chunk,
                        "tickets_count": len(chunk),
                        "draw_date": date_str,
                        "cutoff_draw_id": cutoff,
                        "status": "waiting",
                        "created_at": now,
                    }
                    ins = coll_queue.insert_one(doc_queue)
                    inserted_ids.append(str(ins.inserted_id))
                if total_collected >= count:
                    break
    except FileNotFoundError:
        raise HTTPException(404, detail="Full wheel file not found on disk")
    if bucket_buf:
        chunk = bucket_buf
        doc_queue = {
            "lottery": "euromillones",
            "tickets": chunk,
            "tickets_count": len(chunk),
            "draw_date": date_str,
            "cutoff_draw_id": cutoff,
            "status": "waiting",
            "created_at": now,
        }
        ins = coll_queue.insert_one(doc_queue)
        inserted_ids.append(str(ins.inserted_id))
    if total_collected == 0:
        raise HTTPException(400, detail="No tickets available (all already in queue or bought)")
    return JSONResponse(
        content={
            "status": "ok",
            "queued": len(inserted_ids),
            "total_tickets": total_collected,
            "message": f"{len(inserted_ids)} colas creadas ({total_collected} boletos).",
        },
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.post("/api/euromillones/betting/enqueue-by-range")
async def api_euromillones_betting_enqueue_by_range(request: Request):
    """
    Create buy-queue items by position range from the full-wheeling pool file.

    Body: { "start_position": int, "end_position": int, "draw_date"?, "cutoff_draw_id"?, "exclude_positions"?: int[] }
      - Positions are 1-based and read from the TXT file's "position;..." prefix.
      - Range is inclusive on both ends: positions start_position..end_position.
      - Constraints: start >= 1, end >= start, end <= total_in_file.
      - Optional exclude_positions: wheel line indices to skip (e.g. cesta/cola/guardados from UI).
      - Tickets already in queue (waiting/in_progress/bought) or bought are skipped by combo.
    """
    try:
        body = await request.json() or {}
    except Exception:
        body = {}

    start_position = body.get("start_position")
    end_position = body.get("end_position")
    if not isinstance(start_position, int) or not isinstance(end_position, int):
        raise HTTPException(400, detail="Body must contain 'start_position' and 'end_position' (integers)")
    if start_position < 1:
        raise HTTPException(400, detail="start_position must be >= 1")
    if end_position < start_position:
        raise HTTPException(400, detail="end_position must be >= start_position")

    draw_date = (body.get("draw_date") or "").strip()[:10] or None
    cutoff_draw_id = (body.get("cutoff_draw_id") or "").strip() or None

    if db is None:
        raise HTTPException(503, detail="Database not available")

    coll_progress = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
    coll_queue = db[EUROMILLONES_BUY_QUEUE_COLLECTION]

    date_str = draw_date
    cutoff = cutoff_draw_id
    if not date_str and not cutoff:
        last = (_get_last_draw_date("euromillones") or "").strip()
        if last:
            date_str = last[:10]
        if not date_str:
            raise HTTPException(400, detail="No last_draw_date for euromillones; provide draw_date or cutoff_draw_id")

    doc = None
    if cutoff:
        doc = coll_progress.find_one({"cutoff_draw_id": cutoff})
        if doc is None and cutoff.isdigit():
            doc = coll_progress.find_one({"cutoff_draw_id": int(cutoff)})
    if doc is None and date_str:
        doc = coll_progress.find_one({"full_wheel_draw_date": date_str}) or coll_progress.find_one({"probs_fecha_sorteo": date_str})
    if doc is None:
        raise HTTPException(404, detail="No train progress found for draw_date or cutoff_draw_id")

    path = (doc.get("full_wheel_file_path") or "").strip()
    total_in_file = int(doc.get("full_wheel_total_tickets") or 0)
    if not path or total_in_file <= 0:
        raise HTTPException(404, detail="Full wheel file not found or empty for this draw")

    if start_position > total_in_file:
        raise HTTPException(400, detail=f"start_position must be <= total tickets ({total_in_file})")
    if end_position > total_in_file:
        raise HTTPException(400, detail=f"end_position must be <= total tickets ({total_in_file})")

    exclude_pos = _enqueue_range_exclude_positions_from_body(body)

    # Exclusions: avoid queuing duplicates already in queue (incl. bought rows) for this draw/cutoff.
    excluded: set[tuple[tuple[int, ...], tuple[int, ...]]] = set()
    for d in coll_queue.find({"status": {"$in": ["waiting", "in_progress", "bought"]}}, projection={"tickets": 1}):
        d_date = (d.get("draw_date") or "").strip()
        d_cutoff = (d.get("cutoff_draw_id") or "").strip()
        # Keep only items for the inferred draw_date/cutoff context.
        # (Same logic style as enqueue-by-count.)
        if d_date != date_str and d_cutoff != cutoff:
            continue
        for t in (d.get("tickets") or []):
            mains = t.get("mains")
            stars = t.get("stars")
            if isinstance(mains, list) and len(mains) == 5 and isinstance(stars, list) and len(stars) == 2:
                excluded.add((tuple(sorted(mains)), tuple(sorted(stars))))

    # Exclude bought tickets for this draw/cutoff (same logic as enqueue-by-count).
    for t in (doc.get("bought_tickets") or []):
        mains = t.get("mains")
        stars = t.get("stars")
        if isinstance(mains, list) and len(mains) == 5 and isinstance(stars, list) and len(stars) == 2:
            excluded.add((tuple(sorted(mains)), tuple(sorted(stars))))

    collected: List[Dict] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if not line:
                    continue
                line = line.strip()
                if not line:
                    continue
                sp = _fw_split_euromillones_line(line)
                if not sp:
                    continue
                position, mains_str, stars_str = sp

                # Position range: inclusive start and end
                if position < start_position:
                    continue
                if position > end_position:
                    break
                if position in exclude_pos:
                    continue

                try:
                    mains = [int(x) for x in mains_str.split(",") if x]
                    stars = [int(x) for x in stars_str.split(",") if x]
                except Exception:
                    continue

                if len(mains) != 5 or len(stars) != 2:
                    continue

                key = (tuple(sorted(mains)), tuple(sorted(stars)))
                if key in excluded:
                    continue
                excluded.add(key)
                collected.append({"mains": mains, "stars": stars, "position": position})
    except FileNotFoundError:
        raise HTTPException(404, detail="Full wheel file not found on disk")

    if len(collected) == 0:
        raise HTTPException(400, detail="No tickets available in this range (all already in queue or bought)")

    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    inserted_ids = []
    for i in range(0, len(collected), EUROMILLONES_BUCKET_SIZE):
        chunk = collected[i : i + EUROMILLONES_BUCKET_SIZE]
        doc_queue = {
            "lottery": "euromillones",
            "tickets": chunk,
            "tickets_count": len(chunk),
            "draw_date": date_str,
            "cutoff_draw_id": cutoff,
            "status": "waiting",
            "created_at": now,
        }
        ins = coll_queue.insert_one(doc_queue)
        inserted_ids.append(str(ins.inserted_id))

    requested_count = end_position - start_position + 1
    return JSONResponse(
        content={
            "status": "ok",
            "requested_range": {"start_position": start_position, "end_position": end_position},
            "requested_tickets_count": requested_count,
            "collected_tickets_count": len(collected),
            "queued_items": len(inserted_ids),
            "queued_tickets_count": len(collected),
            "message": f"{len(inserted_ids)} colas creadas ({len(collected)} boletos).",
        },
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/api/euromillones/betting/buy-queue")
def api_euromillones_betting_buy_queue(limit: int = Query(50, ge=1, le=5000)):
    if db is None:
        return JSONResponse(
            content={"items": [], "last_draw_date": None, "next_draw_date": _get_next_draw_date("euromillones")},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    coll = db[EUROMILLONES_BUY_QUEUE_COLLECTION]
    last_draw_date = _get_last_draw_date("euromillones")
    next_draw_date = _get_next_draw_date("euromillones")
    queue_filter: Dict[str, Any] = {}
    if isinstance(last_draw_date, str) and last_draw_date.strip():
        queue_filter["draw_date"] = last_draw_date.strip()[:10]
    else:
        if next_draw_date:
            next_draw_date = next_draw_date.strip()[:10]
        return JSONResponse(
            content={"items": [], "last_draw_date": None, "next_draw_date": next_draw_date},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    cursor = coll.find(queue_filter).sort("created_at", -1).limit(limit)
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
    if isinstance(last_draw_date, str):
        last_draw_date = last_draw_date.strip()[:10]
    if isinstance(next_draw_date, str):
        next_draw_date = next_draw_date.strip()[:10]
    return JSONResponse(
        content={"items": items, "last_draw_date": last_draw_date, "next_draw_date": next_draw_date},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


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


@app.post("/api/euromillones/betting/save-queue-after-print")
async def api_euromillones_betting_save_queue_after_print(request: Request):
    """
    User printed the buy queue (manual slip): copy queue tickets into train_progress.bought_tickets
    (Boletos guardados). Same as save-bought-from-queue for status=bought; additionally promotes
    waiting/failed jobs with tickets to bought+saved. Skips in_progress (bot).
    """
    if db is None:
        return JSONResponse(
            content={"status": "ok", "saved_count": 0, "promoted_count": 0},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    coll = db[EUROMILLONES_BUY_QUEUE_COLLECTION]
    try:
        body = await request.json() or {}
    except Exception:
        body = {}
    selected_oids: List[ObjectId] = []
    for qid in (body.get("queue_ids") or []):
        try:
            selected_oids.append(ObjectId(str(qid)))
        except Exception:
            continue
    id_filter: Dict[str, Any] = {"_id": {"$in": selected_oids}} if selected_oids else {}
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    saved_count = 0
    promoted_count = 0
    cursor = coll.find({**id_filter, "status": "bought", "$or": [{"saved_status": {"$ne": True}}, {"saved_status": {"$exists": False}}]})
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
    for d in coll.find({**id_filter, "status": {"$in": ["waiting", "failed"]}}):
        tickets = d.get("tickets") or []
        if not tickets:
            continue
        oid = d["_id"]
        draw_date = (d.get("draw_date") or "").strip()[:10] or None
        cutoff_draw_id = (d.get("cutoff_draw_id") or "").strip() or None
        _append_euromillones_bought_tickets(tickets, draw_date=draw_date, cutoff_draw_id=cutoff_draw_id)
        coll.update_one(
            {"_id": oid},
            {"$set": {"status": "bought", "saved_status": True, "finished_at": now, "error": None}},
        )
        promoted_count += 1
    return JSONResponse(
        content={"status": "ok", "saved_count": saved_count, "promoted_count": promoted_count},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.delete("/api/euromillones/betting/buy-queue/waiting")
def api_euromillones_betting_buy_queue_delete_all_waiting(draw_date: Optional[str] = Query(None)):
    """Remove waiting queue rows, optionally only for one draw_date."""
    if db is None:
        raise HTTPException(status_code=500, detail="Database not connected")
    coll = db[EUROMILLONES_BUY_QUEUE_COLLECTION]
    q: Dict[str, Any] = {"status": "waiting"}
    draw_date_clean = (draw_date or "").strip()[:10]
    if draw_date_clean:
        q["draw_date"] = draw_date_clean
    result = coll.delete_many(q)
    return JSONResponse(
        content={"status": "ok", "deleted_count": result.deleted_count},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.post("/api/euromillones/betting/buy-queue/{queue_id}/repair")
def api_euromillones_betting_buy_queue_repair(queue_id: str):
    """Set failed/in_progress queue item back to waiting so bot can retry."""
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
    if st not in ("in_progress", "failed"):
        raise HTTPException(status_code=400, detail="Solo se puede reparar cuando está Comprando (in_progress) o Error (failed)")
    coll.update_one(
        {"_id": oid, "status": {"$in": ["in_progress", "failed"]}},
        {"$set": {"status": "waiting", "started_at": None, "finished_at": None, "error": None}},
    )
    return JSONResponse(content={"status": "ok", "message": "Cola reparada y reenviada a waiting"}, headers={"Cache-Control": "no-store, no-cache, must-revalidate"})


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
    """Append tickets to bought_tickets in la_primitiva_train_progress (merge, dedupe by mains). Preserves optional position."""
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
    merged: List[Dict[str, Any]] = [dict(x) for x in existing if isinstance(x, dict)]
    key_to_idx: Dict[Tuple[int, ...], int] = {}
    for i, x in enumerate(merged):
        mm = x.get("mains") or []
        if isinstance(mm, list) and len(mm) == 6:
            try:
                k = tuple(sorted(int(v) for v in mm))
                key_to_idx[k] = i
            except (TypeError, ValueError):
                pass
    for t in tickets:
        if not isinstance(t, dict):
            continue
        mains = t.get("mains") or []
        if len(mains) != 6:
            continue
        key = tuple(sorted(int(x) for x in mains))
        pos = _optional_wheel_position(t)
        if key not in key_to_idx:
            entry: Dict[str, Any] = {"mains": list(mains), "reintegro": int(t.get("reintegro", 0))}
            if pos is not None:
                entry["position"] = pos
            merged.append(entry)
            key_to_idx[key] = len(merged) - 1
        else:
            idx = key_to_idx[key]
            if pos is not None:
                merged[idx]["position"] = pos
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
    """Append tickets to bought_tickets in euromillones_train_progress (merge, dedupe by mains+stars). Preserves optional position."""
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
    merged: List[Dict[str, Any]] = [dict(x) for x in existing if isinstance(x, dict)]
    key_to_idx: Dict[Tuple[Tuple[int, ...], Tuple[int, ...]], int] = {}
    for i, x in enumerate(merged):
        mm = x.get("mains") or []
        ss = x.get("stars") or []
        if isinstance(mm, list) and len(mm) == 5 and isinstance(ss, list) and len(ss) == 2:
            try:
                k = (tuple(int(v) for v in mm), tuple(int(v) for v in ss))
                key_to_idx[k] = i
            except (TypeError, ValueError):
                pass
    for t in tickets:
        if not isinstance(t, dict):
            continue
        mains = t.get("mains") or []
        stars = t.get("stars") or []
        if len(mains) != 5 or len(stars) != 2:
            continue
        key = (tuple(int(x) for x in mains), tuple(int(x) for x in stars))
        pos = _optional_wheel_position(t)
        if key not in key_to_idx:
            entry: Dict[str, Any] = {"mains": list(mains), "stars": list(stars)}
            if pos is not None:
                entry["position"] = pos
            merged.append(entry)
            key_to_idx[key] = len(merged) - 1
        elif pos is not None:
            merged[key_to_idx[key]]["position"] = pos
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
                sp = _fw_split_la_primitiva_line(line)
                if not sp:
                    continue
                position, mains_str, rein_str = sp
                try:
                    mains = [int(x) for x in mains_str.split(",") if x]
                except Exception:
                    continue
                parts = line.split(";")
                tid = parts[0] if len(parts) >= 3 and "_COMBO_" in (parts[0] or "") else None
                row: Dict[str, Any] = {"position": position, "mains": mains}
                if tid:
                    row["ticket_id"] = tid
                if rein_str is not None and str(rein_str).strip() != "":
                    try:
                        row["reintegro"] = int(str(rein_str).strip())
                    except Exception:
                        pass
                tickets_list.append(row)
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
        item_lp: Dict[str, Any] = {"mains": [int(m) for m in mains], "reintegro": int(t.get("reintegro", 0))}
        pos_lp = _optional_wheel_position(t)
        if pos_lp is not None:
            item_lp["position"] = pos_lp
        normalized.append(item_lp)
    draw_date = (body.get("draw_date") or "").strip()[:10] or None
    cutoff_draw_id = (body.get("cutoff_draw_id") or "").strip() or None
    if db is None:
        raise HTTPException(503, detail="Database not available")
    if not draw_date:
        coll_progress = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
        doc_progress = None
        if cutoff_draw_id:
            doc_progress = coll_progress.find_one({"cutoff_draw_id": cutoff_draw_id})
            if doc_progress is None and cutoff_draw_id.isdigit():
                doc_progress = coll_progress.find_one({"cutoff_draw_id": int(cutoff_draw_id)})
        if doc_progress is not None:
            draw_date = (doc_progress.get("probs_fecha_sorteo") or "").strip()[:10] or None
        if not draw_date:
            draw_date = (_get_last_draw_date("la-primitiva") or "").strip()[:10] or None
    if not draw_date:
        raise HTTPException(400, detail="No draw_date resolved; provide draw_date or cutoff_draw_id")
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


LA_PRIMITIVA_BUCKET_SIZE = 8


@app.post("/api/la-primitiva/betting/enqueue-by-count")
async def api_la_primitiva_betting_enqueue_by_count(request: Request):
    """
    Create buy-queue items by ticket count. Body: { "count": int, "draw_date"?, "cutoff_draw_id"? }.
    Reads from full_wheel file, splits into buckets of 8 (one queue item per bucket). Each bucket
    gets a random reintegro (0-9). Capped by LOTTERY_MAX_ENQUEUE_BY_COUNT (default 100000).
    Example: count=800 → 100 queue items of 8 tickets each, each queue with a random reintegro.
    """
    try:
        body = await request.json() or {}
    except Exception:
        body = {}
    count = body.get("count")
    if not isinstance(count, int) or count < 1:
        raise HTTPException(400, detail="Body must contain 'count' (positive integer)")
    max_n = _lottery_max_enqueue_by_count()
    if count > max_n:
        raise HTTPException(
            400,
            detail=f"count must be <= {max_n} per request (raise LOTTERY_MAX_ENQUEUE_BY_COUNT or split into multiple requests)",
        )
    draw_date = (body.get("draw_date") or "").strip()[:10] or None
    cutoff_draw_id = (body.get("cutoff_draw_id") or "").strip() or None
    if db is None:
        raise HTTPException(503, detail="Database not available")
    coll_progress = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
    coll_queue = db[LA_PRIMITIVA_BUY_QUEUE_COLLECTION]
    date_str = draw_date
    cutoff = cutoff_draw_id
    if not date_str and not cutoff:
        last_draw_date = _get_last_draw_date("la-primitiva")
        if not last_draw_date:
            raise HTTPException(400, detail="No last_draw_date for la-primitiva; provide draw_date or cutoff_draw_id")
        date_str = (last_draw_date or "").strip()[:10]
    doc = None
    if cutoff:
        doc = coll_progress.find_one({"cutoff_draw_id": cutoff})
        if doc is None and cutoff.isdigit():
            doc = coll_progress.find_one({"cutoff_draw_id": int(cutoff)})
    if doc is None and date_str:
        doc = coll_progress.find_one({"full_wheel_draw_date": date_str}) or coll_progress.find_one({"probs_fecha_sorteo": date_str})
    if doc is None:
        raise HTTPException(404, detail="No train progress found for draw_date or cutoff_draw_id")
    path = (doc.get("full_wheel_file_path") or "").strip()
    total_in_file = int(doc.get("full_wheel_total_tickets") or 0)
    if not path or total_in_file <= 0:
        raise HTTPException(404, detail="Full wheel file not found or empty for this draw")
    date_str = (doc.get("full_wheel_draw_date") or doc.get("probs_fecha_sorteo") or date_str or "").strip()[:10] or None
    cutoff = str(doc.get("cutoff_draw_id") or "") or cutoff
    excluded: set = set()
    q_filter: dict = {"status": {"$in": ["waiting", "in_progress"]}}
    if date_str:
        q_filter["draw_date"] = date_str
    if cutoff:
        q_filter["cutoff_draw_id"] = cutoff
    for d in coll_queue.find(q_filter, projection={"tickets": 1}):
        for t in (d.get("tickets") or []):
            mains = t.get("mains")
            if isinstance(mains, list) and len(mains) == 6:
                r = t.get("reintegro")
                try:
                    r_int = int(r)
                except Exception:
                    r_int = 0
                excluded.add(tuple(sorted(mains) + [100 + max(0, min(9, r_int))]))
    for t in (doc.get("bought_tickets") or []):
        mains = t.get("mains")
        if isinstance(mains, list) and len(mains) == 6:
            r = t.get("reintegro")
            try:
                r_int = int(r)
            except Exception:
                r_int = 0
            excluded.add(tuple(sorted(mains) + [100 + max(0, min(9, r_int))]))
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    inserted_ids: List[str] = []
    items_meta: List[Dict] = []
    bucket_buf: List[Dict] = []
    total_collected = 0
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if total_collected >= count:
                    break
                line = line.strip()
                if not line:
                    continue
                sp = _fw_split_la_primitiva_line(line)
                if not sp:
                    continue
                position, mains_str, rein_str = sp
                try:
                    mains = [int(x) for x in mains_str.split(",") if x]
                except Exception:
                    continue
                if len(mains) != 6:
                    continue
                r_val = 0
                if rein_str is not None and str(rein_str).strip() != "":
                    try:
                        r_val = int(str(rein_str).strip())
                    except Exception:
                        r_val = 0
                key = tuple(sorted(mains) + [100 + max(0, min(9, r_val))])
                if key in excluded:
                    continue
                excluded.add(key)
                bucket_buf.append({"mains": mains, "position": position, "reintegro": max(0, min(9, r_val))})
                total_collected += 1
                if len(bucket_buf) >= LA_PRIMITIVA_BUCKET_SIZE:
                    raw_chunk = bucket_buf[:LA_PRIMITIVA_BUCKET_SIZE]
                    del bucket_buf[:LA_PRIMITIVA_BUCKET_SIZE]
                    chunk = [{"mains": t["mains"], "reintegro": int(t.get("reintegro") or 0), "position": t["position"]} for t in raw_chunk]
                    doc_queue = {
                        "lottery": "la-primitiva",
                        "tickets": chunk,
                        "tickets_count": len(chunk),
                        "draw_date": date_str,
                        "cutoff_draw_id": cutoff,
                        "status": "waiting",
                        "created_at": now,
                    }
                    ins = coll_queue.insert_one(doc_queue)
                    qid = str(ins.inserted_id)
                    inserted_ids.append(qid)
                    items_meta.append({"id": qid, "tickets_count": len(chunk)})
                if total_collected >= count:
                    break
    except FileNotFoundError:
        raise HTTPException(404, detail="Full wheel file not found on disk")
    if bucket_buf:
        raw_chunk = bucket_buf
        reintegro_bucket = random.randint(0, 9)
        chunk = [{"mains": t["mains"], "reintegro": reintegro_bucket, "position": t["position"]} for t in raw_chunk]
        doc_queue = {
            "lottery": "la-primitiva",
            "tickets": chunk,
            "tickets_count": len(chunk),
            "draw_date": date_str,
            "cutoff_draw_id": cutoff,
            "status": "waiting",
            "created_at": now,
        }
        ins = coll_queue.insert_one(doc_queue)
        qid = str(ins.inserted_id)
        inserted_ids.append(qid)
        items_meta.append({"id": qid, "tickets_count": len(chunk)})
    if total_collected == 0:
        raise HTTPException(400, detail="No tickets available (all already in queue or bought)")
    return JSONResponse(
        content={
            "status": "ok",
            "queued": len(inserted_ids),
            "total_tickets": total_collected,
            "items": items_meta,
            "message": f"{len(inserted_ids)} colas creadas ({total_collected} boletos).",
        },
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.post("/api/la-primitiva/betting/enqueue-by-range")
async def api_la_primitiva_betting_enqueue_by_range(request: Request):
    """
    Create buy-queue items by position range from the La Primitiva full-wheeling pool TXT file.

    Body: { "start_position": int, "end_position": int, "draw_date"?, "cutoff_draw_id"?, "exclude_positions"?: int[] }
      - Range is inclusive on both ends: positions start_position..end_position.
      - Each queued ticket gets a random reintegro (0-9).
      - Optional exclude_positions: wheel line indices to skip (cesta/cola/guardados from UI).
      - Tickets already in queue (waiting/in_progress/bought) or bought are skipped by combo.
    """
    try:
        body = await request.json() or {}
    except Exception:
        body = {}

    start_position = body.get("start_position")
    end_position = body.get("end_position")
    if not isinstance(start_position, int) or not isinstance(end_position, int):
        raise HTTPException(400, detail="Body must contain 'start_position' and 'end_position' (integers)")
    if start_position < 1:
        raise HTTPException(400, detail="start_position must be >= 1")
    if end_position < start_position:
        raise HTTPException(400, detail="end_position must be >= start_position")

    draw_date = (body.get("draw_date") or "").strip()[:10] or None
    cutoff_draw_id = (body.get("cutoff_draw_id") or "").strip() or None

    if db is None:
        raise HTTPException(503, detail="Database not available")

    coll_progress = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
    coll_queue = db[LA_PRIMITIVA_BUY_QUEUE_COLLECTION]

    date_str = draw_date
    cutoff = cutoff_draw_id
    if not date_str and not cutoff:
        last_draw_date = _get_last_draw_date("la-primitiva")
        if last_draw_date:
            date_str = (last_draw_date or "").strip()[:10]
        if not date_str:
            raise HTTPException(400, detail="No last_draw_date for la-primitiva; provide draw_date or cutoff_draw_id")

    doc = None
    if cutoff:
        doc = coll_progress.find_one({"cutoff_draw_id": cutoff})
        if doc is None and cutoff.isdigit():
            doc = coll_progress.find_one({"cutoff_draw_id": int(cutoff)})
    if doc is None and date_str:
        doc = coll_progress.find_one({"full_wheel_draw_date": date_str}) or coll_progress.find_one({"probs_fecha_sorteo": date_str})
    if doc is None:
        raise HTTPException(404, detail="No train progress found for draw_date or cutoff_draw_id")

    path = (doc.get("full_wheel_file_path") or "").strip()
    total_in_file = int(doc.get("full_wheel_total_tickets") or 0)
    if not path or total_in_file <= 0:
        raise HTTPException(404, detail="Full wheel file not found or empty for this draw")

    if start_position > total_in_file:
        raise HTTPException(400, detail=f"start_position must be <= total tickets ({total_in_file})")
    if end_position > total_in_file:
        raise HTTPException(400, detail=f"end_position must be <= total tickets ({total_in_file})")

    exclude_pos = _enqueue_range_exclude_positions_from_body(body)

    # Exclusions (queued + bought) to avoid duplicates.
    excluded: set[tuple[int, ...]] = set()
    for d in coll_queue.find({"status": {"$in": ["waiting", "in_progress", "bought"]}}, projection={"tickets": 1}):
        d_date = (d.get("draw_date") or "").strip()
        d_cutoff = (d.get("cutoff_draw_id") or "").strip()
        if d_date != date_str and d_cutoff != cutoff:
            continue
        for t in (d.get("tickets") or []):
            mains = t.get("mains")
            if isinstance(mains, list) and len(mains) == 6:
                r = t.get("reintegro")
                try:
                    r_int = int(r)
                except Exception:
                    r_int = 0
                excluded.add(tuple(sorted(mains) + [100 + max(0, min(9, r_int))]))

    for t in (doc.get("bought_tickets") or []):
        mains = t.get("mains")
        if isinstance(mains, list) and len(mains) == 6:
            r = t.get("reintegro")
            try:
                r_int = int(r)
            except Exception:
                r_int = 0
            excluded.add(tuple(sorted(mains) + [100 + max(0, min(9, r_int))]))

    collected: List[Dict] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if not line:
                    continue
                line = line.strip()
                if not line:
                    continue
                sp = _fw_split_la_primitiva_line(line)
                if not sp:
                    continue
                position, mains_str, rein_str = sp

                if position < start_position:
                    continue
                if position > end_position:
                    break
                if position in exclude_pos:
                    continue

                try:
                    mains = [int(x) for x in mains_str.split(",") if x]
                except Exception:
                    continue

                if len(mains) != 6:
                    continue

                r_val = 0
                if rein_str is not None and str(rein_str).strip() != "":
                    try:
                        r_val = int(str(rein_str).strip())
                    except Exception:
                        r_val = 0

                key = tuple(sorted(mains) + [100 + r_val])
                if key in excluded:
                    continue
                excluded.add(key)
                collected.append({"mains": mains, "position": position, "reintegro": r_val})
    except FileNotFoundError:
        raise HTTPException(404, detail="Full wheel file not found on disk")

    if len(collected) == 0:
        raise HTTPException(400, detail="No tickets available in this range (all already in queue or bought)")

    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    inserted_ids: List[str] = []
    for i in range(0, len(collected), LA_PRIMITIVA_BUCKET_SIZE):
        raw_chunk = collected[i : i + LA_PRIMITIVA_BUCKET_SIZE]
        chunk = [{"mains": t["mains"], "reintegro": int(t.get("reintegro") or 0), "position": t["position"]} for t in raw_chunk]
        doc_queue = {
            "lottery": "la-primitiva",
            "tickets": chunk,
            "tickets_count": len(chunk),
            "draw_date": date_str,
            "cutoff_draw_id": cutoff,
            "status": "waiting",
            "created_at": now,
        }
        ins = coll_queue.insert_one(doc_queue)
        inserted_ids.append(str(ins.inserted_id))

    requested_count = end_position - start_position + 1
    return JSONResponse(
        content={
            "status": "ok",
            "requested_range": {"start_position": start_position, "end_position": end_position},
            "requested_tickets_count": requested_count,
            "collected_tickets_count": len(collected),
            "queued_items": len(inserted_ids),
            "queued_tickets_count": len(collected),
            "message": f"{len(inserted_ids)} colas creadas ({len(collected)} boletos).",
        },
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/api/la-primitiva/betting/buy-queue")
def api_la_primitiva_betting_buy_queue(limit: int = Query(50, ge=1, le=5000)):
    if db is None:
        return JSONResponse(
            content={"items": [], "last_draw_date": None, "next_draw_date": _get_next_draw_date("la-primitiva")},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    coll = db[LA_PRIMITIVA_BUY_QUEUE_COLLECTION]
    last_draw_date = _get_last_draw_date("la-primitiva")
    next_draw_date = _get_next_draw_date("la-primitiva")
    queue_filter: Dict[str, Any] = {}
    if isinstance(last_draw_date, str) and last_draw_date.strip():
        queue_filter["draw_date"] = last_draw_date.strip()[:10]
    else:
        if next_draw_date:
            next_draw_date = next_draw_date.strip()[:10]
        return JSONResponse(
            content={"items": [], "last_draw_date": None, "next_draw_date": next_draw_date},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    cursor = coll.find(queue_filter).sort("created_at", -1).limit(limit)
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
    if isinstance(last_draw_date, str):
        last_draw_date = last_draw_date.strip()[:10]
    if isinstance(next_draw_date, str):
        next_draw_date = next_draw_date.strip()[:10]
    return JSONResponse(
        content={"items": items, "last_draw_date": last_draw_date, "next_draw_date": next_draw_date},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


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


@app.post("/api/la-primitiva/betting/save-queue-after-print")
async def api_la_primitiva_betting_save_queue_after_print(request: Request):
    """Print buy queue → sync into la_primitiva_train_progress.bought_tickets; promote waiting/failed with tickets."""
    if db is None:
        return JSONResponse(
            content={"status": "ok", "saved_count": 0, "promoted_count": 0},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    coll = db[LA_PRIMITIVA_BUY_QUEUE_COLLECTION]
    try:
        body = await request.json() or {}
    except Exception:
        body = {}
    selected_oids: List[ObjectId] = []
    for qid in (body.get("queue_ids") or []):
        try:
            selected_oids.append(ObjectId(str(qid)))
        except Exception:
            continue
    id_filter: Dict[str, Any] = {"_id": {"$in": selected_oids}} if selected_oids else {}
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    saved_count = 0
    promoted_count = 0
    cursor = coll.find({**id_filter, "status": "bought", "$or": [{"saved_status": {"$ne": True}}, {"saved_status": {"$exists": False}}]})
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
    for d in coll.find({**id_filter, "status": {"$in": ["waiting", "failed"]}}):
        tickets = d.get("tickets") or []
        if not tickets:
            continue
        oid = d["_id"]
        draw_date = (d.get("draw_date") or "").strip()[:10] or None
        cutoff_draw_id = (d.get("cutoff_draw_id") or "").strip() or None
        _append_la_primitiva_bought_tickets(tickets, draw_date=draw_date, cutoff_draw_id=cutoff_draw_id)
        coll.update_one(
            {"_id": oid},
            {"$set": {"status": "bought", "saved_status": True, "finished_at": now, "error": None}},
        )
        promoted_count += 1
    return JSONResponse(
        content={"status": "ok", "saved_count": saved_count, "promoted_count": promoted_count},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.delete("/api/la-primitiva/betting/buy-queue/waiting")
def api_la_primitiva_betting_buy_queue_delete_all_waiting(draw_date: Optional[str] = Query(None)):
    """Remove waiting queue rows, optionally only for one draw_date."""
    if db is None:
        raise HTTPException(status_code=500, detail="Database not connected")
    coll = db[LA_PRIMITIVA_BUY_QUEUE_COLLECTION]
    q: Dict[str, Any] = {"status": "waiting"}
    draw_date_clean = (draw_date or "").strip()[:10]
    if draw_date_clean:
        q["draw_date"] = draw_date_clean
    result = coll.delete_many(q)
    return JSONResponse(
        content={"status": "ok", "deleted_count": result.deleted_count},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.post("/api/la-primitiva/betting/buy-queue/{queue_id}/repair")
def api_la_primitiva_betting_buy_queue_repair(queue_id: str):
    """Set failed/in_progress queue item back to waiting so bot can retry."""
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
    if st not in ("in_progress", "failed"):
        raise HTTPException(status_code=400, detail="Solo se puede reparar cuando está Comprando (in_progress) o Error (failed)")
    coll.update_one(
        {"_id": oid, "status": {"$in": ["in_progress", "failed"]}},
        {"$set": {"status": "waiting", "started_at": None, "finished_at": None, "error": None}},
    )
    return JSONResponse(content={"status": "ok", "message": "Cola reparada y reenviada a waiting"}, headers={"Cache-Control": "no-store, no-cache, must-revalidate"})


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
    """Run steps 1–4 in the backend. Frontend only starts and polls progress. Atomic: only one run per cutoff."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if not cutoff_draw_id:
        raise HTTPException(400, detail="cutoff_draw_id is required")
    cid = cutoff_draw_id.strip()
    coll = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
    doc = coll.find_one({"cutoff_draw_id": cid}, projection={"pipeline_status": 1, "rules_applied": 1})
    if doc and doc.get("pipeline_status") == "running":
        return JSONResponse(content={"status": "running", "cutoff_draw_id": cid})
    if doc and doc.get("rules_applied"):
        return JSONResponse(content={"status": "done", "cutoff_draw_id": cid, "message": "Pool already generated for this cutoff."})
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    # Atomic claim: only one request can set running (prevents rerun from starting a second thread)
    res = coll.update_one(
        {"cutoff_draw_id": cid, "$or": [{"pipeline_status": {"$exists": False}}, {"pipeline_status": {"$ne": "running"}}]},
        {"$set": {"cutoff_draw_id": cid, "pipeline_status": "running", "pipeline_error": None, "pipeline_started_at": now}},
    )
    if res.modified_count >= 1:
        pass
    else:
        doc2 = coll.find_one({"cutoff_draw_id": cid}, projection={"pipeline_status": 1})
        if doc2 and doc2.get("pipeline_status") == "running":
            return JSONResponse(content={"status": "running", "cutoff_draw_id": cid})
        if not doc2:
            try:
                coll.insert_one({"cutoff_draw_id": cid, "pipeline_status": "running", "pipeline_error": None, "pipeline_started_at": now})
            except DuplicateKeyError:
                return JSONResponse(content={"status": "running", "cutoff_draw_id": cid})

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
                {"$set": {
                    "models_trained": True,
                    "trained_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "main_accuracy": info.get("main_accuracy"),
                    "clave_accuracy": info.get("clave_accuracy"),
                    "main_rf_accuracy": info.get("main_rf_accuracy"),
                    "clave_rf_accuracy": info.get("clave_rf_accuracy"),
                    "main_lstm_accuracy": info.get("main_lstm_accuracy"),
                    "clave_lstm_accuracy": info.get("clave_lstm_accuracy"),
                }},
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
    """Run steps 1–4 in the backend. Frontend only starts and polls progress. Atomic: only one run per cutoff."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if not cutoff_draw_id:
        raise HTTPException(400, detail="cutoff_draw_id is required")
    cid = cutoff_draw_id.strip()
    coll = db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION]
    doc = coll.find_one({"cutoff_draw_id": cid}, projection={"pipeline_status": 1, "rules_applied": 1})
    if doc and doc.get("pipeline_status") == "running":
        return JSONResponse(content={"status": "running", "cutoff_draw_id": cid})
    if doc and doc.get("rules_applied"):
        return JSONResponse(content={"status": "done", "cutoff_draw_id": cid, "message": "Pool already generated for this cutoff."})
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    # Atomic claim: only one request can set running (prevents rerun from starting a second thread)
    res = coll.update_one(
        {"cutoff_draw_id": cid, "$or": [{"pipeline_status": {"$exists": False}}, {"pipeline_status": {"$ne": "running"}}]},
        {"$set": {"cutoff_draw_id": cid, "pipeline_status": "running", "pipeline_error": None, "pipeline_started_at": now}},
    )
    if res.modified_count >= 1:
        pass
    else:
        doc2 = coll.find_one({"cutoff_draw_id": cid}, projection={"pipeline_status": 1})
        if doc2 and doc2.get("pipeline_status") == "running":
            return JSONResponse(content={"status": "running", "cutoff_draw_id": cid})
        if not doc2:
            try:
                coll.insert_one({"cutoff_draw_id": cid, "pipeline_status": "running", "pipeline_error": None, "pipeline_started_at": now})
            except DuplicateKeyError:
                return JSONResponse(content={"status": "running", "cutoff_draw_id": cid})

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
                {"$set": {
                    "models_trained": True,
                    "trained_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "main_accuracy": info.get("main_accuracy"),
                    "reintegro_accuracy": info.get("reintegro_accuracy"),
                    "main_rf_accuracy": info.get("main_rf_accuracy"),
                    "reintegro_rf_accuracy": info.get("reintegro_rf_accuracy"),
                    "main_lstm_accuracy": info.get("main_lstm_accuracy"),
                    "reintegro_lstm_accuracy": info.get("reintegro_lstm_accuracy"),
                }},
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
        description="Optional YYYY-MM-DD or YYYYMMDD; default is scraper_metadata.next_draw_date for la-primitiva.",
    ),
):
    """
    Generate full La Primitiva wheeling file from the Step 4 pool (49 mains).

    - Reads filtered_mains_probs from la_primitiva_train_progress.
    - Builds the ordered mains pool (49 numbers, prioritized first, then the rest).
    - Generates ALL tickets C(49,6) in a manifold order using the pool order.
    - Applies structural rules via _la_primitiva_ticket_tier to group tickets
      into tiers (good → bad) and writes them in that order to TXT.
    - Filename la_primitiva_<YYYYMMDD>.txt; lines prefixed with LA_PRIMITIVA_DRAW_<date>_COMBO_<n>.
    - Default draw date: scraper_metadata.next_draw_date (optional draw_date overrides).
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

    date_iso, date_compact = _resolve_full_wheel_dates_for_api("la-primitiva", draw_date)
    date_str = date_iso

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
                draw_date_iso=date_iso,
                file_date_compact=date_compact,
                lottery_token="LA_PRIMITIVA",
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
            "file_date_compact": date_compact,
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
                    "main_rf_accuracy": info.get("main_rf_accuracy"),
                    "star_rf_accuracy": info.get("star_rf_accuracy"),
                    "main_lstm_accuracy": info.get("main_lstm_accuracy"),
                    "star_lstm_accuracy": info.get("star_lstm_accuracy"),
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
    Frontend only starts this and polls GET /api/euromillones/train/progress. Atomic: only one run per cutoff.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if not cutoff_draw_id:
        raise HTTPException(400, detail="cutoff_draw_id is required")
    cid = cutoff_draw_id.strip()
    coll = db[EUROMILLONES_TRAIN_PROGRESS_COLLECTION]
    doc = coll.find_one({"cutoff_draw_id": cid}, projection={"pipeline_status": 1, "rules_applied": 1})
    if doc and doc.get("pipeline_status") == "running":
        return JSONResponse(content={"status": "running", "cutoff_draw_id": cid})
    if doc and doc.get("rules_applied"):
        return JSONResponse(
            content={"status": "done", "cutoff_draw_id": cid, "message": "Pool already generated for this cutoff."}
        )
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    # Atomic claim: only one request can set running (prevents rerun from starting a second thread)
    res = coll.update_one(
        {"cutoff_draw_id": cid, "$or": [{"pipeline_status": {"$exists": False}}, {"pipeline_status": {"$ne": "running"}}]},
        {
            "$set": {
                "cutoff_draw_id": cid,
                "pipeline_status": "running",
                "pipeline_error": None,
                "pipeline_started_at": now,
            }
        },
    )
    if res.modified_count >= 1:
        pass
    else:
        doc2 = coll.find_one({"cutoff_draw_id": cid}, projection={"pipeline_status": 1})
        if doc2 and doc2.get("pipeline_status") == "running":
            return JSONResponse(content={"status": "running", "cutoff_draw_id": cid})
        if not doc2:
            try:
                coll.insert_one({"cutoff_draw_id": cid, "pipeline_status": "running", "pipeline_error": None, "pipeline_started_at": now})
            except DuplicateKeyError:
                return JSONResponse(content={"status": "running", "cutoff_draw_id": cid})

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
                        # RF + LSTM ensemble accuracies
                        "main_rf_accuracy": info.get("main_rf_accuracy"),
                        "star_rf_accuracy": info.get("star_rf_accuracy"),
                        "main_lstm_accuracy": info.get("main_lstm_accuracy"),
                        "star_lstm_accuracy": info.get("star_lstm_accuracy"),
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
        description="Optional YYYY-MM-DD or YYYYMMDD; default is scraper_metadata.next_draw_date for euromillones.",
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
    - Filename euromillones_<YYYYMMDD>.txt; each line prefixed with EUROMILLONES_DRAW_<date>_COMBO_<n>.
    - Default draw date: scraper_metadata.next_draw_date (optional draw_date overrides).
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

    date_iso, date_compact = _resolve_full_wheel_dates_for_api("euromillones", draw_date)
    date_str = date_iso

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
                draw_date_iso=date_iso,
                file_date_compact=date_compact,
                lottery_token="EUROMILLONES",
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
            "file_date_compact": date_compact,
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
        description="Optional YYYY-MM-DD or YYYYMMDD; default is scraper_metadata.next_draw_date for el-gordo.",
    ),
):
    """
    Generate full El Gordo wheeling file from Step 4 pool (mirror Euromillones).

    - Uses filtered_mains_probs and filtered_stars_probs from Step 4 (54 mains, 10 claves after extend).
    - Generates ALL tickets: C(54,5) * 10 = 31,625,100; classifies by tier (good/bad), writes good first.
    - Filename el_gordo_<YYYYMMDD>.txt; lines prefixed with ELGORDO_DRAW_<date>_COMBO_<n>.
    - Default draw date: scraper_metadata.next_draw_date (optional draw_date overrides).
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

    date_iso, date_compact = _resolve_full_wheel_dates_for_api("el-gordo", draw_date)
    date_str = date_iso

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
                draw_date_iso=date_iso,
                file_date_compact=date_compact,
                lottery_token="ELGORDO",
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
            "file_date_compact": date_compact,
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
                sp = _fw_split_el_gordo_line(line)
                if not sp:
                    continue
                position, mains_str, clave_str = sp
                try:
                    mains = [int(x) for x in mains_str.split(",") if x]
                    clave = int(clave_str)
                except Exception:
                    continue
                parts = line.split(";")
                tid = parts[0] if len(parts) >= 4 and "_COMBO_" in (parts[0] or "") else None
                row: Dict[str, Any] = {"position": position, "mains": mains, "clave": clave}
                if tid:
                    row["ticket_id"] = tid
                tickets.append(row)
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
                sp = _fw_split_euromillones_line(line)
                if not sp:
                    continue
                position, mains_str, stars_str = sp
                try:
                    mains = [int(x) for x in mains_str.split(",") if x]
                    stars = [int(x) for x in stars_str.split(",") if x]
                except Exception:
                    continue
                parts = line.split(";")
                tid = parts[0] if len(parts) >= 4 and "_COMBO_" in (parts[0] or "") else None
                row: Dict[str, Any] = {"position": position, "mains": mains, "stars": stars}
                if tid:
                    row["ticket_id"] = tid
                tickets.append(row)
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
    return JSONResponse(content={"status": "ok", "candidate_pool_count": len(tickets)})


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
                sp = _fw_split_la_primitiva_line(line)
                if not sp:
                    continue
                position, mains_str, rein_str = sp
                try:
                    mains = [int(x) for x in mains_str.split(",") if x]
                except Exception:
                    continue
                parts = line.split(";")
                tid = parts[0] if len(parts) >= 3 and "_COMBO_" in (parts[0] or "") else None
                row: Dict[str, Any] = {"position": position, "mains": mains}
                if tid:
                    row["ticket_id"] = tid
                if rein_str is not None and str(rein_str).strip() != "":
                    try:
                        row["reintegro"] = int(str(rein_str).strip())
                    except Exception:
                        pass
                tickets.append(row)
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


# Dev-only: pool TXT folders (el_gordo_pools, euromillones_pools, la_primitiva_pools) — list and delete.
_POOL_FOLDERS = {
    "el_gordo": "el_gordo_pools",
    "euromillones": "euromillones_pools",
    "la_primitiva": "la_primitiva_pools",
}


def _get_pool_folder_path(lottery: str) -> Path:
    if lottery not in _POOL_FOLDERS:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}")
    return Path(_ROOT_DIR) / _POOL_FOLDERS[lottery]


@app.get("/api/dev/pools")
def api_dev_pools_list():
    """
    List .txt files in each pool folder (el_gordo_pools, euromillones_pools, la_primitiva_pools).
    Public endpoint for dev UI; no link in main app.
    """
    result: Dict[str, List[Dict[str, Any]]] = {}
    for lottery, folder_name in _POOL_FOLDERS.items():
        folder = Path(_ROOT_DIR) / folder_name
        files: List[Dict[str, Any]] = []
        if folder.is_dir():
            for p in sorted(folder.iterdir()):
                if p.is_file() and p.suffix.lower() == ".txt":
                    try:
                        stat = p.stat()
                        files.append({
                            "name": p.name,
                            "size": stat.st_size,
                            "modified": dt.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%dT%H:%M:%SZ"),
                        })
                    except OSError:
                        files.append({"name": p.name, "size": None, "modified": None})
        result[lottery] = files
    return result


@app.delete("/api/dev/pools")
def api_dev_pools_delete(
    lottery: Optional[str] = Query(None, description="el_gordo | euromillones | la_primitiva; if omitted, delete all three."),
    file: Optional[str] = Query(None, description="If set with lottery, delete only this .txt filename (no path)."),
):
    """
    Delete .txt files in pool folder(s). If lottery+file are given, delete only that file; if lottery only, that folder; else all three.
    Public endpoint for dev UI; no link in main app.
    """
    if file is not None and file.strip():
        # Single file delete: require lottery, and filename must be safe (no path, .txt)
        if not lottery:
            raise HTTPException(400, detail="lottery is required when file is specified")
        if lottery not in _POOL_FOLDERS:
            raise HTTPException(400, detail=f"Unknown lottery: {lottery}")
        name = file.strip()
        if "/" in name or "\\" in name or name != os.path.basename(name):
            raise HTTPException(400, detail="Invalid file name")
        if not name.lower().endswith(".txt"):
            raise HTTPException(400, detail="Only .txt files can be deleted")
        folder = _get_pool_folder_path(lottery)
        path = folder / name
        if not path.is_file():
            raise HTTPException(404, detail=f"File not found: {name}")
        try:
            path.unlink()
        except OSError as e:
            raise HTTPException(500, detail=str(e))
        return {"deleted": {lottery: 1}, "errors": None}

    to_process = [lottery] if lottery else list(_POOL_FOLDERS.keys())
    if lottery and lottery not in _POOL_FOLDERS:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}")
    deleted: Dict[str, int] = {}
    errors: List[str] = []
    for lot in to_process:
        folder = _get_pool_folder_path(lot)
        count = 0
        if folder.is_dir():
            for p in folder.iterdir():
                if p.is_file() and p.suffix.lower() == ".txt":
                    try:
                        p.unlink()
                        count += 1
                    except OSError as e:
                        errors.append(f"{lot}/{p.name}: {e}")
        deleted[lot] = count
    return {"deleted": deleted, "errors": errors[:20] if errors else None}


# ── Ranking Dashboard endpoints ───────────────────────────────────────────────

_RANKING_LOTTERY_MAP = {
    "euromillones": {
        "draw_probs":    "euromillones_draw_probs",
        "compare":       "euromillones_compare_results",
        "feature":       "euromillones_feature",
        "tickets":       "euromillones_tickets",
        "secondary_key": "stars_probs",
        "secondary_field": "stars",
        "ticket_cost":   2.50,
    },
    "el-gordo": {
        "draw_probs":    "el_gordo_draw_probs",
        "compare":       "el_gordo_compare_results",
        "feature":       "el_gordo_feature",
        "tickets":       "el_gordo_tickets",
        "secondary_key": "clave_probs",
        "secondary_field": "clave",
        "ticket_cost":   1.50,
    },
    "la-primitiva": {
        "draw_probs":    "la_primitiva_draw_probs",
        "compare":       "la_primitiva_compare_results",
        "feature":       "la_primitiva_feature",
        "tickets":       "la_primitiva_tickets",
        "secondary_key": "rein_probs",
        "secondary_field": "reintegro",
        "ticket_cost":   1.00,
    },
}


@app.get("/api/ranking/draws")
def api_ranking_draws(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
    limit: int = Query(20, ge=1, le=200),
    skip: int = Query(0, ge=0),
):
    """
    Return historical draws with jackpot position from compare_results.
    Sorted by date DESC. Used by the Ranking Dashboard table.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    cfg = _RANKING_LOTTERY_MAP.get(lottery)
    if not cfg:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}")

    coll = db[cfg["compare"]]
    total = coll.count_documents({"jackpot_position": {"$exists": True, "$ne": None}})
    cursor = coll.find(
        {"jackpot_position": {"$exists": True, "$ne": None}},
        projection={"_id": 0, "current_id": 1, "pre_id": 1, "date": 1,
                    "jackpot_position": 1, "earning": 1, "ticket_cost": 1,
                    "categories": 1, "source": 1},
    ).sort("date", -1).skip(skip).limit(limit)

    rows = []
    for doc in cursor:
        rows.append({
            "draw_id":          str(doc.get("current_id") or ""),
            "pre_id":           str(doc.get("pre_id") or ""),
            "date":             (doc.get("date") or "")[:10],
            "jackpot_position": doc.get("jackpot_position"),
            "earning":          doc.get("earning"),
            "ticket_cost":      doc.get("ticket_cost"),
            "source":           doc.get("source", "db"),
        })

    return JSONResponse(content={"rows": rows, "total": total, "skip": skip, "limit": limit})


@app.get("/api/ranking/chart")
def api_ranking_chart(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
    max_points: int = Query(200, ge=10, le=500),
):
    """
    Return jackpot position trend over time for charting.
    Returns up to max_points evenly sampled from all compare results.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    cfg = _RANKING_LOTTERY_MAP.get(lottery)
    if not cfg:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}")

    coll = db[cfg["compare"]]
    all_docs = list(coll.find(
        {"jackpot_position": {"$exists": True, "$ne": None}, "date": {"$exists": True}},
        projection={"_id": 0, "date": 1, "jackpot_position": 1, "current_id": 1},
    ).sort("date", 1))

    if not all_docs:
        return JSONResponse(content={"points": []})

    # Evenly sample up to max_points
    total = len(all_docs)
    if total <= max_points:
        sampled = all_docs
    else:
        step = total / max_points
        sampled = [all_docs[int(i * step)] for i in range(max_points)]

    points = [
        {
            "date":             (doc.get("date") or "")[:10],
            "jackpot_position": doc.get("jackpot_position"),
            "draw_id":          str(doc.get("current_id") or ""),
        }
        for doc in sampled
        if doc.get("jackpot_position") is not None
    ]

    return JSONResponse(content={"points": points, "total": total})


@app.get("/api/ranking/top-tickets")
def api_ranking_top_tickets(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
    limit: int = Query(20, ge=1, le=100),
    skip: int = Query(0, ge=0),
):
    """
    Return top predicted tickets from pre-computed ranking cache.
    Fast — reads from ranking_top_tickets collection, no scanning.
    Call POST /api/ranking/compute-top-tickets first to populate.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    cfg = _RANKING_LOTTERY_MAP.get(lottery)
    if not cfg:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}")

    coll = db["ranking_top_tickets"]
    total = coll.count_documents({"lottery": lottery})

    if total == 0:
        raise HTTPException(404, detail="No pre-computed top tickets found. Call POST /api/ranking/compute-top-tickets first.")

    docs = list(coll.find(
        {"lottery": lottery},
        projection={"_id": 0},
        sort=[("rank", 1)],
        skip=skip,
        limit=limit,
    ))

    # Get metadata from latest draw_probs
    probs_doc = db[cfg["draw_probs"]].find_one(sort=[("draw_date", -1)])
    draw_id   = str(probs_doc.get("draw_id") or "") if probs_doc else ""
    draw_date = str(probs_doc.get("draw_date") or "") if probs_doc else ""
    source    = str(probs_doc.get("source") or "") if probs_doc else ""

    return JSONResponse(content={
        "lottery":   lottery,
        "draw_id":   draw_id,
        "draw_date": draw_date,
        "source":    source,
        "total":     total,
        "skip":      skip,
        "limit":     limit,
        "tickets":   docs,
    })


@app.post("/api/ranking/compute-top-tickets")
def api_ranking_compute_top_tickets(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
    top_n: int = Query(1000, ge=10, le=10000, description="How many top tickets to pre-compute and store"),
):
    """
    Pre-compute top N tickets by scoring all tickets against latest probabilities.
    Stores results in ranking_top_tickets collection for fast retrieval.
    Runs in background — poll GET /api/ranking/top-tickets to check when ready.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    cfg = _RANKING_LOTTERY_MAP.get(lottery)
    if not cfg:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}")

    probs_doc = db[cfg["draw_probs"]].find_one(sort=[("draw_date", -1)])
    if not probs_doc:
        raise HTTPException(404, detail=f"No probability snapshot found for {lottery}")

    mains_probs = {int(k): float(v) for k, v in (probs_doc.get("mains_probs") or {}).items()}
    sec_probs   = {int(k): float(v) for k, v in (probs_doc.get(cfg["secondary_key"]) or {}).items()}

    if not mains_probs:
        raise HTTPException(404, detail="No probabilities available")

    draw_id   = str(probs_doc.get("draw_id") or "")
    draw_date = str(probs_doc.get("draw_date") or "")
    source    = str(probs_doc.get("source") or "")
    lottery_key = lottery.replace("-", "_")
    sec_field   = cfg["secondary_field"]

    def _run():
        import math as _math, heapq
        tickets_coll = db[cfg["tickets"]]
        heap = []

        def _score(mains, secondary):
            s = sum(_math.log(max(mains_probs.get(n, 1e-6), 1e-9)) for n in mains)
            if isinstance(secondary, list):
                s += sum(_math.log(max(sec_probs.get(x, 1e-6), 1e-9)) for x in secondary)
            else:
                s += _math.log(max(sec_probs.get(int(secondary), 1e-6), 1e-9))
            return s

        for doc in tickets_coll.find(
            {"lottery": lottery_key},
            projection={"_id": 0, "position": 1, "mains": 1, sec_field: 1, "tier": 1},
        ):
            secondary = doc.get(sec_field)
            if secondary is None:
                continue
            score = _score(doc["mains"], secondary)
            if len(heap) < top_n:
                heapq.heappush(heap, (score, doc["position"], doc))
            elif score > heap[0][0]:
                heapq.heapreplace(heap, (score, doc["position"], doc))

        top = sorted(heap, key=lambda x: x[0], reverse=True)

        # Clear old results and insert new
        coll = db["ranking_top_tickets"]
        coll.delete_many({"lottery": lottery})
        batch = []
        for rank, (score, _, doc) in enumerate(top, 1):
            t = {
                "lottery":   lottery,
                "rank":      rank,
                "position":  doc["position"],
                "mains":     doc["mains"],
                "score":     round(score, 6),
                "tier":      doc.get("tier", 0),
                "draw_id":   draw_id,
                "draw_date": draw_date,
                "source":    source,
            }
            t[sec_field] = doc.get(sec_field)
            batch.append(t)
            if len(batch) >= 500:
                coll.insert_many(batch, ordered=False)
                batch.clear()
        if batch:
            coll.insert_many(batch, ordered=False)

        # Create index for fast pagination
        coll.create_index([("lottery", 1), ("rank", 1)])
        logging.info("[ranking] compute-top-tickets done lottery=%s top_n=%d", lottery, len(top))

    t = threading.Thread(target=_run, daemon=True)
    t.start()

    return JSONResponse(content={
        "status":  "started",
        "lottery": lottery,
        "top_n":   top_n,
        "message": f"Computing top {top_n} tickets in background. Poll GET /api/ranking/top-tickets to check.",
    })

    return JSONResponse(content={
        "lottery":    lottery,
        "draw_id":    str(probs_doc.get("draw_id") or ""),
        "draw_date":  str(probs_doc.get("draw_date") or ""),
        "source":     str(probs_doc.get("source") or ""),
        "tickets":    tickets_out,
    })


@app.get("/api/ranking/study-progress")
def api_ranking_study_progress(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
    limit: int = Query(20, ge=1, le=100),
    skip: int = Query(0, ge=0),
):
    """
    Return draw_probs history — shows how model probabilities evolved draw by draw.
    Each row: draw_id, draw_date, source (ml_model/freq_gap),
    top 5 mains by probability, top 2 secondary by probability.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    cfg = _RANKING_LOTTERY_MAP.get(lottery)
    if not cfg:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}")

    coll = db[cfg["draw_probs"]]
    total = coll.count_documents({})

    docs = list(coll.find(
        {},
        projection={"_id": 0, "draw_id": 1, "draw_date": 1, "source": 1,
                    "mains_probs": 1, cfg["secondary_key"]: 1},
        sort=[("draw_date", -1)],
        skip=skip,
        limit=limit,
    ))

    rows = []
    for doc in docs:
        mains_raw = doc.get("mains_probs") or {}
        sec_raw   = doc.get(cfg["secondary_key"]) or {}

        # Sort by probability descending — return ALL numbers
        top_mains = sorted(
            [{"number": int(k), "p": round(float(v), 4)} for k, v in mains_raw.items()],
            key=lambda x: x["p"], reverse=True
        )
        top_sec = sorted(
            [{"number": int(k), "p": round(float(v), 4)} for k, v in sec_raw.items()],
            key=lambda x: x["p"], reverse=True
        )

        rows.append({
            "draw_id":    str(doc.get("draw_id") or ""),
            "draw_date":  str(doc.get("draw_date") or ""),
            "source":     str(doc.get("source") or ""),
            "top_mains":  top_mains,
            "top_secondary": top_sec,
            "secondary_label": cfg["secondary_field"],
        })

    return JSONResponse(content={"rows": rows, "total": total, "skip": skip, "limit": limit})


@app.get("/api/ranking/study-summary")
def api_ranking_study_summary(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
):
    """
    Summary of study progress:
    - total draws with probs
    - ml_model vs freq_gap count
    - first and latest draw dates
    - how many numbers have been seen (coverage)
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    cfg = _RANKING_LOTTERY_MAP.get(lottery)
    if not cfg:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}")

    coll = db[cfg["draw_probs"]]
    total      = coll.count_documents({})
    ml_count   = coll.count_documents({"source": "ml_model"})
    fg_count   = coll.count_documents({"source": "freq_gap"})

    first  = coll.find_one({}, sort=[("draw_date",  1)], projection={"draw_id": 1, "draw_date": 1, "source": 1})
    latest = coll.find_one({}, sort=[("draw_date", -1)], projection={"draw_id": 1, "draw_date": 1, "source": 1, "mains_probs": 1})

    # Top 10 most probable numbers from latest draw
    top_numbers: list = []
    if latest:
        mains_raw = latest.get("mains_probs") or {}
        top_numbers = sorted(
            [{"number": int(k), "p": round(float(v), 4)} for k, v in mains_raw.items()],
            key=lambda x: x["p"], reverse=True
        )[:10]

    return JSONResponse(content={
        "lottery":       lottery,
        "total_draws":   total,
        "ml_draws":      ml_count,
        "freq_gap_draws": fg_count,
        "ml_pct":        round(ml_count / total * 100, 1) if total else 0,
        "first_draw":    {"id": str(first.get("draw_id") or ""), "date": str(first.get("draw_date") or ""), "source": str(first.get("source") or "")} if first else None,
        "latest_draw":   {"id": str(latest.get("draw_id") or ""), "date": str(latest.get("draw_date") or ""), "source": str(latest.get("source") or "")} if latest else None,
        "top_numbers_latest": top_numbers,
    })


@app.post("/api/ranking/save-draw-probs")
def api_ranking_save_draw_probs(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
    draw_id: str = Query(..., description="id_sorteo to save probs for"),
):
    """
    Save probability snapshot for a specific draw_id to draw_probs collection.
    Reads from train_progress (ML model probs if available, else freq/gap from feature row).
    Called by daily automation after pipeline runs.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    cfg = _RANKING_LOTTERY_MAP.get(lottery)
    if not cfg:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}")

    draw_id_clean = draw_id.strip()
    probs_coll = db[cfg["draw_probs"]]

    # Map lottery slug to progress collection and field names
    progress_map = {
        "euromillones": ("euromillones_train_progress", "stars_probs",      "euromillones_feature", 50, 12, 50, 1),
        "el-gordo":     ("el_gordo_train_progress",     "clave_probs",      "el_gordo_feature",     54, 10, 54, 0),
        "la-primitiva": ("la_primitiva_train_progress", "reintegro_probs",  "la_primitiva_feature", 49, 10, 98, 0),
    }
    prog_coll_name, sec_field_ml, feat_coll_name, mc, sc, so, sb = progress_map[lottery]

    # Get draw date from feature collection
    feat_doc = db[feat_coll_name].find_one({"id_sorteo": draw_id_clean})
    if not feat_doc:
        # Try int id
        if draw_id_clean.isdigit():
            feat_doc = db[feat_coll_name].find_one({"id_sorteo": int(draw_id_clean)})
    draw_date = str(feat_doc.get("fecha_sorteo") or "").split(" ")[0] if feat_doc else ""
    source = "freq_gap"
    mains_probs: dict = {}
    sec_probs: dict = {}

    # Try ML probs from train_progress first
    prog_doc = db[prog_coll_name].find_one({"cutoff_draw_id": draw_id_clean})
    if prog_doc and prog_doc.get("mains_probs"):
        mains_raw = prog_doc.get("mains_probs") or []
        sec_raw   = prog_doc.get(sec_field_ml) or []
        mains_probs = {str(int(x["number"])): float(x.get("p", 0.0)) for x in mains_raw if x.get("number") is not None}
        sec_probs   = {str(int(x["number"])): float(x.get("p", 0.0)) for x in sec_raw   if x.get("number") is not None}
        if mains_probs:
            source = "ml_model"

    # Fall back to freq/gap from feature row
    if not mains_probs and feat_doc:
        frequency = list(feat_doc.get("frequency") or [])
        gap       = list(feat_doc.get("gap") or [])
        src_idx   = int(feat_doc.get("source_index", 0))

        def _build(offset: int, count: int, base: int) -> dict:
            freqs = [int(frequency[offset+i]) if offset+i < len(frequency) else 0 for i in range(count)]
            gaps  = [gap[offset+i] if offset+i < len(gap) else None for i in range(count)]
            mf = max(freqs) if any(f > 0 for f in freqs) else 1
            vg = [g for g in gaps if g is not None]
            mg = max(vg) + 1 if vg else 1
            return {
                str(base + i): max(0.4 * freqs[i]/mf + 0.6*(0.0 if gaps[i] is None else 1.0 - gaps[i]/mg), 1e-6)
                for i in range(count)
            }

        mains_probs = _build(0, mc, 1)
        sec_probs   = _build(so, sc, sb)
        source = "freq_gap"

    if not mains_probs:
        raise HTTPException(404, detail=f"No probs available for draw_id={draw_id_clean}")

    probs_coll.replace_one(
        {"draw_id": draw_id_clean},
        {
            "draw_id":              draw_id_clean,
            "draw_date":            draw_date,
            "saved_at":             dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "mains_probs":          mains_probs,
            cfg["secondary_key"]:   sec_probs,
            "source":               source,
        },
        upsert=True,
    )

    return JSONResponse(content={
        "draw_id":   draw_id_clean,
        "draw_date": draw_date,
        "source":    source,
        "mains_count": len(mains_probs),
        "sec_count":   len(sec_probs),
    })


# ═══════════════════════════════════════════════════════════════════════════════
# ITEM 2 — Online Learning API endpoints
# ═══════════════════════════════════════════════════════════════════════════════

def _import_online_learning():
    """Lazy import of online_learning module (avoids circular import at startup)."""
    import importlib
    import sys as _sys
    if "online_learning" not in _sys.modules:
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "online_learning",
            os.path.join(_SCRIPTS_DIR, "online_learning.py"),
        )
        mod = importlib.util.module_from_spec(spec)
        _sys.modules["online_learning"] = mod
        spec.loader.exec_module(mod)
    return _sys.modules["online_learning"]


@app.post("/api/online-learning/pre-draw")
def api_online_learning_pre_draw(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
    cutoff_draw_id: str = Query(..., description="Draw ID to snapshot model state for"),
):
    """
    Pre-draw step: generate .orc binary model snapshot + SHA-256 hash.

    Call this AFTER the full wheel TXT has been generated so both files
    can share the same validation hash.

    Stores metadata in MongoDB model_orc_snapshots collection.
    Returns: {orc_path, orc_hash, txt_hash, draw_id, lottery, created_at}
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    ol = _import_online_learning()
    valid_lotteries = list(ol.LOTTERY_CONFIG.keys())
    if lottery not in valid_lotteries:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}. Valid: {valid_lotteries}")

    try:
        result = ol.pre_draw(lottery=lottery, cutoff_draw_id=cutoff_draw_id.strip())
        return JSONResponse(content=result)
    except Exception as e:
        logger.exception("pre-draw ORC failed: %s", e)
        raise HTTPException(500, detail=str(e))


@app.post("/api/online-learning/post-draw")
def api_online_learning_post_draw(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
    current_id: str = Query(..., description="Draw ID of the draw that just happened"),
    pre_id: str = Query(..., description="Draw ID used to generate the pre-draw ORC snapshot"),
):
    """
    Post-draw feedback loop — continuous learning step.

    Steps:
    1. Load .orc snapshot from pre_id (model state before the draw)
    2. Read actual draw result from MongoDB
    3. Compute error = jackpot_position / total_tickets (0=perfect, 1=worst)
    4. Warm-start GBM: add 10 estimators weighted toward winning numbers
    5. Save updated model files + generate new .orc for next draw cycle
    6. Store feedback record in model_feedback_log collection

    The model accumulates knowledge with each draw WITHOUT reviewing old data —
    knowledge is encoded in the weights.

    Returns: {lottery, draw_id, pre_draw_id, error_rate, updated_at, feedback_records}
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    ol = _import_online_learning()
    valid_lotteries = list(ol.LOTTERY_CONFIG.keys())
    if lottery not in valid_lotteries:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}. Valid: {valid_lotteries}")

    try:
        result = ol.post_draw(
            lottery=lottery,
            current_id=current_id.strip(),
            pre_id=pre_id.strip(),
        )
        # Remove large binary fields before returning
        safe = {k: v for k, v in result.items() if k not in ("feedback_records",)}
        safe["feedback_records"] = result.get("feedback_records", [])
        return JSONResponse(content=safe)
    except Exception as e:
        logger.exception("post-draw feedback failed: %s", e)
        raise HTTPException(500, detail=str(e))


@app.post("/api/online-learning/validate-hashes")
def api_online_learning_validate_hashes(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
    draw_id: str = Query(..., description="Draw ID to validate"),
):
    """
    Validate that the .orc and .txt files on disk still match their stored SHA-256 hashes.

    Returns: {valid, orc_match, txt_match, details, draw_id, lottery}
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    ol = _import_online_learning()
    valid_lotteries = list(ol.LOTTERY_CONFIG.keys())
    if lottery not in valid_lotteries:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}. Valid: {valid_lotteries}")

    try:
        result = ol.validate_hashes(lottery=lottery, draw_id=draw_id.strip())
        return JSONResponse(content=result)
    except Exception as e:
        logger.exception("validate-hashes failed: %s", e)
        raise HTTPException(500, detail=str(e))


@app.get("/api/online-learning/history")
def api_online_learning_history(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
    limit: int = Query(50, ge=1, le=500),
    skip: int = Query(0, ge=0),
):
    """
    Learning history aligned with compare results (newest first).

    Each row is one draw with a compare result. Rows with completed post-draw feedback
    include model update details; others show as ``feedback_status=pending`` until the
    feedback loop runs (automatically after compare when an ORC snapshot exists).
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    if lottery not in _VALIDATION_TOTAL_TICKETS:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}")

    compare_coll = db[_validation_compare_collection(lottery)]
    total_tickets = _VALIDATION_TOTAL_TICKETS[lottery]
    compare_filt = _compare_rows_filter()
    total = compare_coll.count_documents(compare_filt)

    compares_raw, _ = _fetch_compare_rows_page(
        compare_coll,
        skip,
        limit,
        projection={
            "_id": 0,
            "current_id": 1,
            "pre_id": 1,
            "date": 1,
            "jackpot_position": 1,
            "error_rate": 1,
            "updated_at": 1,
        },
    )

    draw_ids = [str(c.get("current_id") or "").strip() for c in compares_raw if c.get("current_id")]
    feedback_id_query: List[Any] = []
    _seen_fb_ids: set = set()
    for did in draw_ids:
        for v in _draw_id_variants(did):
            key = (type(v).__name__, v)
            if key not in _seen_fb_ids:
                _seen_fb_ids.add(key)
                feedback_id_query.append(v)
    feedback_by_draw: Dict[str, dict] = {}
    if feedback_id_query:
        for fb in db["model_feedback_log"].find(
            {"lottery": lottery, "draw_id": {"$in": feedback_id_query}},
            projection={
                "_id": 0,
                "draw_id": 1,
                "pre_draw_id": 1,
                "draw_date": 1,
                "error_rate": 1,
                "updated_at": 1,
                "actual_jackpot_position": 1,
                "feedback_records": 1,
                "new_orc_hash": 1,
            },
        ):
            fb_json = _item_to_json(fb)
            did = str(fb_json.get("draw_id") or "").strip()
            feedback_by_draw[did] = fb_json
            if did.isdigit():
                feedback_by_draw[str(int(did))] = fb_json

    rows: list[dict] = []
    for c in compares_raw:
        rows.append(
            _compare_doc_to_learning_row(
                db, lottery, c, feedback_by_draw, total_tickets
            )
        )

    if skip == 0:
        rows = _prepend_latest_feature_gaps(db, lottery, rows, feedback_by_draw)

    rows = _enrich_feedback_rows(db, lottery, rows)
    diag = _learning_history_diagnostics(db, lottery)

    return JSONResponse(content={
        "lottery": lottery,
        "total": total,
        "rows": rows,
        "skip": skip,
        "limit": limit,
        "diagnostics": diag,
    })


@app.post("/api/online-learning/sync-pending")
def api_online_learning_sync_pending(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
    last: int = Query(30, ge=1, le=100),
    run_compare: bool = Query(
        False,
        description="Run full-wheel compare when missing (needs train progress + TXT; not for old data)",
    ),
):
    """
    Refresh learning/validation fields from **existing** compare results.

    Default (``run_compare=false``): only reads Mongo compare rows — no training
    progress or full-wheel TXT required (correct for old historical data).

    For each recent feature draw pair:
    - Finds compare by (current_id, pre_id), or by current_id only for legacy rows
    - Re-saves validation metrics and schedules feedback if ORC exists
    - Skips pairs with no compare (does not error)

    Set ``run_compare=true`` only for new draws that have train progress + wheel file.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if lottery not in _VALIDATION_TOTAL_TICKETS:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}")

    pairs = _load_recent_draw_pairs(db, lottery, last)
    if not pairs:
        raise HTTPException(404, detail=f"No draw pairs found for {lottery}")

    compare_coll = db[_validation_compare_collection(lottery)]
    compare_runners = {
        "euromillones": _euromillones_full_wheel_compare,
        "el-gordo": _el_gordo_full_wheel_compare,
        "la-primitiva": _la_primitiva_full_wheel_compare,
    }
    runner = compare_runners[lottery]

    refreshed = 0
    compared = 0
    skipped_no_compare = 0
    feedback_scheduled = 0
    errors: List[str] = []

    for pair in pairs:
        current_id = pair["current_id"]
        pre_id = pair["pre_id"]
        label = f"{current_id} (pre={pre_id})"
        try:
            doc = _find_compare_for_draw(
                compare_coll,
                current_id,
                pre_id,
                allow_current_id_only=not run_compare,
            )
            if doc is None and run_compare:
                try:
                    runner(current_id, pre_id, db)
                    compared += 1
                    doc = _find_compare_doc(compare_coll, current_id, pre_id)
                except HTTPException as exc:
                    errors.append(f"{label}: compare — {exc.detail}")
                    continue
                except Exception as exc:
                    errors.append(f"{label}: compare — {exc}")
                    continue
            elif doc is None:
                skipped_no_compare += 1
                continue

            if doc:
                effective_pre_id = str(doc.get("pre_id") or pre_id).strip()
                before_fb = _feedback_log_exists(db, lottery, current_id)
                _save_compare_result(db, lottery, doc, touch_updated_at=False)
                refreshed += 1
                if not before_fb and _orc_exists_for_pre_draw(db, lottery, effective_pre_id):
                    feedback_scheduled += 1
        except Exception as exc:
            errors.append(f"{label}: {exc}")

    compare_filt = _compare_rows_filter()
    total_compares = compare_coll.count_documents(compare_filt)
    feedback_count = db["model_feedback_log"].count_documents({"lottery": lottery})

    return JSONResponse(
        content={
            "lottery": lottery,
            "pairs_checked": len(pairs),
            "refreshed": refreshed,
            "compared_new": compared,
            "skipped_no_compare": skipped_no_compare,
            "feedback_scheduled": feedback_scheduled,
            "errors": errors,
            "diagnostics": {
                "compare_count": total_compares,
                "feedback_count": feedback_count,
                "pending_feedback": max(0, total_compares - feedback_count),
            },
        }
    )


_TRAIN_PROGRESS_COLLECTION_BY_LOTTERY: Dict[str, str] = {
    "euromillones": EUROMILLONES_TRAIN_PROGRESS_COLLECTION,
    "el-gordo": EL_GORDO_TRAIN_PROGRESS_COLLECTION,
    "la-primitiva": LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION,
}

_repair_feedback_busy: set[str] = set()
_repair_feedback_lock = threading.Lock()


def _train_progress_doc(db_instance, lottery: str, cutoff_draw_id: str) -> Optional[Dict[str, Any]]:
    coll = db_instance[_TRAIN_PROGRESS_COLLECTION_BY_LOTTERY[lottery]]
    for variant in _draw_id_variants(cutoff_draw_id):
        doc = coll.find_one({"cutoff_draw_id": variant})
        if doc:
            return doc
    return None


def _train_pipeline_ready(doc: Optional[Dict[str, Any]]) -> bool:
    if not doc:
        return False
    if doc.get("rules_applied"):
        return True
    return str(doc.get("pipeline_status") or "").lower() == "done"


def _wait_train_pipeline_done(
    db_instance,
    lottery: str,
    cutoff_draw_id: str,
    *,
    timeout_seconds: int = 45 * 60,
) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        doc = _train_progress_doc(db_instance, lottery, cutoff_draw_id)
        if doc and _train_pipeline_ready(doc):
            return
        if doc and str(doc.get("pipeline_status") or "").lower() == "error":
            raise RuntimeError(str(doc.get("pipeline_error") or "train pipeline error"))
        time.sleep(8)
    raise RuntimeError(f"Train pipeline timeout for cutoff={cutoff_draw_id}")


def _start_train_pipeline(lottery: str, cutoff_draw_id: str) -> None:
    cid = cutoff_draw_id.strip()
    if lottery == "euromillones":
        api_euromillones_run_pipeline(cutoff_draw_id=cid)
    elif lottery == "el-gordo":
        api_el_gordo_run_pipeline(cutoff_draw_id=cid)
    elif lottery == "la-primitiva":
        api_la_primitiva_run_pipeline(cutoff_draw_id=cid)
    else:
        raise ValueError(f"Unknown lottery: {lottery}")


def _ensure_orc_for_pre_draw(
    db_instance,
    lottery: str,
    pre_id: str,
    *,
    ensure_train: bool,
) -> None:
    """Create ORC snapshot for pre_id when missing (train if needed, no full wheel)."""
    if _orc_exists_for_pre_draw(db_instance, lottery, pre_id):
        return

    doc = _train_progress_doc(db_instance, lottery, pre_id)
    if not _train_pipeline_ready(doc):
        if not ensure_train:
            raise RuntimeError(
                f"No ORC for pre_id={pre_id} and train pipeline not ready "
                f"(set ensure_train=true or run train for this cutoff first)"
            )
        _start_train_pipeline(lottery, pre_id)
        _wait_train_pipeline_done(db_instance, lottery, pre_id)

    api_ranking_save_draw_probs(lottery=lottery, draw_id=pre_id)
    ol = _import_online_learning()
    ol.pre_draw(lottery=lottery, cutoff_draw_id=pre_id.strip())
    if not _orc_exists_for_pre_draw(db_instance, lottery, pre_id):
        raise RuntimeError(f"pre_draw did not create ORC for pre_id={pre_id}")


def _run_repair_feedback_chain(
    db_instance,
    lottery: str,
    pairs: List[Dict[str, str]],
    *,
    ensure_train: bool,
) -> Dict[str, Any]:
    """
    Process draw pairs oldest-first so each post-draw feedback creates the ORC
    the next draw needs (pre_id chain).
    """
    compare_coll = db_instance[_validation_compare_collection(lottery)]
    ol = _import_online_learning()
    repaired = 0
    skipped_complete = 0
    skipped_no_compare = 0
    orc_created = 0
    errors: List[str] = []

    for pair in pairs:
        current_id = pair["current_id"]
        pre_id = pair["pre_id"]
        label = f"{current_id} (pre={pre_id})"

        if _feedback_log_exists(db_instance, lottery, current_id):
            skipped_complete += 1
            continue

        doc = _find_compare_for_draw(
            compare_coll,
            current_id,
            pre_id,
            allow_current_id_only=True,
        )
        if not doc:
            skipped_no_compare += 1
            continue

        effective_pre_id = str(doc.get("pre_id") or pre_id).strip()
        try:
            had_orc = _orc_exists_for_pre_draw(db_instance, lottery, effective_pre_id)
            if not had_orc:
                _ensure_orc_for_pre_draw(
                    db_instance,
                    lottery,
                    effective_pre_id,
                    ensure_train=ensure_train,
                )
                orc_created += 1

            ol.post_draw(
                lottery=lottery,
                current_id=current_id,
                pre_id=effective_pre_id,
            )
            _save_compare_result(
                db_instance,
                lottery,
                doc,
                touch_updated_at=False,
            )
            repaired += 1
            logging.info("[repair-feedback] completed %s %s", lottery, label)
        except Exception as exc:
            logging.exception("[repair-feedback] failed %s: %s", label, exc)
            errors.append(f"{label}: {exc}")

    return {
        "repaired": repaired,
        "orc_created": orc_created,
        "skipped_complete": skipped_complete,
        "skipped_no_compare": skipped_no_compare,
        "errors": errors,
    }


@app.post("/api/online-learning/repair-feedback")
def api_online_learning_repair_feedback(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
    last: int = Query(24, ge=1, le=50),
    ensure_train: bool = Query(
        True,
        description="When ORC is missing for pre_id, run train pipeline + pre-draw ORC (no full wheel)",
    ),
    background: bool = Query(
        True,
        description="Run in background thread (recommended — train+feedback can take many minutes)",
    ),
):
    """
    Backfill learning history for recent draws with compare but no feedback.

    Processes pairs **oldest first** so ORC snapshots chain correctly:
    feedback for draw N creates ORC(N), which draw N+1 needs as pre_id.

    Fixes **Compare only (legacy)** and **Pending learning** rows in learning history.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if lottery not in _VALIDATION_TOTAL_TICKETS:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}")

    pairs = _load_recent_draw_pairs(db, lottery, last)
    if not pairs:
        raise HTTPException(404, detail=f"No draw pairs found for {lottery}")

    with _repair_feedback_lock:
        if lottery in _repair_feedback_busy:
            raise HTTPException(
                503,
                detail=f"Repair already running for {lottery}. Wait and refresh learning history.",
            )

    if not background:
        with _repair_feedback_lock:
            _repair_feedback_busy.add(lottery)
        try:
            result = _run_repair_feedback_chain(db, lottery, pairs, ensure_train=ensure_train)
            return JSONResponse(content={"lottery": lottery, "pairs": len(pairs), **result})
        finally:
            with _repair_feedback_lock:
                _repair_feedback_busy.discard(lottery)

    def _run() -> None:
        with _repair_feedback_lock:
            _repair_feedback_busy.add(lottery)
        try:
            result = _run_repair_feedback_chain(db, lottery, pairs, ensure_train=ensure_train)
            logging.info("[repair-feedback] %s finished: %s", lottery, result)
        except Exception as exc:
            logging.exception("[repair-feedback] %s failed: %s", lottery, exc)
        finally:
            with _repair_feedback_lock:
                _repair_feedback_busy.discard(lottery)

    threading.Thread(
        target=_run,
        daemon=True,
        name=f"repair-feedback-{lottery}",
    ).start()

    return JSONResponse(
        status_code=202,
        content={
            "status": "started",
            "lottery": lottery,
            "pairs": len(pairs),
            "message": (
                f"Repair running for last {len(pairs)} draw pair(s) (oldest first). "
                "Refresh learning history in a few minutes. Check logs: journalctl -u lottery-backend -f"
            ),
        },
    )


@app.get("/api/online-learning/orc-snapshots")
def api_online_learning_orc_snapshots(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
    limit: int = Query(20, ge=1, le=200),
    skip: int = Query(0, ge=0),
):
    """
    Return ORC snapshot metadata for a lottery (newest first).
    Used by the validation dashboard hash panel.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["model_orc_snapshots"]
    total = coll.count_documents({"lottery": lottery})
    rows_raw = list(
        coll.find(
            {"lottery": lottery},
            projection={"_id": 0, "lottery": 1, "draw_id": 1, "orc_hash": 1,
                        "txt_hash": 1, "orc_path": 1, "txt_path": 1, "created_at": 1},
            sort=[("created_at", -1)],
            skip=skip,
            limit=limit,
        )
    )
    rows = [_item_to_json(r) for r in rows_raw]
    return JSONResponse(content={"lottery": lottery, "total": total, "rows": rows})


# ═══════════════════════════════════════════════════════════════════════════════
# ITEM 3 — Validation Dashboard API endpoints
# ═══════════════════════════════════════════════════════════════════════════════


def _compare_difference(special: Any, first: Any) -> Optional[int]:
    """Client metric: Special position minus 1st-tier position (when both are valid ranks)."""
    try:
        s = int(special) if special is not None else 0
        f = int(first) if first is not None else 0
    except (TypeError, ValueError):
        return None
    if s <= 0 or f <= 0:
        return None
    return s - f


def _tier1_position_from_row(row: Dict[str, Any]) -> Optional[int]:
    """El Gordo: 1ª (5+1) — pos_1th / jackpot."""
    for key in ("pos_1th", "jackpot_position", "special_position"):
        v = row.get(key)
        if isinstance(v, int) and v > 0:
            return v
        if v is not None and str(v).strip():
            try:
                n = int(v)
                if n > 0:
                    return n
            except (TypeError, ValueError):
                pass
    return None


def _tier1_special_position_from_row(row: Dict[str, Any]) -> Optional[int]:
    """Euromillones / La Primitiva: Special (jackpot) tier for diff column."""
    for key in ("special_position", "jackpot_position", "pos_1th"):
        v = row.get(key)
        if isinstance(v, int) and v > 0:
            return v
        if v is not None and str(v).strip():
            try:
                n = int(v)
                if n > 0:
                    return n
            except (TypeError, ValueError):
                pass
    return None


def _draw_id_lookup_variants(draw_id: str) -> List[Any]:
    """Mongo keys for id_sorteo may be stored as str or int."""
    s = str(draw_id or "").strip()
    if not s:
        return []
    variants: List[Any] = [s]
    if s.isdigit():
        variants.append(int(s))
    return variants


def _annotate_tier1_differences_via_pre_id(
    db_instance,
    collection_name: str,
    rows: List[Dict[str, Any]],
    *,
    map_doc_to_row: Any,
    tier1_from_row: Any = None,
) -> List[Dict[str, Any]]:
    """
    Set difference_prev_special = current 1ª minus previous draw 1ª.

    Uses each row's pre_id (previous draw) and looks up that draw in compare_results,
    so pagination does not leave gaps at page boundaries.
    """
    if not rows or db_instance is None:
        return rows

    extract_tier1 = tier1_from_row or _tier1_position_from_row
    coll = db_instance[collection_name]
    lookup_ids: List[Any] = []
    seen: set[str] = set()
    for r in rows:
        for v in _draw_id_lookup_variants(str(r.get("pre_id") or "")):
            sig = f"{type(v).__name__}:{v}"
            if sig not in seen:
                seen.add(sig)
                lookup_ids.append(v)

    tier1_by_current: Dict[str, int] = {}
    if lookup_ids:
        for doc in coll.find({"current_id": {"$in": lookup_ids}}):
            mapped = map_doc_to_row(doc)
            cid = str(doc.get("current_id") or "")
            t1 = extract_tier1(mapped)
            if t1:
                tier1_by_current[cid] = t1
                for v in _draw_id_lookup_variants(cid):
                    tier1_by_current[str(v)] = t1

    for r in rows:
        curr = extract_tier1(r)
        pid = str(r.get("pre_id") or "").strip()
        prev: Optional[int] = None
        if pid:
            prev = tier1_by_current.get(pid)
            if prev is None:
                for v in _draw_id_lookup_variants(pid):
                    prev = tier1_by_current.get(str(v))
                    if prev is not None:
                        break
        if prev is None and (r.get("date") or ""):
            date_str = (r.get("date") or "")[:10]
            prev_doc = coll.find_one(
                {"date": {"$lt": date_str}},
                sort=[("date", -1)],
                projection={"_id": 0},
            )
            if prev_doc:
                prev = extract_tier1(map_doc_to_row(prev_doc))

        r["prev_special_position"] = prev
        r["difference_prev_special"] = (
            int(curr) - int(prev) if curr and prev else None
        )
    return rows


def _euromillones_analysis_row_from_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    date_str = (doc.get("date") or "")[:10]
    second_positions = doc.get("second_positions") or []
    third_positions = doc.get("third_positions") or []
    fourth_positions = doc.get("fourth_positions") or []
    special_pos = int(doc.get("jackpot_position") or 0) or None
    pos_5p0 = int(third_positions[0]) if third_positions else None
    pos_5p1 = int(second_positions[0]) if second_positions else None
    pos_4p2 = int(fourth_positions[0]) if fourth_positions else None
    return {
        "date": date_str,
        "current_id": str(doc.get("current_id") or ""),
        "pre_id": str(doc.get("pre_id") or ""),
        "special_position": special_pos,
        "jackpot_position": special_pos,
        "pos_1th": pos_5p0 if pos_5p0 and pos_5p0 > 0 else 0,
        "pos_2th": pos_5p1,
        "pos_3th": pos_4p2,
        "pos_4th": None,
    }


def _la_primitiva_analysis_row_from_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    date_str = (doc.get("date") or "")[:10]
    cats = doc.get("categories") or []
    cat_by_key: Dict[Tuple[int, int], int] = {}
    if isinstance(cats, list):
        for c in cats:
            if not isinstance(c, dict):
                continue
            hm = int(c.get("main_hits") or 0)
            rh = int(c.get("reintegro_hit") or 0)
            fp = int(c.get("first_position") or 0)
            cat_by_key[(hm, rh)] = fp
    positions = [cat_by_key.get(key) for key in _LA_PRIMITIVA_ANALYSIS_KEYS]
    special_pos = int(doc.get("special_position") or 0) or None
    pos_1th = positions[0] if positions[0] is not None and positions[0] > 0 else 0
    return {
        "date": date_str,
        "current_id": str(doc.get("current_id") or ""),
        "pre_id": str(doc.get("pre_id") or ""),
        "jackpot_position": int(doc.get("jackpot_position") or 0) or None,
        "special_position": special_pos,
        "pos_1th": pos_1th,
        "pos_2th": positions[1] if positions[1] and positions[1] > 0 else None,
        "pos_3th": positions[2] if positions[2] and positions[2] > 0 else None,
        "pos_4th": positions[3] if positions[3] and positions[3] > 0 else None,
        "pos_5th": positions[4] if positions[4] and positions[4] > 0 else None,
    }


def _annotate_special_draw_differences(
    rows: List[Dict[str, Any]],
    *,
    order: str,
) -> List[Dict[str, Any]]:
    """
    Legacy: adjacent-row diff only (kept for internal samples). Prefer
    _annotate_tier1_differences_via_pre_id for paginated analysis APIs.
    """
    if not rows:
        return rows

    if order == "desc":
        for i, row in enumerate(rows):
            curr = _tier1_position_from_row(row)
            nxt = _tier1_position_from_row(rows[i + 1]) if i + 1 < len(rows) else None
            row["prev_special_position"] = nxt
            row["difference_prev_special"] = (
                int(curr) - int(nxt) if curr and nxt else None
            )
    else:
        prev_val: Optional[int] = None
        for row in rows:
            curr = _tier1_position_from_row(row)
            row["prev_special_position"] = prev_val
            row["difference_prev_special"] = (
                int(curr) - int(prev_val) if curr and prev_val else None
            )
            prev_val = curr
    return rows


def _compare_coll_for_lottery(lottery: str) -> str:
    return {
        "euromillones": EUROMILLONES_COMPARE_RESULTS_COLLECTION,
        "el-gordo":     EL_GORDO_COMPARE_RESULTS_COLLECTION,
        "la-primitiva": LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION,
    }[lottery]


def _draw_coll_for_lottery(lottery: str) -> str:
    return {
        "euromillones": "euromillones",
        "el-gordo":     "el_gordo",
        "la-primitiva": "la_primitiva",
    }[lottery]


def _resolve_draw_dates(db, lottery: str, draw_ids: list[str]) -> dict[str, str]:
    """Map draw_id → YYYY-MM-DD from compare results, then draw collections."""
    ids = [str(d).strip() for d in draw_ids if str(d).strip()]
    if not ids:
        return {}

    dates: dict[str, str] = {}
    compare_coll = db[_compare_coll_for_lottery(lottery)]
    for doc in compare_coll.find(
        {"current_id": {"$in": ids}},
        projection={"current_id": 1, "date": 1},
    ):
        did = str(doc.get("current_id") or "").strip()
        d = str(doc.get("date") or "").strip()[:10]
        if did and d and did not in dates:
            dates[did] = d

    missing = [d for d in ids if d not in dates]
    if missing:
        draw_coll = db[_draw_coll_for_lottery(lottery)]
        id_variants: list = list(missing)
        for d in missing:
            if d.isdigit():
                id_variants.append(int(d))
        for doc in draw_coll.find(
            {"id_sorteo": {"$in": id_variants}},
            projection={"id_sorteo": 1, "fecha_sorteo": 1},
        ):
            did = str(doc.get("id_sorteo") or "").strip()
            d = str(doc.get("fecha_sorteo") or "").strip()[:10]
            if did and d:
                dates[did] = d

    return dates


def _enrich_feedback_rows(db, lottery: str, rows: list[dict]) -> list[dict]:
    missing_ids = [
        str(r.get("draw_id") or "").strip()
        for r in rows
        if not str(r.get("draw_date") or "").strip()[:10]
    ]
    lookup = _resolve_draw_dates(db, lottery, missing_ids)
    enriched: list[dict] = []
    for r in rows:
        row = dict(r)
        did = str(row.get("draw_id") or "").strip()
        existing = str(row.get("draw_date") or "").strip()[:10]
        row["draw_date"] = existing or lookup.get(did, "")
        enriched.append(row)
    return enriched


_VALIDATION_TOTAL_TICKETS: Dict[str, int] = {
    "euromillones": 139_838_160,
    "el-gordo": 31_625_100,
    "la-primitiva": 139_838_160,
}


def _validation_compare_collection(lottery: str) -> str:
    return {
        "euromillones": EUROMILLONES_COMPARE_RESULTS_COLLECTION,
        "el-gordo": EL_GORDO_COMPARE_RESULTS_COLLECTION,
        "la-primitiva": LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION,
    }[lottery]


def _find_compare_for_draw(
    coll,
    current_id: str,
    pre_id: str,
    *,
    allow_current_id_only: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Load a compare row for learning/validation sync.

    Tries exact (current_id, pre_id) first. Old data may have been saved with a
    different pre_id than today's feature row — when ``allow_current_id_only``,
    fall back to the newest compare for ``current_id`` only (no training progress).
    """
    doc = _find_compare_doc(coll, current_id, pre_id)
    if doc:
        return doc
    if not allow_current_id_only:
        return None
    filt = _compare_rows_filter()
    for ck in _draw_id_variants(current_id):
        doc = coll.find_one(
            {**filt, "current_id": ck},
            sort=_compare_chronological_sort(),
        )
        if doc:
            return doc
    return None


def _feedback_log_exists(db_instance, lottery: str, draw_id: str) -> bool:
    variants = _draw_id_variants(draw_id)
    if not variants:
        return False
    return (
        db_instance["model_feedback_log"].find_one(
            {"lottery": lottery, "draw_id": {"$in": variants}},
            projection={"_id": 1},
        )
        is not None
    )


def _load_recent_draw_pairs(db_instance, lottery: str, last_n: int) -> List[Dict[str, str]]:
    feature_map = {
        "euromillones": "euromillones_feature",
        "el-gordo": "el_gordo_feature",
        "la-primitiva": "la_primitiva_feature",
    }
    coll_name = feature_map.get(lottery)
    if not coll_name:
        return []
    docs = list(
        db_instance[coll_name]
        .find(
            {},
            projection={
                "id_sorteo": 1,
                "pre_id_sorteo": 1,
                "fecha_sorteo": 1,
                "source_index": 1,
            },
        )
        .sort("source_index", 1)
    )
    if len(docs) < 2:
        return []
    pairs: List[Dict[str, str]] = []
    for i in range(1, len(docs)):
        cur, prev = docs[i], docs[i - 1]
        current_id = str(cur.get("id_sorteo") or "").strip()
        pre_id = str(cur.get("pre_id_sorteo") or prev.get("id_sorteo") or "").strip()
        if not current_id or not pre_id:
            continue
        fecha = str(cur.get("fecha_sorteo") or "").strip()
        pairs.append(
            {
                "current_id": current_id,
                "pre_id": pre_id,
                "draw_date": fecha.split()[0][:10] if fecha else "",
            }
        )
    return pairs[-last_n:] if last_n > 0 else pairs


def _latest_feature_draw(db_instance, lottery: str) -> Optional[Dict[str, str]]:
    pairs = _load_recent_draw_pairs(db_instance, lottery, 1)
    return pairs[-1] if pairs else None


def _orc_exists_for_pre_draw(db_instance, lottery: str, pre_id: str) -> bool:
    variants = _draw_id_variants(pre_id)
    if not variants:
        return False
    return (
        db_instance["model_orc_snapshots"].find_one(
            {"lottery": lottery, "draw_id": {"$in": variants}},
            projection={"_id": 1},
        )
        is not None
    )


def _learning_feedback_status(
    db_instance,
    lottery: str,
    *,
    draw_id: str,
    pre_id: str,
    has_feedback: bool,
) -> str:
    """
    complete — feedback log exists (model weights updated).
    pending — compare exists, ORC exists, feedback not run yet (new pipeline).
    legacy — old compare only (no ORC); error rate from compare is still valid.
    """
    if has_feedback:
        return "complete"
    if _orc_exists_for_pre_draw(db_instance, lottery, pre_id):
        return "pending"
    return "legacy"


def _learning_history_diagnostics(db_instance, lottery: str) -> dict:
    compare_coll = db_instance[_validation_compare_collection(lottery)]
    filt = _compare_rows_filter()
    latest_feature = _latest_feature_draw(db_instance, lottery)
    latest_compare_doc = compare_coll.find_one(
        filt,
        projection={"current_id": 1, "date": 1, "pre_id": 1},
        sort=_compare_chronological_sort(),
    )
    latest_feature_has_compare = False
    if latest_feature:
        latest_feature_has_compare = (
            _find_compare_for_draw(
                compare_coll,
                latest_feature["current_id"],
                latest_feature["pre_id"],
                allow_current_id_only=True,
            )
            is not None
        )
    feedback_count = db_instance["model_feedback_log"].count_documents({"lottery": lottery})
    compare_count = compare_coll.count_documents(filt)
    return {
        "compare_count": compare_count,
        "orc_count": db_instance["model_orc_snapshots"].count_documents({"lottery": lottery}),
        "feedback_count": feedback_count,
        "pending_feedback": max(0, compare_count - feedback_count),
        "latest_feature_draw": latest_feature,
        "latest_feature_has_compare": latest_feature_has_compare,
        "latest_compare_in_db": (
            {
                "draw_id": str(latest_compare_doc.get("current_id") or ""),
                "draw_date": str(latest_compare_doc.get("date") or ""),
                "pre_id": str(latest_compare_doc.get("pre_id") or ""),
            }
            if latest_compare_doc
            else None
        ),
    }


def _compare_doc_to_learning_row(
    db_instance,
    lottery: str,
    c: dict,
    feedback_by_draw: Dict[str, dict],
    total_tickets: int,
) -> dict:
    cid = str(c.get("current_id") or "").strip()
    pid = str(c.get("pre_id") or "").strip()
    jp = int(c.get("jackpot_position") or 0)
    fb = feedback_by_draw.get(cid)
    if fb:
        row = dict(fb)
        row["pre_draw_id"] = str(row.get("pre_draw_id") or pid)
        row["feedback_status"] = "complete"
        return row
    er = c.get("error_rate")
    if er is None and jp > 0:
        er = round(jp / total_tickets, 6)
    return {
        "draw_id": cid,
        "pre_draw_id": pid,
        "draw_date": str(c.get("date") or ""),
        "actual_jackpot_position": jp,
        "error_rate": float(er) if er is not None else 0.0,
        "updated_at": str(c.get("updated_at") or ""),
        "feedback_records": [],
        "new_orc_hash": "",
        "feedback_status": _learning_feedback_status(
            db_instance, lottery, draw_id=cid, pre_id=pid, has_feedback=False
        ),
    }


def _prepend_latest_feature_gaps(
    db_instance,
    lottery: str,
    rows: list[dict],
    feedback_by_draw: Dict[str, dict],
    *,
    check_last_n: int = 5,
) -> list[dict]:
    """On page 1, show newest feature draws even when compare or feedback is missing."""
    pairs = _load_recent_draw_pairs(db_instance, lottery, check_last_n)
    if not pairs:
        return rows
    compare_coll = db_instance[_validation_compare_collection(lottery)]
    total_tickets = _VALIDATION_TOTAL_TICKETS[lottery]
    seen = {str(r.get("draw_id") or "").strip() for r in rows}
    prepend: list[dict] = []
    for pair in reversed(pairs):
        cid = pair["current_id"]
        if cid in seen:
            continue
        doc = _find_compare_for_draw(
            compare_coll, cid, pair["pre_id"], allow_current_id_only=True
        )
        if doc:
            row = _compare_doc_to_learning_row(
                db_instance, lottery, doc, feedback_by_draw, total_tickets
            )
            row["draw_date"] = row.get("draw_date") or pair.get("draw_date") or ""
        else:
            row = {
                "draw_id": cid,
                "pre_draw_id": pair["pre_id"],
                "draw_date": pair.get("draw_date") or "",
                "actual_jackpot_position": 0,
                "error_rate": 0.0,
                "updated_at": "",
                "feedback_records": [],
                "new_orc_hash": "",
                "feedback_status": "no_compare",
            }
        prepend.append(row)
        seen.add(cid)
    if not prepend:
        return rows
    merged = prepend + rows
    merged.sort(
        key=lambda r: (str(r.get("draw_date") or ""), str(r.get("draw_id") or "")),
        reverse=True,
    )
    return merged


def _compare_chronological_sort() -> List[Tuple[str, int]]:
    """Newest draw first (by official date, then draw id). Do not sort by ``updated_at``."""
    return [("date", -1), ("current_id", -1)]


def _enrich_compare_result_for_validation(
    db_instance,
    lottery: str,
    result: Dict[str, Any],
    *,
    touch_updated_at: bool = True,
) -> Dict[str, Any]:
    """Normalize date + precomputed validation metrics whenever a compare is saved."""
    out = {k: v for k, v in result.items() if k != "_id"}
    current_id = str(out.get("current_id") or "").strip()
    pre_id = str(out.get("pre_id") or "").strip()

    date_norm = str(out.get("date") or "").strip()[:10]
    if len(date_norm) < 10 and current_id:
        date_norm = _resolve_draw_dates(db_instance, lottery, [current_id]).get(current_id, "")

    jp_raw = out.get("jackpot_position")
    if jp_raw is None:
        jp_raw = out.get("special_position")
    try:
        jp_int = int(jp_raw) if jp_raw is not None else 0
    except (TypeError, ValueError):
        jp_int = 0

    total_tickets = _VALIDATION_TOTAL_TICKETS[lottery]
    error_rate = round(jp_int / total_tickets, 6) if jp_int > 0 else None
    error_rate_pct = round(error_rate * 100, 4) if error_rate is not None else None

    out["current_id"] = current_id
    out["pre_id"] = pre_id
    out["date"] = date_norm
    if jp_int > 0:
        out["jackpot_position"] = jp_int
    out["error_rate"] = error_rate
    out["error_rate_pct"] = error_rate_pct
    out["validation_ready"] = bool(jp_int > 0 and len(date_norm) >= 10)
    if touch_updated_at or not str(out.get("updated_at") or "").strip():
        out["updated_at"] = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    return out


def _maybe_schedule_post_draw_feedback(db_instance, lottery: str, compare_doc: Dict[str, Any]) -> None:
    """Run post-draw learning in the background when compare exists and ORC is available."""
    current_id = str(compare_doc.get("current_id") or "").strip()
    pre_id = str(compare_doc.get("pre_id") or "").strip()
    try:
        jp_int = int(compare_doc.get("jackpot_position") or 0)
    except (TypeError, ValueError):
        jp_int = 0
    if not current_id or not pre_id or pre_id == "__synthetic__" or jp_int <= 0:
        return
    if _feedback_log_exists(db_instance, lottery, current_id):
        return
    if not _orc_exists_for_pre_draw(db_instance, lottery, pre_id):
        logging.info(
            "[feedback-auto] skip %s draw_id=%s: no ORC for pre_id=%s",
            lottery,
            current_id,
            pre_id,
        )
        return

    def _run() -> None:
        try:
            ol = _import_online_learning()
            ol.post_draw(lottery=lottery, current_id=current_id, pre_id=pre_id)
            logging.info("[feedback-auto] completed %s draw_id=%s", lottery, current_id)
        except Exception as exc:
            logging.warning(
                "[feedback-auto] failed %s draw_id=%s: %s",
                lottery,
                current_id,
                exc,
            )

    threading.Thread(
        target=_run,
        daemon=True,
        name=f"feedback-auto-{lottery}-{current_id}",
    ).start()


def _save_compare_result(
    db_instance,
    lottery: str,
    result: Dict[str, Any],
    *,
    touch_updated_at: bool = True,
) -> Dict[str, Any]:
    """Persist compare result and refresh fields used by /api/validation/*."""
    enriched = _enrich_compare_result_for_validation(
        db_instance, lottery, result, touch_updated_at=touch_updated_at
    )
    coll = db_instance[_validation_compare_collection(lottery)]
    _delete_compare_pair_variants(
        coll, enriched["current_id"], enriched["pre_id"]
    )
    coll.replace_one(
        {"current_id": enriched["current_id"], "pre_id": enriched["pre_id"]},
        enriched,
        upsert=True,
    )
    _maybe_schedule_post_draw_feedback(db_instance, lottery, enriched)
    return enriched


def _compare_rows_filter(*, exclude_synthetic: bool = True) -> dict:
    filt: dict = {"jackpot_position": {"$gt": 0}}
    if exclude_synthetic:
        filt["pre_id"] = {"$ne": "__synthetic__"}
    return filt


def _fetch_recent_compare_rows(
    coll,
    limit: int,
    projection: dict,
    *,
    exclude_synthetic: bool = True,
) -> tuple[list, int]:
    """
    Return the most recent `limit` compare rows, oldest→newest (for charts).
    Also returns total count of valid rows in the collection.
    """
    filt = _compare_rows_filter(exclude_synthetic=exclude_synthetic)
    total_in_db = coll.count_documents(filt)
    rows_raw = list(
        coll.find(filt, projection=projection)
        .sort(_compare_chronological_sort())
        .limit(limit)
    )
    rows_raw.reverse()
    return rows_raw, total_in_db


def _latest_compare_snapshot(coll, *, exclude_synthetic: bool = True) -> Optional[dict]:
    """Most recent compare row in the collection (for validation dashboard header)."""
    doc = coll.find_one(
        _compare_rows_filter(exclude_synthetic=exclude_synthetic),
        projection={"current_id": 1, "date": 1, "jackpot_position": 1},
        sort=_compare_chronological_sort(),
    )
    if not doc:
        return None
    jp = doc.get("jackpot_position")
    if not jp:
        return None
    return {
        "draw_id": str(doc.get("current_id") or ""),
        "draw_date": str(doc.get("date") or ""),
        "jackpot_position": int(jp),
    }


def _fetch_compare_rows_page(
    coll,
    skip: int,
    limit: int,
    projection: dict,
    *,
    exclude_synthetic: bool = True,
) -> tuple[list, int]:
    """Paginated compare rows, newest→oldest (for tables)."""
    filt = _compare_rows_filter(exclude_synthetic=exclude_synthetic)
    total_in_db = coll.count_documents(filt)
    rows_raw = list(
        coll.find(filt, projection=projection)
        .sort(_compare_chronological_sort())
        .skip(skip)
        .limit(limit)
    )
    return rows_raw, total_in_db


@app.post("/api/validation/rebuild-metrics")
def api_validation_rebuild_metrics(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
):
    """
    Recompute validation fields (date, error_rate, updated_at) on all compare rows.
    Run once after deploy so /validation shows the latest draws.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if lottery not in _VALIDATION_TOTAL_TICKETS:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}")

    coll = db[_validation_compare_collection(lottery)]
    updated = 0
    for doc in coll.find({}):
        _save_compare_result(db, lottery, doc)
        updated += 1
    return JSONResponse(content={"lottery": lottery, "updated": updated})


@app.get("/api/validation/accuracy-chart")
def api_validation_accuracy_chart(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
    limit: int = Query(100, ge=1, le=1000),
):
    """
    Accuracy chart data per draw.

    For each draw that has a compare result, returns:
    - draw_id, draw_date
    - jackpot_position (actual position of the winning ticket in the ranked list)
    - total_tickets (total tickets in the wheel)
    - error_rate (jackpot_position / total_tickets — lower is better)
    - error_rate_pct (error_rate * 100)
    - model_source (ml_model | freq_gap)

    Sorted oldest → newest (most recent `limit` draws in DB).
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    compare_map = {
        "euromillones": (EUROMILLONES_COMPARE_RESULTS_COLLECTION, 139_838_160),
        "el-gordo":     (EL_GORDO_COMPARE_RESULTS_COLLECTION,     31_625_100),
        "la-primitiva": (LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION, 139_838_160),
    }
    if lottery not in compare_map:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}")

    coll_name, total_tickets = compare_map[lottery]
    coll = db[coll_name]

    # Most recent compare rows (not the oldest in DB)
    rows_raw, total_in_db = _fetch_recent_compare_rows(
        coll,
        limit,
        projection={"_id": 0, "current_id": 1, "pre_id": 1, "date": 1,
                    "jackpot_position": 1, "source": 1},
    )

    # Enrich with model source from draw_probs
    draw_probs_map = {
        "euromillones": "euromillones_draw_probs",
        "el-gordo":     "el_gordo_draw_probs",
        "la-primitiva": "la_primitiva_draw_probs",
    }
    probs_coll = db[draw_probs_map[lottery]]

    rows = []
    for r in rows_raw:
        jp = r.get("jackpot_position")
        if not jp:
            continue
        jp = int(jp)
        error_rate = r.get("error_rate")
        if error_rate is None:
            error_rate = round(jp / total_tickets, 6)
        else:
            error_rate = float(error_rate)

        # Get model source from draw_probs for the pre_id (the model used to rank)
        pre_id = str(r.get("pre_id") or "").strip()
        prob_doc = probs_coll.find_one({"draw_id": pre_id}, projection={"source": 1}) if pre_id else None
        model_source = str(prob_doc.get("source") or "unknown") if prob_doc else "unknown"

        rows.append({
            "draw_id":        str(r.get("current_id") or ""),
            "pre_id":         pre_id,
            "draw_date":      str(r.get("date") or ""),
            "jackpot_position": jp,
            "total_tickets":  total_tickets,
            "error_rate":     error_rate,
            "error_rate_pct": round(error_rate * 100, 4),
            "model_source":   model_source,
        })

    # Compute rolling mean error (window=5) for trend line
    for i, row in enumerate(rows):
        window = rows[max(0, i - 4): i + 1]
        row["rolling_mean_error_rate"] = round(
            sum(w["error_rate"] for w in window) / len(window), 6
        )

    total_draws = len(rows)
    avg_error = round(sum(r["error_rate"] for r in rows) / total_draws, 6) if total_draws else 0
    best = min(rows, key=lambda x: x["jackpot_position"]) if rows else None
    worst = max(rows, key=lambda x: x["jackpot_position"]) if rows else None
    latest = _latest_compare_snapshot(coll)

    return JSONResponse(content={
        "lottery":      lottery,
        "total_draws":  total_draws,
        "total_in_db":  total_in_db,
        "limit":        limit,
        "total_tickets": total_tickets,
        "avg_error_rate": avg_error,
        "avg_error_rate_pct": round(avg_error * 100, 4),
        "latest_draw": latest,
        "best_draw":    {"draw_id": best["draw_id"], "jackpot_position": best["jackpot_position"], "draw_date": best["draw_date"]} if best else None,
        "worst_draw":   {"draw_id": worst["draw_id"], "jackpot_position": worst["jackpot_position"], "draw_date": worst["draw_date"]} if worst else None,
        "rows":         rows,
    })


@app.get("/api/validation/mean-error-chart")
def api_validation_mean_error_chart(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
    limit: int = Query(100, ge=1, le=1000),
):
    """
    Mean error chart: distance between predicted position and actual jackpot position.

    Returns per-draw error distance and cumulative moving average,
    so the client can visualise whether the model is improving over time.
    Uses the most recent `limit` compare rows (oldest→newest in response).
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    compare_map = {
        "euromillones": (EUROMILLONES_COMPARE_RESULTS_COLLECTION, 139_838_160),
        "el-gordo":     (EL_GORDO_COMPARE_RESULTS_COLLECTION,     31_625_100),
        "la-primitiva": (LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION, 139_838_160),
    }
    if lottery not in compare_map:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}")

    coll_name, total_tickets = compare_map[lottery]
    coll = db[coll_name]

    rows_raw, total_in_db = _fetch_recent_compare_rows(
        coll,
        limit,
        projection={"_id": 0, "current_id": 1, "date": 1, "jackpot_position": 1},
    )

    rows = []
    cumulative_sum = 0.0
    for i, r in enumerate(rows_raw):
        jp = r.get("jackpot_position")
        if not jp:
            continue
        jp = int(jp)
        error_distance = jp  # absolute distance from position 1 (perfect)
        cumulative_sum += error_distance
        cumulative_mean = round(cumulative_sum / (i + 1), 2)

        rows.append({
            "draw_id":          str(r.get("current_id") or ""),
            "draw_date":        str(r.get("date") or ""),
            "jackpot_position": jp,
            "error_distance":   error_distance,
            "cumulative_mean_error": cumulative_mean,
        })

    total_draws = len(rows)
    overall_mean = round(sum(r["error_distance"] for r in rows) / total_draws, 2) if total_draws else 0

    return JSONResponse(content={
        "lottery":       lottery,
        "total_draws":   total_draws,
        "total_in_db":   total_in_db,
        "limit":         limit,
        "total_tickets": total_tickets,
        "overall_mean_error": overall_mean,
        "rows":          rows,
    })


@app.get("/api/validation/accuracy-rows")
def api_validation_accuracy_rows(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
):
    """Paginated accuracy table rows (newest draw first)."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    compare_map = {
        "euromillones": (EUROMILLONES_COMPARE_RESULTS_COLLECTION, 139_838_160),
        "el-gordo":     (EL_GORDO_COMPARE_RESULTS_COLLECTION,     31_625_100),
        "la-primitiva": (LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION, 139_838_160),
    }
    if lottery not in compare_map:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}")

    coll_name, total_tickets = compare_map[lottery]
    coll = db[coll_name]

    rows_raw, total_in_db = _fetch_compare_rows_page(
        coll,
        skip,
        limit,
        projection={"_id": 0, "current_id": 1, "pre_id": 1, "date": 1,
                    "jackpot_position": 1, "error_rate": 1, "source": 1},
    )

    draw_probs_map = {
        "euromillones": "euromillones_draw_probs",
        "el-gordo":     "el_gordo_draw_probs",
        "la-primitiva": "la_primitiva_draw_probs",
    }
    probs_coll = db[draw_probs_map[lottery]]

    rows = []
    for r in rows_raw:
        jp = r.get("jackpot_position")
        if not jp:
            continue
        jp = int(jp)
        error_rate = r.get("error_rate")
        if error_rate is None:
            error_rate = round(jp / total_tickets, 6)
        else:
            error_rate = float(error_rate)
        pre_id = str(r.get("pre_id") or "").strip()
        prob_doc = probs_coll.find_one({"draw_id": pre_id}, projection={"source": 1}) if pre_id else None
        model_source = str(prob_doc.get("source") or "unknown") if prob_doc else "unknown"
        rows.append({
            "draw_id":          str(r.get("current_id") or ""),
            "pre_id":           pre_id,
            "draw_date":        str(r.get("date") or ""),
            "jackpot_position": jp,
            "total_tickets":    total_tickets,
            "error_rate":       error_rate,
            "error_rate_pct":   round(error_rate * 100, 4),
            "model_source":     model_source,
        })

    return JSONResponse(content={
        "lottery":     lottery,
        "total":       total_in_db,
        "skip":        skip,
        "limit":       limit,
        "rows":        rows,
    })


@app.get("/api/validation/mean-error-rows")
def api_validation_mean_error_rows(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
):
    """Paginated mean-error table rows (newest draw first)."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    compare_map = {
        "euromillones": (EUROMILLONES_COMPARE_RESULTS_COLLECTION, 139_838_160),
        "el-gordo":     (EL_GORDO_COMPARE_RESULTS_COLLECTION,     31_625_100),
        "la-primitiva": (LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION, 139_838_160),
    }
    if lottery not in compare_map:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}")

    coll_name, total_tickets = compare_map[lottery]
    coll = db[coll_name]

    rows_raw, total_in_db = _fetch_compare_rows_page(
        coll,
        skip,
        limit,
        projection={"_id": 0, "current_id": 1, "date": 1, "jackpot_position": 1},
    )

    rows = []
    for r in rows_raw:
        jp = r.get("jackpot_position")
        if not jp:
            continue
        jp = int(jp)
        rows.append({
            "draw_id":          str(r.get("current_id") or ""),
            "draw_date":        str(r.get("date") or ""),
            "jackpot_position": jp,
            "error_distance":   jp,
        })

    return JSONResponse(content={
        "lottery":       lottery,
        "total":         total_in_db,
        "skip":          skip,
        "limit":         limit,
        "total_tickets": total_tickets,
        "rows":          rows,
    })


@app.get("/api/validation/top-tickets")
def api_validation_top_tickets(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
    draw_id: str = Query(..., description="current_id of the draw to inspect"),
    pre_id: str = Query(..., description="pre_id used for ranking"),
    limit: int = Query(100, ge=1, le=25000),
):
    """
    Top-N tickets from the ranked full wheel for a specific draw.

    Loads the probability snapshot for pre_id, scores all tickets in memory,
    returns the top `limit` tickets with their rank, numbers, and score.

    The client can use this to show the top 100 or top 1000 tickets and
    how many numbers they hit against the actual draw result.

    Also returns how many of the top-N tickets matched 0/1/2/3/4/5 main numbers.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    lottery_cfg = {
        "euromillones": {
            "tickets_coll":  "euromillones_tickets",
            "draw_probs_coll": "euromillones_draw_probs",
            "draw_coll":     "euromillones",
            "secondary_key": "stars_probs",
            "secondary_field": "stars",
            "mains_field":   "mains",
            "secondary_type": "list",
        },
        "el-gordo": {
            "tickets_coll":  "el_gordo_tickets",
            "draw_probs_coll": "el_gordo_draw_probs",
            "draw_coll":     "el_gordo",
            "secondary_key": "clave_probs",
            "secondary_field": "clave",
            "mains_field":   "mains",
            "secondary_type": "int",
        },
        "la-primitiva": {
            "tickets_coll":  "la_primitiva_tickets",
            "draw_probs_coll": "la_primitiva_draw_probs",
            "draw_coll":     "la_primitiva",
            "secondary_key": "rein_probs",
            "secondary_field": "reintegro",
            "mains_field":   "mains",
            "secondary_type": "int",
        },
    }
    if lottery not in lottery_cfg:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}")

    cfg = lottery_cfg[lottery]
    draw_id_clean = draw_id.strip()
    pre_id_clean  = pre_id.strip()

    # Load probability snapshot for pre_id
    probs_doc = db[cfg["draw_probs_coll"]].find_one({"draw_id": pre_id_clean})
    if not probs_doc:
        raise HTTPException(404, detail=f"No probability snapshot for pre_id={pre_id_clean!r}")

    mains_probs_raw = probs_doc.get("mains_probs") or {}
    sec_probs_raw   = probs_doc.get(cfg["secondary_key"]) or {}
    mains_probs = {int(k): float(v) for k, v in mains_probs_raw.items()}
    sec_probs   = {int(k): float(v) for k, v in sec_probs_raw.items()}

    if not mains_probs:
        raise HTTPException(404, detail=f"Empty probability snapshot for pre_id={pre_id_clean!r}")

    # Load actual draw result to compute hits
    draw_doc = db[cfg["draw_coll"]].find_one({"id_sorteo": draw_id_clean})
    if not draw_doc and draw_id_clean.isdigit():
        draw_doc = db[cfg["draw_coll"]].find_one({"id_sorteo": int(draw_id_clean)})

    actual_mains: set = set()
    actual_secondary = None
    if draw_doc:
        nums = draw_doc.get("numbers") or []
        if lottery == "euromillones":
            actual_mains = {int(x) for x in nums[:5]}
            actual_secondary = {int(x) for x in nums[5:7]} if len(nums) >= 7 else set()
        elif lottery == "el-gordo":
            actual_mains = {int(x) for x in nums[:5]}
            rein = draw_doc.get("reintegro")
            actual_secondary = int(rein) if rein is not None else None
        else:  # la-primitiva
            actual_mains = {int(x) for x in nums[:6]}
            rein = draw_doc.get("reintegro")
            actual_secondary = int(rein) if rein is not None else None

    import math

    def _score(mains, secondary):
        s = sum(math.log(max(mains_probs.get(n, 1e-6), 1e-9)) for n in mains)
        if isinstance(secondary, list):
            s += sum(math.log(max(sec_probs.get(x, 1e-6), 1e-9)) for x in secondary)
        elif secondary is not None:
            s += math.log(max(sec_probs.get(int(secondary), 1e-6), 1e-9))
        return s

    # Stream and score all tickets, keep top-limit using a min-heap
    import heapq
    heap = []  # (score, mains, secondary)

    tickets_coll = db[cfg["tickets_coll"]]
    proj = {"_id": 0, "mains": 1, cfg["secondary_field"]: 1}

    for doc in tickets_coll.find({}, projection=proj):
        m = doc.get("mains") or []
        sec = doc.get(cfg["secondary_field"])
        if not m:
            continue
        sc = _score(m, sec)
        if len(heap) < limit:
            heapq.heappush(heap, (sc, m, sec))
        elif sc > heap[0][0]:
            heapq.heapreplace(heap, (sc, m, sec))

    # Sort descending by score
    top = sorted(heap, key=lambda x: x[0], reverse=True)

    # Build result rows with hit counts
    hit_distribution = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0}
    result_rows = []
    for rank, (sc, mains, secondary) in enumerate(top, 1):
        hits_main = sum(1 for n in mains if n in actual_mains) if actual_mains else None
        if lottery == "euromillones":
            hits_sec = sum(1 for n in (secondary or []) if n in (actual_secondary or set())) if actual_secondary is not None else None
        else:
            hits_sec = (1 if secondary == actual_secondary else 0) if actual_secondary is not None else None

        if hits_main is not None:
            hit_distribution[hits_main] = hit_distribution.get(hits_main, 0) + 1

        row: dict = {
            "rank":   rank,
            "mains":  mains,
            "score":  round(sc, 6),
            "hits_main": hits_main,
            "hits_secondary": hits_sec,
        }
        if lottery == "euromillones":
            row["stars"] = secondary
        elif lottery == "el-gordo":
            row["clave"] = secondary
        else:
            row["reintegro"] = secondary
        result_rows.append(row)

    return JSONResponse(content={
        "lottery":          lottery,
        "draw_id":          draw_id_clean,
        "pre_id":           pre_id_clean,
        "total_returned":   len(result_rows),
        "limit":            limit,
        "has_actual_draw":  bool(draw_doc),
        "hit_distribution": hit_distribution,
        "rows":             result_rows,
    })


@app.get("/api/validation/cross-validation")
def api_validation_cross_validation(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
    limit: int = Query(50, ge=5, le=500),
):
    """
    Cross-validation report per draw.

    For each draw with a compare result, computes:
    - jackpot_position rank
    - percentile rank (what % of tickets were ranked above the jackpot)
    - whether the jackpot was in the top 1%, 5%, 10%, 25%

    Returns summary statistics and per-draw breakdown.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    compare_map = {
        "euromillones": (EUROMILLONES_COMPARE_RESULTS_COLLECTION, 139_838_160),
        "el-gordo":     (EL_GORDO_COMPARE_RESULTS_COLLECTION,     31_625_100),
        "la-primitiva": (LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION, 139_838_160),
    }
    if lottery not in compare_map:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}")

    coll_name, total_tickets = compare_map[lottery]
    coll = db[coll_name]

    rows_raw, _ = _fetch_recent_compare_rows(
        coll,
        limit,
        projection={"_id": 0, "current_id": 1, "pre_id": 1, "date": 1, "jackpot_position": 1},
    )

    rows = []
    in_top_1pct = in_top_5pct = in_top_10pct = in_top_25pct = 0

    for r in reversed(rows_raw):
        jp = r.get("jackpot_position")
        if not jp:
            continue
        jp = int(jp)
        percentile = round(jp / total_tickets * 100, 4)
        top_1  = jp <= total_tickets * 0.01
        top_5  = jp <= total_tickets * 0.05
        top_10 = jp <= total_tickets * 0.10
        top_25 = jp <= total_tickets * 0.25

        if top_1:  in_top_1pct  += 1
        if top_5:  in_top_5pct  += 1
        if top_10: in_top_10pct += 1
        if top_25: in_top_25pct += 1

        rows.append({
            "draw_id":          str(r.get("current_id") or ""),
            "pre_id":           str(r.get("pre_id") or ""),
            "draw_date":        str(r.get("date") or ""),
            "jackpot_position": jp,
            "percentile":       percentile,
            "in_top_1pct":      top_1,
            "in_top_5pct":      top_5,
            "in_top_10pct":     top_10,
            "in_top_25pct":     top_25,
        })

    n = len(rows)
    return JSONResponse(content={
        "lottery":       lottery,
        "total_draws":   n,
        "total_tickets": total_tickets,
        "summary": {
            "in_top_1pct":  in_top_1pct,
            "in_top_5pct":  in_top_5pct,
            "in_top_10pct": in_top_10pct,
            "in_top_25pct": in_top_25pct,
            "pct_in_top_1pct":  round(in_top_1pct  / n * 100, 1) if n else 0,
            "pct_in_top_5pct":  round(in_top_5pct  / n * 100, 1) if n else 0,
            "pct_in_top_10pct": round(in_top_10pct / n * 100, 1) if n else 0,
            "pct_in_top_25pct": round(in_top_25pct / n * 100, 1) if n else 0,
        },
        "rows": rows,
    })


@app.get("/api/validation/score-vs-position")
def api_validation_score_vs_position(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
    limit: int = Query(100, ge=5, le=500),
):
    """
    Scatter data: model score (X) vs actual jackpot rank (Y) per draw.
    One point per draw — winning combination scored with pre-draw probability snapshot.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    import math

    cfg_map = {
        "euromillones": {
            "compare": EUROMILLONES_COMPARE_RESULTS_COLLECTION,
            "probs": "euromillones_draw_probs",
            "draw": "euromillones",
            "sec_key": "stars_probs",
            "sec_list": True,
        },
        "el-gordo": {
            "compare": EL_GORDO_COMPARE_RESULTS_COLLECTION,
            "probs": "el_gordo_draw_probs",
            "draw": "el_gordo",
            "sec_key": "clave_probs",
            "sec_list": False,
        },
        "la-primitiva": {
            "compare": LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION,
            "probs": "la_primitiva_draw_probs",
            "draw": "la_primitiva",
            "sec_key": "rein_probs",
            "sec_list": False,
        },
    }
    if lottery not in cfg_map:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}")

    cfg = cfg_map[lottery]
    compare_coll = db[cfg["compare"]]
    probs_coll = db[cfg["probs"]]
    draw_coll = db[cfg["draw"]]

    docs = list(
        compare_coll.find(
            {"jackpot_position": {"$gt": 0}},
            projection={"_id": 0, "current_id": 1, "pre_id": 1, "date": 1, "jackpot_position": 1},
            sort=[("date", -1)],
            limit=limit,
        )
    )
    docs.reverse()

    points: List[Dict[str, Any]] = []
    for doc in docs:
        current_id = str(doc.get("current_id") or "").strip()
        pre_id = str(doc.get("pre_id") or "").strip()
        jp = int(doc.get("jackpot_position") or 0)
        if not current_id or not pre_id or jp <= 0:
            continue

        prob_doc = probs_coll.find_one({"draw_id": pre_id})
        if not prob_doc:
            continue
        mains_probs = {int(k): float(v) for k, v in (prob_doc.get("mains_probs") or {}).items()}
        sec_probs = {int(k): float(v) for k, v in (prob_doc.get(cfg["sec_key"]) or {}).items()}
        if not mains_probs:
            continue

        draw_doc = draw_coll.find_one({"id_sorteo": current_id})
        if not draw_doc and current_id.isdigit():
            draw_doc = draw_coll.find_one({"id_sorteo": int(current_id)})
        if not draw_doc:
            continue

        nums = draw_doc.get("numbers") or []
        if lottery == "euromillones":
            mains = [int(x) for x in nums[:5]]
            sec = [int(x) for x in nums[5:7]] if len(nums) >= 7 else []
        elif lottery == "el-gordo":
            mains = [int(x) for x in nums[:5]]
            rein = draw_doc.get("reintegro")
            sec = int(rein) if rein is not None else None
        else:
            mains = [int(x) for x in nums[:6]]
            rein = draw_doc.get("reintegro")
            sec = int(rein) if rein is not None else None

        score = sum(math.log(max(mains_probs.get(n, 1e-6), 1e-9)) for n in mains)
        if cfg["sec_list"] and isinstance(sec, list):
            score += sum(math.log(max(sec_probs.get(x, 1e-6), 1e-9)) for x in sec)
        elif sec is not None:
            score += math.log(max(sec_probs.get(int(sec), 1e-6), 1e-9))

        points.append({
            "draw_id": current_id,
            "pre_id": pre_id,
            "draw_date": str(doc.get("date") or "")[:10],
            "score": round(score, 6),
            "jackpot_position": jp,
        })

    return JSONResponse(content={
        "lottery": lottery,
        "total_points": len(points),
        "points": points,
    })


@app.get("/api/validation/model-performance")
def api_validation_model_performance(
    lottery: str = Query(..., description="euromillones | el-gordo | la-primitiva"),
    limit: int = Query(50, ge=5, le=500),
):
    """
    Combined learning report for the validation dashboard (client spec 1.md).
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    compare_map = {
        "euromillones": (EUROMILLONES_COMPARE_RESULTS_COLLECTION, 139_838_160),
        "el-gordo":     (EL_GORDO_COMPARE_RESULTS_COLLECTION,     31_625_100),
        "la-primitiva": (LA_PRIMITIVA_COMPARE_RESULTS_COLLECTION, 139_838_160),
    }
    if lottery not in compare_map:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}")

    coll_name, total_tickets = compare_map[lottery]
    coll = db[coll_name]

    rows_raw, total_in_db = _fetch_recent_compare_rows(
        coll,
        limit,
        projection={"_id": 0, "current_id": 1, "pre_id": 1, "date": 1, "jackpot_position": 1},
    )

    error_history: List[int] = []
    accuracy_history: List[float] = []
    for r in rows_raw:
        jp = int(r.get("jackpot_position") or 0)
        if jp <= 0:
            continue
        error_history.append(jp)
        accuracy_history.append(round((1.0 - jp / total_tickets) * 100, 4))

    feedback_rows = _enrich_feedback_rows(
        db,
        lottery,
        [_item_to_json(r) for r in db["model_feedback_log"].find(
            {"lottery": lottery},
            projection={"_id": 0, "draw_id": 1, "draw_date": 1, "error_rate": 1, "updated_at": 1},
            sort=[("updated_at", -1)],
            limit=10,
        )],
    )

    last_validation = None
    if rows_raw:
        last = rows_raw[-1]
        jp = int(last.get("jackpot_position") or 0)
        prev_jp = int(rows_raw[-2].get("jackpot_position") or 0) if len(rows_raw) >= 2 else jp
        improvement = ""
        if prev_jp > 0 and jp < prev_jp:
            improvement = f"{round((prev_jp - jp) / prev_jp * 100, 1)}% better rank"
        elif prev_jp > 0 and jp > prev_jp:
            improvement = f"{round((jp - prev_jp) / prev_jp * 100, 1)}% worse rank"
        else:
            improvement = "stable"
        last_validation = {
            "draw_id": str(last.get("current_id") or ""),
            "draw_date": str(last.get("date") or "")[:10],
            "jackpot_position": jp,
            "mean_error": jp,
            "error_rate_pct": round(jp / total_tickets * 100, 4),
            "improvement_since_last": improvement,
        }

    avg_error = int(mean(error_history)) if error_history else 0
    learning_cycles = db["model_feedback_log"].count_documents({"lottery": lottery})

    return JSONResponse(content={
        "lottery": lottery,
        "total_in_db": total_in_db,
        "accuracy_history": accuracy_history,
        "error_history": error_history,
        "total_draws": len(error_history),
        "avg_mean_error": avg_error,
        "learning_cycles": learning_cycles,
        "last_validation": last_validation,
        "recent_feedback": feedback_rows,
    })
