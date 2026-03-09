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
import threading
import time
from contextlib import asynccontextmanager
from statistics import mean
from typing import Optional, List, Dict

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

# Ensure we can import helper scripts from the project-level `scripts` folder.
_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SCRIPTS_DIR = os.path.join(_ROOT_DIR, "scripts")
if _SCRIPTS_DIR not in sys.path:
    sys.path.append(_SCRIPTS_DIR)

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


def build_step4_pool( 
    mains_probs: List[Dict],
    stars_probs: List[Dict],
    prev_main_numbers: Optional[List[int]] = None,
    prev_star_numbers: Optional[List[int]] = None,
    seed: Optional[int] = None,
) -> Dict:
    """ 
    Step 4 pool: 20 main numbers + 4 star numbers.

    - 3 mains + 1 star: from previous draw (prev_main_numbers, prev_star_numbers) if available,
      else random. No duplicate with the rest.
    - 17 mains: from Step 3 ranking (mains_probs sorted by p), no duplicate with the 3 above:
      from 1st–20th select 8, 21st–30th select 3, 31st–40th select 3, 41st–50th select 3.
    - 3 stars: from Step 3 star ranking, no duplicate with the 1 above:
      from 1st–6th select 2, 7th–12th select 1.

    Returns filtered_mains (20 items), filtered_stars (4 items), rules_used, stats.
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

    # Randomly reorder mains and stars so Step 5 receives a shuffled pool.
    random.shuffle(filtered_mains)
    random.shuffle(filtered_stars)

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

    # Randomly reorder mains and clave so Step 5 receives shuffled pools.
    random.shuffle(filtered_mains)
    random.shuffle(filtered_clave)

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
      Final mains list is truncated to 20.
    - Reintegro pool:
        * 1 reintegro from previous draw (snapshot) if available, else random
        * Up to 3 additional reintegros from ranking (reintegro_probs sorted by p), no duplicate
      Final reintegro list is truncated to 4.
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

    # Main pool cap 20; distribution: 4–5 prev + (6–8) + 4 + 4 from bands
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
    filtered_mains = filtered_mains_items[:MAIN_POOL_MAX]

    # --- Reintegro pool (cap 4): snapshot + top ranking reintegros (avoid duplicates) ---
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
    filtered_rein = filtered_rein_items[:REIN_POOL_MAX]

    # Randomly reorder mains and reintegro so Step 5 receives shuffled pools.
    random.shuffle(filtered_mains)
    random.shuffle(filtered_rein)

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
EL_GORDO_TRAIN_PROGRESS_COLLECTION = "el_gordo_train_progress"
EL_GORDO_BUY_QUEUE_COLLECTION = "el_gordo_buy_queue"
LA_PRIMITIVA_BUY_QUEUE_COLLECTION = "la_primitiva_buy_queue"
EUROMILLONES_BUY_QUEUE_COLLECTION = "euromillones_buy_queue"
LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION = "la_primitiva_train_progress"
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
    db[EL_GORDO_TRAIN_PROGRESS_COLLECTION].create_index("cutoff_draw_id", unique=True)
    db[EL_GORDO_BUY_QUEUE_COLLECTION].create_index([("status", 1), ("created_at", 1)])
    db[LA_PRIMITIVA_TRAIN_PROGRESS_COLLECTION].create_index("cutoff_draw_id", unique=True)
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

# Paths that do not require Authorization Bearer token (health, login, bot claim/complete, bot active-credentials)
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
    """Return El Gordo training progress for the given cutoff_draw_id (dataset prepared, models trained)."""
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
    }
    return JSONResponse(
        content={"progress": progress},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


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
    Step 4 (new_flow): build pool of 20 main numbers + 6 clave numbers for El Gordo.
    - 4–5 mains + 1 clave from row where pre_id_sorteo == current id_sorteo, or random if not found.
    - Remaining mains from Step 3 ranking bands (1–20: 6–7, 21–30: 3, 31–40: 3, 41–54: 3), no duplicate.
    - Clave pool: snapshot clave + up to 5 additional clave candidates from Step 3 ranking, no duplicate.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    if not cutoff_draw_id:
        raise HTTPException(400, detail="cutoff_draw_id is required")
    cid = cutoff_draw_id.strip()
    progress_coll = db[EL_GORDO_TRAIN_PROGRESS_COLLECTION]
    feature_coll = db["el_gordo_feature"]
    doc = progress_coll.find_one({"cutoff_draw_id": cid})
    if not doc:
        raise HTTPException(404, detail="Progress not found for this cutoff_draw_id")
    mains = doc.get("mains_probs") or []
    claves = doc.get("clave_probs") or []
    if not mains and not claves:
        raise HTTPException(400, detail="Run step 3 (compute probabilities) first.")

    # Row where pre_id_sorteo == current id_sorteo (current id_sorteo == row.pre_id_sorteo)
    prev_main_numbers: Optional[List[int]] = None
    prev_clave: Optional[int] = None
    prev_row = feature_coll.find_one({"pre_id_sorteo": cid})
    if prev_row:
        pm = prev_row.get("main_number") or []
        prev_main_numbers = [int(x) for x in pm if isinstance(x, (int, float))]
        clave_val = prev_row.get("clave")
        if isinstance(clave_val, (int, float)):
            prev_clave = int(clave_val)

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
    return JSONResponse(
        content={
            "status": "ok",
            "filtered_mains": result["filtered_mains"],
            "filtered_clave": result["filtered_clave"],
            "rules_used": result["rules_used"],
            "excluded": result["excluded"],
        }
    )


@app.post("/api/el-gordo/train/candidate-pool")
def api_el_gordo_candidate_pool(
    cutoff_draw_id: str | None = Query(
        None,
        description="id_sorteo for this El Gordo training run.",
    ),
    num_tickets: int = Query(3000, ge=100, le=10000),
):
    """
    Step 5 (new_flow): generate candidate ticket pool for El Gordo from Step 4 pool.

    - Uses filtered_mains_probs (20 mains) and filtered_clave_probs (up to 6 claves)
      produced by Step 4.
    - Each ticket: 5 distinct mains from the 20, 1 clave from the clave pool
      (assigned in round-robin order).
    - Saves candidate_pool (list of {mains, clave}) and count to el_gordo_train_progress.
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

    # Generate mains combinations in a structured way (no duplicates),
    # then pair them with claves in round-robin fashion.
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
    feature_coll = db["la_primitiva_feature"]
    doc = progress_coll.find_one({"cutoff_draw_id": cid})
    if not doc:
        raise HTTPException(404, detail="Progress not found for this cutoff_draw_id")
    mains = doc.get("mains_probs") or []
    reintegros = doc.get("reintegro_probs") or []
    if not mains and not reintegros:
        raise HTTPException(400, detail="Run step 3 (compute probabilities) first.")

    # Row where pre_id_sorteo == current id_sorteo
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
    return JSONResponse(
        content={
            "status": "ok",
            "filtered_mains": result["filtered_mains"],
            "filtered_reintegro": result["filtered_reintegro"],
            "rules_used": result["rules_used"],
            "excluded": result["excluded"],
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
    feature_coll = db["euromillones_feature"]
    doc = progress_coll.find_one({"cutoff_draw_id": cid})
    if not doc:
        raise HTTPException(404, detail="Progress not found for this cutoff_draw_id")
    mains = doc.get("mains_probs") or []
    stars = doc.get("stars_probs") or []
    if not mains and not stars:
        raise HTTPException(400, detail="Run step 3 (compute probabilities) first.")

    # Row where pre_id_sorteo == current id_sorteo (current id_sorteo == row.pre_id_sorteo)
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
    return JSONResponse(
        content={
            "status": "ok",
            "filtered_mains": result["filtered_mains"],
            "filtered_stars": result["filtered_stars"],
            "rules_used": result["rules_used"],
            "excluded": result["excluded"],
        }
    )


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
