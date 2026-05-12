"""
DB-based ticket pool — permanent tickets + per-draw ranking history.

Collections
===========
  {lottery}_tickets   — permanent, never changes after bootstrap
                        { position, mains, stars/clave/reintegro, tier }
                        ONE document per combination, ever.

  {lottery}_rankings  — one document per draw_id
                        { draw_id, draw_date, ranked_at,
                          scores: [{position, score, rank}] }
                        Full ranking snapshot saved after every draw pipeline.

Flow
====
  Bootstrap (once):
    - Generate ALL combinations, store in {lottery}_tickets with tier only.
    - Compute initial scores from first feature row (freq+gap).
    - Save as draw_id="bootstrap" in {lottery}_rankings.

  Every draw (pipeline done):
    - Compute new scores from ML probabilities.
    - Assign rank (1 = best) by sorting scores DESC.
    - Save full snapshot to {lottery}_rankings keyed by draw_id.

  Compare:
    - Load ranking for pre_id from {lottery}_rankings.
    - Join with {lottery}_tickets to get mains/stars.
    - Stream in rank order, find jackpot position.
"""
from __future__ import annotations

import logging
import math
from datetime import datetime as dt
from itertools import combinations
from typing import Any, Callable, Dict, Generator, List, Optional, Sequence, Tuple

logger = logging.getLogger("lottery.ticket_db")

# ── Full number universes ─────────────────────────────────────────────────────
EUROMILLONES_MAINS = list(range(1, 51))
EUROMILLONES_STARS = list(range(1, 13))
EL_GORDO_MAINS     = list(range(1, 55))
EL_GORDO_CLAVES    = list(range(0, 10))
LA_PRIMITIVA_MAINS = list(range(1, 50))
LA_PRIMITIVA_REINS = list(range(0, 10))

# ── Ticket tier ───────────────────────────────────────────────────────────────
def _ticket_tier_mains(mains: Sequence[int]) -> int:
    nums = sorted(int(n) for n in mains)
    score = 0
    longest_run = current_run = 1
    for i in range(1, len(nums)):
        if nums[i] == nums[i - 1] + 1:
            current_run += 1
            longest_run = max(longest_run, current_run)
        else:
            current_run = 1
    if longest_run >= 4: score += 3
    elif longest_run == 3: score += 1
    decades     = {n // 10 for n in nums}
    last_digits = {n % 10  for n in nums}
    if len(decades)     == 1: score += 2
    if len(last_digits) == 1: score += 2
    odds  = sum(1 for n in nums if n % 2 == 1)
    evens = len(nums) - odds
    if odds == len(nums) or evens == len(nums): score += 2
    if score >= 5: return 3
    if score >= 3: return 2
    if score >= 1: return 1
    return 0

# ── Score computation ─────────────────────────────────────────────────────────
def _compute_score(
    mains: List[int],
    secondary: "int | List[int]",
    mains_probs: Dict[int, float],
    secondary_probs: Dict[int, float],
) -> float:
    if not mains_probs:
        return 0.0
    log_score = 0.0
    for n in mains:
        p = mains_probs.get(n, 1e-6)
        log_score += math.log(max(p, 1e-9))
    if isinstance(secondary, list):
        for s in secondary:
            p = secondary_probs.get(s, 1e-6)
            log_score += math.log(max(p, 1e-9))
    else:
        p = secondary_probs.get(int(secondary), 1e-6)
        log_score += math.log(max(p, 1e-9))
    return log_score

# ── Initial scores from feature row ──────────────────────────────────────────
def _initial_scores_from_feature(
    db,
    feature_collection: str,
    mains_count: int,
    secondary_count: int,
    secondary_offset: int,
) -> Tuple[Dict[int, float], Dict[int, float]]:
    from pymongo import ASCENDING as ASC
    doc = db[feature_collection].find_one({}, sort=[("source_index", ASC)])
    if not doc:
        return {}, {}
    frequency = list(doc.get("frequency") or [])
    gap       = list(doc.get("gap") or [])

    def _build(offset: int, count: int, number_base: int) -> Dict[int, float]:
        freqs = []
        gaps  = []
        for i in range(count):
            idx = offset + i
            freqs.append(int(frequency[idx]) if idx < len(frequency) else 0)
            gaps.append(gap[idx] if idx < len(gap) else None)
        max_freq   = max(freqs) if any(f > 0 for f in freqs) else 1
        valid_gaps = [g for g in gaps if g is not None]
        max_gap    = max(valid_gaps) + 1 if valid_gaps else 1
        result: Dict[int, float] = {}
        for i in range(count):
            n            = number_base + i
            freq_norm    = freqs[i] / max_freq
            g            = gaps[i]
            recency_norm = 0.0 if g is None else 1.0 - (g / max_gap)
            result[n]    = max(0.4 * freq_norm + 0.6 * recency_norm, 1e-6)
        return result

    mains_probs     = _build(0,               mains_count,     1)
    secondary_probs = _build(secondary_offset, secondary_count, 0 if secondary_offset >= mains_count else 1)
    return mains_probs, secondary_probs

# ── Generic bootstrap: store permanent tickets ────────────────────────────────
def _bootstrap_tickets(
    db,
    tickets_collection: str,
    lottery_key: str,
    ticket_generator: Generator[Tuple, None, None],
    doc_builder: Callable,
    batch_size: int = 5000,
    progress_cb: Optional[Callable[[int], None]] = None,
) -> Tuple[int, int]:
    """Insert permanent ticket docs. Returns (total_inserted, total_skipped)."""
    coll = db[tickets_collection]
    now  = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    total_inserted = total_skipped = 0
    batch: List[Dict] = []

    for ticket_tuple in ticket_generator:
        batch.append(doc_builder(ticket_tuple[0], ticket_tuple, now))
        if len(batch) >= batch_size:
            try:
                r = coll.insert_many(batch, ordered=False)
                total_inserted += len(r.inserted_ids)
            except Exception as e:
                ins = getattr(getattr(e, "details", None), "get", lambda *a: 0)("nInserted", 0)
                total_inserted += ins
            batch.clear()
            if progress_cb:
                progress_cb(total_inserted + total_skipped)

    if batch:
        try:
            r = coll.insert_many(batch, ordered=False)
            total_inserted += len(r.inserted_ids)
        except Exception as e:
            ins = getattr(getattr(e, "details", None), "get", lambda *a: 0)("nInserted", 0)
            total_inserted += ins

    logger.info("[%s] tickets stored: inserted=%d skipped=%d",
                lottery_key, total_inserted, total_skipped)
    return total_inserted, total_skipped

# ── Generic ranking snapshot save ────────────────────────────────────────────
def _save_ranking_snapshot(
    db,
    rankings_collection: str,
    draw_id: str,
    draw_date: Optional[str],
    tickets_collection: str,
    lottery_key: str,
    secondary_field: str,
    mains_probs: Dict[int, float],
    secondary_probs: Dict[int, float],
    batch_size: int = 5000,
) -> int:
    """
    Compute score for every ticket, sort by score DESC to assign rank,
    save full snapshot {draw_id, draw_date, ranked_at, scores:[{position,score,rank}]}
    to rankings_collection.

    Returns total tickets ranked.
    """
    tickets_coll  = db[tickets_collection]
    rankings_coll = db[rankings_collection]
    now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    # Build list of (position, score)
    scored: List[Tuple[int, float]] = []
    for doc in tickets_coll.find(
        {"lottery": lottery_key},
        projection={"_id": 0, "position": 1, "mains": 1, secondary_field: 1},
    ):
        secondary = doc[secondary_field]
        score = _compute_score(doc["mains"], secondary, mains_probs, secondary_probs)
        scored.append((doc["position"], score))

    # Sort DESC by score → assign rank
    scored.sort(key=lambda x: x[1], reverse=True)
    scores_list = [
        {"position": pos, "score": round(score, 8), "rank": rank + 1}
        for rank, (pos, score) in enumerate(scored)
    ]

    # Upsert snapshot
    rankings_coll.replace_one(
        {"draw_id": draw_id},
        {
            "draw_id":   draw_id,
            "draw_date": draw_date,
            "ranked_at": now,
            "total":     len(scores_list),
            "scores":    scores_list,
        },
        upsert=True,
    )
    logger.info("[%s] ranking snapshot saved draw_id=%s total=%d",
                lottery_key, draw_id, len(scores_list))
    return len(scores_list)

# ── Euromillones ──────────────────────────────────────────────────────────────
def _euromillones_ticket_gen() -> Generator[Tuple, None, None]:
    pos = 0
    for mains in combinations(EUROMILLONES_MAINS, 5):
        for stars in combinations(EUROMILLONES_STARS, 2):
            pos += 1
            yield pos, mains, stars

def bootstrap_euromillones_tickets(
    db,
    progress_cb: Optional[Callable[[int], None]] = None,
) -> Dict[str, Any]:
    """
    One-time: store ALL Euromillones ticket combinations permanently.
    Then save initial ranking snapshot (draw_id='bootstrap') from first feature row.
    Safe to re-run — skips already-inserted tickets.
    """
    def doc_builder(position: int, ticket_tuple: Tuple, now: str) -> Dict:
        _, mains, stars = ticket_tuple
        return {
            "lottery":  "euromillones",
            "position": position,
            "mains":    list(mains),
            "stars":    list(stars),
            "tier":     _ticket_tier_mains(list(mains)),
        }

    inserted, skipped = _bootstrap_tickets(
        db, "euromillones_tickets", "euromillones",
        _euromillones_ticket_gen(), doc_builder, progress_cb=progress_cb,
    )

    # Initial ranking from first feature row
    mains_probs, stars_probs = _initial_scores_from_feature(
        db, "euromillones_feature", 50, 12, 50,
    )
    ranked = _save_ranking_snapshot(
        db, "euromillones_rankings", "bootstrap", None,
        "euromillones_tickets", "euromillones", "stars",
        mains_probs, stars_probs,
    )
    return {"total_tickets": inserted + skipped, "total_inserted": inserted,
            "total_skipped": skipped, "initial_ranking_saved": ranked}

def update_euromillones_ranking(
    db,
    draw_id: str,
    draw_date: Optional[str],
    mains_probs: Dict[int, float],
    stars_probs: Dict[int, float],
) -> int:
    """
    Compute new scores from ML probabilities, save ranking snapshot for this draw.
    Called after every draw pipeline. Returns tickets ranked.
    """
    return _save_ranking_snapshot(
        db, "euromillones_rankings", draw_id, draw_date,
        "euromillones_tickets", "euromillones", "stars",
        mains_probs, stars_probs,
    )

def compare_euromillones_from_db(
    db,
    current_id: str,
    pre_id: str,
    main_draw: List[int],
    star_draw: List[int],
    prize_map: Dict[Tuple[int, int], float],
    draw_date: Optional[str],
    ticket_cost_eur: float = 2.50,
) -> Dict[str, Any]:
    """
    Load ranking for pre_id, join with tickets, stream in rank order,
    find jackpot (5+2) position.
    """
    rankings_coll = db["euromillones_rankings"]
    tickets_coll  = db["euromillones_tickets"]

    ranking_doc = rankings_coll.find_one({"draw_id": pre_id})
    if not ranking_doc:
        raise ValueError(f"No ranking snapshot found for draw_id={pre_id!r}")

    scores_list: List[Dict] = ranking_doc.get("scores") or []
    if not scores_list:
        raise ValueError(f"Ranking snapshot for draw_id={pre_id!r} is empty")

    # Build position → ticket lookup
    ticket_map: Dict[int, Dict] = {
        doc["position"]: doc
        for doc in tickets_coll.find(
            {"lottery": "euromillones"},
            projection={"_id": 0, "position": 1, "mains": 1, "stars": 1},
        )
    }

    main_set = set(main_draw)
    star_set  = set(star_draw)
    _CATEGORY_ORDER = [
        (5,2),(5,1),(5,0),(4,2),(4,1),(4,0),
        (3,2),(3,1),(3,0),(2,2),(2,1),(1,2),(2,0),
    ]
    _CATEGORY_LABELS = [
        "1th(5+2)","2th(5+1)","3th(5+0)","4th(4+2)","5th(4+1)","6th(4+0)",
        "7th(3+2)","8th(3+1)","9th(3+0)","10th(2+2)","11th(2+1)","12th(1+2)","13th(2+0)",
    ]

    category_stats: Dict[Tuple[int,int], Tuple[int,float]] = {}
    total_earning = 0.0
    jackpot_position: Optional[int] = None
    second_positions: List[int] = []
    third_positions:  List[int] = []
    fourth_positions: List[int] = []

    # scores_list is already sorted by rank ASC (rank=1 first)
    for entry in scores_list:
        rank = entry["rank"]
        pos  = entry["position"]
        ticket = ticket_map.get(pos)
        if not ticket:
            continue
        hits_main = sum(1 for n in ticket["mains"] if n in main_set)
        hits_star = sum(1 for n in ticket["stars"] if n in star_set)
        prize = prize_map.get((hits_main, hits_star), 0.0)
        total_earning += prize
        key  = (hits_main, hits_star)
        prev = category_stats.get(key, (0, 0.0))
        category_stats[key] = (prev[0] + 1, prev[1] + prize)
        if hits_main == 5 and hits_star == 1: second_positions.append(rank)
        elif hits_main == 5 and hits_star == 0: third_positions.append(rank)
        elif hits_main == 4 and hits_star == 2: fourth_positions.append(rank)
        if hits_main == 5 and hits_star == 2:
            jackpot_position = rank
            break

    if jackpot_position is None:
        raise ValueError("Jackpot (5+2) not found in ranking snapshot")

    categories_out: List[Dict] = []
    for i, (hm, hs) in enumerate(_CATEGORY_ORDER):
        label = _CATEGORY_LABELS[i] if i < len(_CATEGORY_LABELS) else f"{hm}+{hs}"
        count, earning = category_stats.get((hm, hs), (0, 0.0))
        categories_out.append({"category": label, "count": count, "earning": round(earning, 2)})
    for (hm, hs), (count, earning) in category_stats.items():
        if (hm, hs) not in set(_CATEGORY_ORDER):
            categories_out.append({"category": f"{hm}+{hs}", "count": count, "earning": round(earning, 2)})

    return {
        "current_id": current_id, "date": draw_date, "pre_id": pre_id,
        "jackpot_position": jackpot_position,
        "second_positions": second_positions,
        "third_positions":  third_positions,
        "fourth_positions": fourth_positions,
        "categories":       categories_out,
        "total_tickets":    jackpot_position,
        "earning":          round(total_earning, 2),
        "ticket_cost":      round(jackpot_position * ticket_cost_eur, 2),
        "source":           "db",
    }

# ── El Gordo ──────────────────────────────────────────────────────────────────
def _el_gordo_ticket_gen() -> Generator[Tuple, None, None]:
    pos = 0
    for mains in combinations(EL_GORDO_MAINS, 5):
        for clave in EL_GORDO_CLAVES:
            pos += 1
            yield pos, mains, clave

def bootstrap_el_gordo_tickets(
    db,
    progress_cb: Optional[Callable[[int], None]] = None,
) -> Dict[str, Any]:
    def doc_builder(position: int, ticket_tuple: Tuple, now: str) -> Dict:
        _, mains, clave = ticket_tuple
        return {
            "lottery":  "el_gordo",
            "position": position,
            "mains":    list(mains),
            "clave":    int(clave),
            "tier":     _ticket_tier_mains(list(mains)),
        }

    inserted, skipped = _bootstrap_tickets(
        db, "el_gordo_tickets", "el_gordo",
        _el_gordo_ticket_gen(), doc_builder, progress_cb=progress_cb,
    )
    mains_probs, clave_probs = _initial_scores_from_feature(
        db, "el_gordo_feature", 54, 10, 54,
    )
    ranked = _save_ranking_snapshot(
        db, "el_gordo_rankings", "bootstrap", None,
        "el_gordo_tickets", "el_gordo", "clave",
        mains_probs, clave_probs,
    )
    return {"total_tickets": inserted + skipped, "total_inserted": inserted,
            "total_skipped": skipped, "initial_ranking_saved": ranked}

def update_el_gordo_ranking(
    db,
    draw_id: str,
    draw_date: Optional[str],
    mains_probs: Dict[int, float],
    clave_probs: Dict[int, float],
) -> int:
    return _save_ranking_snapshot(
        db, "el_gordo_rankings", draw_id, draw_date,
        "el_gordo_tickets", "el_gordo", "clave",
        mains_probs, clave_probs,
    )

def compare_el_gordo_from_db(
    db,
    current_id: str,
    pre_id: str,
    main_draw: List[int],
    clave_draw: int,
    draw_date: Optional[str],
    ticket_cost_eur: float = 1.50,
) -> Dict[str, Any]:
    rankings_coll = db["el_gordo_rankings"]
    tickets_coll  = db["el_gordo_tickets"]

    ranking_doc = rankings_coll.find_one({"draw_id": pre_id})
    if not ranking_doc:
        raise ValueError(f"No ranking snapshot found for draw_id={pre_id!r}")

    ticket_map: Dict[int, Dict] = {
        doc["position"]: doc
        for doc in tickets_coll.find(
            {"lottery": "el_gordo"},
            projection={"_id": 0, "position": 1, "mains": 1, "clave": 1},
        )
    }

    main_set = set(main_draw)
    category_stats: Dict[Tuple[int,int], Tuple[int,int]] = {}
    jackpot_position: Optional[int] = None
    pos_2th = pos_3th = pos_4th = None

    for entry in (ranking_doc.get("scores") or []):
        rank   = entry["rank"]
        ticket = ticket_map.get(entry["position"])
        if not ticket:
            continue
        hits_main  = sum(1 for n in ticket["mains"] if n in main_set)
        hits_clave = 1 if ticket["clave"] == clave_draw else 0
        key = (hits_main, hits_clave)
        prev_count, prev_first = category_stats.get(key, (0, rank))
        category_stats[key] = (prev_count + 1, prev_first if prev_count > 0 else rank)
        if hits_main == 5 and hits_clave == 1:
            jackpot_position = rank; break
        if hits_main == 5 and hits_clave == 0 and pos_2th is None: pos_2th = rank
        if hits_main == 4 and hits_clave == 1 and pos_3th is None: pos_3th = rank
        if hits_main == 4 and hits_clave == 0 and pos_4th is None: pos_4th = rank

    if jackpot_position is None:
        raise ValueError("Jackpot (5+clave) not found in ranking snapshot")

    categories_out = [
        {"category": f"{hm}+{hc}", "main_hits": hm, "clave_hit": hc,
         "first_position": fp, "count": cnt}
        for (hm, hc), (cnt, fp) in sorted(category_stats.items(), key=lambda x: (-x[0][0], -x[0][1]))
    ]
    return {
        "current_id": current_id, "date": draw_date, "pre_id": pre_id,
        "jackpot_position": jackpot_position,
        "pos_2th": pos_2th, "pos_3th": pos_3th, "pos_4th": pos_4th,
        "categories": categories_out, "source": "db",
    }

# ── La Primitiva ──────────────────────────────────────────────────────────────
def _la_primitiva_ticket_gen(resume_from: int = 0) -> Generator[Tuple, None, None]:
    """Yield (position, mains_tuple, reintegro). Skips positions <= resume_from."""
    pos = 0
    for mains in combinations(LA_PRIMITIVA_MAINS, 6):
        block_start = pos + 1
        block_end   = pos + len(LA_PRIMITIVA_REINS)
        # Skip entire mains block if all reintegros already inserted
        if block_end <= resume_from:
            pos = block_end
            continue
        for reintegro in LA_PRIMITIVA_REINS:
            pos += 1
            if pos <= resume_from:
                continue
            yield pos, mains, reintegro

def bootstrap_la_primitiva_tickets(
    db,
    progress_cb: Optional[Callable[[int], None]] = None,
) -> Dict[str, Any]:
    def doc_builder(position: int, ticket_tuple: Tuple, now: str) -> Dict:
        _, mains, reintegro = ticket_tuple
        return {
            "lottery":   "la_primitiva",
            "position":  position,
            "mains":     list(mains),
            "reintegro": int(reintegro),
            "tier":      _ticket_tier_mains(list(mains)),
        }

    # Find resume point before starting generator
    coll = db["la_primitiva_tickets"]
    last = coll.find_one(
        {"lottery": "la_primitiva"},
        sort=[("position", -1)],
        projection={"position": 1},
    )
    resume_from = int(last["position"]) if last else 0
    logger.info("[la_primitiva] resuming from position %d", resume_from)

    inserted, skipped = _bootstrap_tickets(
        db, "la_primitiva_tickets", "la_primitiva",
        _la_primitiva_ticket_gen(resume_from=resume_from),
        doc_builder, progress_cb=progress_cb,
    )
    mains_probs, reintegro_probs = _initial_scores_from_feature(
        db, "la_primitiva_feature", 49, 10, 98,
    )
    ranked = _save_ranking_snapshot(
        db, "la_primitiva_rankings", "bootstrap", None,
        "la_primitiva_tickets", "la_primitiva", "reintegro",
        mains_probs, reintegro_probs,
    )
    return {"total_tickets": inserted + skipped, "total_inserted": inserted,
            "total_skipped": skipped, "initial_ranking_saved": ranked}

def update_la_primitiva_ranking(
    db,
    draw_id: str,
    draw_date: Optional[str],
    mains_probs: Dict[int, float],
    reintegro_probs: Dict[int, float],
) -> int:
    return _save_ranking_snapshot(
        db, "la_primitiva_rankings", draw_id, draw_date,
        "la_primitiva_tickets", "la_primitiva", "reintegro",
        mains_probs, reintegro_probs,
    )

def compare_la_primitiva_from_db(
    db,
    current_id: str,
    pre_id: str,
    main_draw: List[int],
    reintegro_draw: int,
    complementario_draw: Optional[int],
    draw_date: Optional[str],
    ticket_cost_eur: float = 1.0,
) -> Dict[str, Any]:
    rankings_coll = db["la_primitiva_rankings"]
    tickets_coll  = db["la_primitiva_tickets"]

    ranking_doc = rankings_coll.find_one({"draw_id": pre_id})
    if not ranking_doc:
        raise ValueError(f"No ranking snapshot found for draw_id={pre_id!r}")

    ticket_map: Dict[int, Dict] = {
        doc["position"]: doc
        for doc in tickets_coll.find(
            {"lottery": "la_primitiva"},
            projection={"_id": 0, "position": 1, "mains": 1, "reintegro": 1},
        )
    }

    main_set = set(main_draw)
    category_stats: Dict[Tuple[int,int], Tuple[int,int]] = {}
    jackpot_position: Optional[int] = None
    pos_2th = pos_3th = pos_4th = None

    for entry in (ranking_doc.get("scores") or []):
        rank   = entry["rank"]
        ticket = ticket_map.get(entry["position"])
        if not ticket:
            continue
        hits_main = sum(1 for n in ticket["mains"] if n in main_set)
        hits_rein = 1 if ticket["reintegro"] == reintegro_draw else 0
        key = (hits_main, hits_rein)
        prev_count, prev_first = category_stats.get(key, (0, rank))
        category_stats[key] = (prev_count + 1, prev_first if prev_count > 0 else rank)
        if hits_main == 6:
            jackpot_position = rank; break
        if (hits_main == 5 and complementario_draw is not None
                and complementario_draw in ticket["mains"] and pos_2th is None):
            pos_2th = rank
        if hits_main == 5 and pos_3th is None: pos_3th = rank
        if hits_main == 4 and pos_4th is None: pos_4th = rank

    if jackpot_position is None:
        raise ValueError("Jackpot (6 mains) not found in ranking snapshot")

    categories_out = [
        {"category": f"{hm}+{hr}", "main_hits": hm, "reintegro_hit": hr,
         "first_position": fp, "count": cnt}
        for (hm, hr), (cnt, fp) in sorted(category_stats.items(), key=lambda x: (-x[0][0], -x[0][1]))
    ]
    return {
        "current_id": current_id, "date": draw_date, "pre_id": pre_id,
        "jackpot_position": jackpot_position,
        "pos_2th": pos_2th, "pos_3th": pos_3th, "pos_4th": pos_4th,
        "categories": categories_out,
        "total_categories": len(categories_out),
        "source": "db",
    }

# ── Helpers ───────────────────────────────────────────────────────────────────
def probs_list_to_dict(probs: list) -> Dict[int, float]:
    return {
        int(x["number"]): float(x.get("p", 0.0))
        for x in (probs or [])
        if x.get("number") is not None
    }
