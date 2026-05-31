"""
DB-based ticket pool — permanent tickets + per-draw probability snapshots.

Architecture
============
Tickets (permanent, never change):
  euromillones_tickets  { position, mains, stars, tier }
  el_gordo_tickets      { position, mains, clave, tier }
  la_primitiva_tickets  { position, mains, reintegro, tier }

Per-draw probability snapshots (tiny — ~2KB per draw):
  euromillones_draw_probs  { draw_id, draw_date, mains_probs:{n:p}, stars_probs:{n:p} }
  el_gordo_draw_probs      { draw_id, draw_date, mains_probs:{n:p}, clave_probs:{n:p} }
  la_primitiva_draw_probs  { draw_id, draw_date, mains_probs:{n:p}, rein_probs:{n:p} }

Compare flow (streaming — safe for VPS):
  1. Load prob snapshot for pre_id from draw_probs collection.
  2. Stream tickets in batches; spill sorted batches to temp disk files.
  3. K-way merge by score (constant RAM); find jackpot without loading all tickets.

This avoids storing 139M scores per draw (which would be terabytes).
Instead we store ~60 numbers per draw and recompute scores at query time.
"""
from __future__ import annotations

import heapq
import json
import logging
import math
import os
import shutil
import tempfile
from datetime import datetime as dt
from itertools import combinations
from typing import Any, Callable, Dict, Generator, List, Optional, Sequence, Tuple

logger = logging.getLogger("lottery.ticket_db")

# Score + spill batches to disk during compare (avoids loading 140M tickets into RAM).
COMPARE_SCORE_BATCH_SIZE = int(os.getenv("COMPARE_SCORE_BATCH_SIZE", "200000"))

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
            current_run += 1; longest_run = max(longest_run, current_run)
        else:
            current_run = 1
    if longest_run >= 4: score += 3
    elif longest_run == 3: score += 1
    if len({n // 10 for n in nums}) == 1: score += 2
    if len({n % 10  for n in nums}) == 1: score += 2
    odds = sum(1 for n in nums if n % 2 == 1)
    if odds == len(nums) or odds == 0: score += 2
    return 3 if score >= 5 else 2 if score >= 3 else 1 if score >= 1 else 0


# ── Score computation ─────────────────────────────────────────────────────────
def _compute_score(
    mains: List[int],
    secondary: "int | List[int]",
    mains_probs: Dict[int, float],
    secondary_probs: Dict[int, float],
) -> float:
    if not mains_probs:
        return 0.0
    s = 0.0
    for n in mains:
        s += math.log(max(mains_probs.get(n, 1e-6), 1e-9))
    if isinstance(secondary, list):
        for x in secondary:
            s += math.log(max(secondary_probs.get(x, 1e-6), 1e-9))
    else:
        s += math.log(max(secondary_probs.get(int(secondary), 1e-6), 1e-9))
    return s


def _write_score_batch(path: str, batch: List[Tuple[float, int, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        for score, pos, payload in batch:
            fh.write(json.dumps([score, pos, payload], separators=(",", ":")) + "\n")


class _JsonlBatchReader:
    """One sorted batch on disk; only the current line is held in memory."""

    __slots__ = ("path", "_fh", "head")

    def __init__(self, path: str):
        self.path = path
        self._fh = open(path, encoding="utf-8")
        self.head = self._read_next()

    def _read_next(self) -> Optional[Tuple[float, int, Any]]:
        fh = self._fh
        if fh is None:
            return None
        while True:
            line = fh.readline()
            if not line:
                fh.close()
                self._fh = None
                return None
            line = line.strip()
            if not line:
                continue
            score, pos, payload = json.loads(line)
            return float(score), int(pos), payload

    def advance(self) -> None:
        self.head = self._read_next()

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None


def _stream_tickets_by_score(
    coll,
    lottery: str,
    projection: dict,
    score_fn: Callable[[dict], float],
    payload_fn: Callable[[dict], Any],
    *,
    batch_size: int = COMPARE_SCORE_BATCH_SIZE,
) -> Generator[Tuple[int, float, Any], None, None]:
    """
    Yield (rank, score, payload) in global score-desc / position-asc order.
    Uses temp disk batches + line-by-line k-way merge (constant RAM).
    """
    tmp_dir = tempfile.mkdtemp(prefix=f"compare_{lottery}_")
    batch_paths: List[str] = []
    readers: List[_JsonlBatchReader] = []
    try:
        batch: List[Tuple[float, int, Any]] = []
        cursor = coll.find({"lottery": lottery}, projection=projection).batch_size(5000)
        for doc in cursor:
            pos = int(doc.get("position") or 0)
            score = score_fn(doc)
            batch.append((score, pos, payload_fn(doc)))
            if len(batch) >= batch_size:
                batch.sort(key=lambda x: (-x[0], x[1]))
                path = os.path.join(tmp_dir, f"{len(batch_paths)}.jsonl")
                _write_score_batch(path, batch)
                batch_paths.append(path)
                batch = []
                logger.info("[compare-stream/%s] spilled batch %d", lottery, len(batch_paths))
        if batch:
            batch.sort(key=lambda x: (-x[0], x[1]))
            path = os.path.join(tmp_dir, f"{len(batch_paths)}.jsonl")
            _write_score_batch(path, batch)
            batch_paths.append(path)

        if not batch_paths:
            return

        readers = [_JsonlBatchReader(p) for p in batch_paths]
        heap: List[Tuple[float, int, int]] = []
        for ri, reader in enumerate(readers):
            if reader.head is not None:
                sc, pos, _ = reader.head
                heap.append((-sc, pos, ri))
        heapq.heapify(heap)

        rank = 0
        while heap:
            neg_sc, pos, ri = heapq.heappop(heap)
            reader = readers[ri]
            assert reader.head is not None
            sc, pos, payload = reader.head
            reader.advance()
            rank += 1
            yield rank, sc, payload
            if reader.head is not None:
                sc2, pos2, _ = reader.head
                heapq.heappush(heap, (-sc2, pos2, ri))
    finally:
        for reader in readers:
            reader.close()
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ── Initial scores from feature row ──────────────────────────────────────────
def _initial_scores_from_feature(
    db, feature_collection: str,
    mains_count: int, secondary_count: int, secondary_offset: int,
) -> Tuple[Dict[int, float], Dict[int, float]]:
    from pymongo import DESCENDING as DESC
    # Use the LATEST draw (highest source_index) so bootstrap scores reflect
    # the most recent frequency/gap data, not the oldest draw.
    doc = db[feature_collection].find_one({}, sort=[("source_index", DESC)])
    if not doc:
        return {}, {}
    frequency = list(doc.get("frequency") or [])
    gap       = list(doc.get("gap") or [])

    def _build(offset: int, count: int, base: int) -> Dict[int, float]:
        freqs = [int(frequency[offset+i]) if offset+i < len(frequency) else 0 for i in range(count)]
        gaps  = [gap[offset+i] if offset+i < len(gap) else None for i in range(count)]
        mf = max(freqs) if any(f > 0 for f in freqs) else 1
        vg = [g for g in gaps if g is not None]
        mg = max(vg) + 1 if vg else 1
        return {
            base + i: max(0.4 * freqs[i]/mf + 0.6 * (0.0 if gaps[i] is None else 1.0 - gaps[i]/mg), 1e-6)
            for i in range(count)
        }

    return _build(0, mains_count, 1), _build(secondary_offset, secondary_count, 0 if secondary_offset >= mains_count else 1)


# ── Save probability snapshot (tiny — ~2KB per draw) ─────────────────────────
def save_draw_probs(
    db,
    collection: str,
    draw_id: str,
    draw_date: Optional[str],
    mains_probs: Dict[int, float],
    secondary_probs: Dict[int, float],
    secondary_key: str = "secondary_probs",
) -> None:
    """Save per-number probabilities for one draw. Upserts by draw_id."""
    db[collection].replace_one(
        {"draw_id": draw_id},
        {
            "draw_id":        draw_id,
            "draw_date":      draw_date,
            "saved_at":       dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "mains_probs":    {str(k): v for k, v in mains_probs.items()},
            secondary_key:    {str(k): v for k, v in secondary_probs.items()},
        },
        upsert=True,
    )


def load_draw_probs(
    db, collection: str, draw_id: str, secondary_key: str = "secondary_probs",
) -> Tuple[Dict[int, float], Dict[int, float]]:
    """Load probability snapshot for a draw. Returns ({n:p}, {n:p})."""
    doc = None
    draw_key = str(draw_id or "").strip()
    for key in (draw_key, int(draw_key) if draw_key.isdigit() else None):
        if key is None:
            continue
        doc = db[collection].find_one({"draw_id": key})
        if doc:
            break
    if not doc:
        return {}, {}
    mains = {int(k): float(v) for k, v in (doc.get("mains_probs") or {}).items()}
    sec   = {int(k): float(v) for k, v in (doc.get(secondary_key) or {}).items()}
    return mains, sec


# ── Generic bootstrap ─────────────────────────────────────────────────────────
def _bootstrap_tickets(
    db, tickets_collection: str, lottery_key: str,
    ticket_generator: Generator[Tuple, None, None],
    doc_builder: Callable,
    batch_size: int = 5000,
    progress_cb: Optional[Callable[[int], None]] = None,
) -> Tuple[int, int]:
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
                total_skipped  += len(batch) - ins
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
            total_skipped  += len(batch) - ins

    logger.info("[%s] tickets stored: inserted=%d skipped=%d", lottery_key, total_inserted, total_skipped)
    return total_inserted, total_skipped


# ── Euromillones ──────────────────────────────────────────────────────────────
def _euromillones_ticket_gen() -> Generator[Tuple, None, None]:
    pos = 0
    for mains in combinations(EUROMILLONES_MAINS, 5):
        for stars in combinations(EUROMILLONES_STARS, 2):
            pos += 1
            yield pos, mains, stars


def bootstrap_euromillones_tickets(db, progress_cb=None) -> Dict[str, Any]:
    def doc_builder(position, t, now):
        _, mains, stars = t
        return {"lottery": "euromillones", "position": position,
                "mains": list(mains), "stars": list(stars),
                "tier": _ticket_tier_mains(list(mains))}

    inserted, skipped = _bootstrap_tickets(
        db, "euromillones_tickets", "euromillones",
        _euromillones_ticket_gen(), doc_builder, progress_cb=progress_cb,
    )
    # Save initial prob snapshot from first feature row
    mp, sp = _initial_scores_from_feature(db, "euromillones_feature", 50, 12, 50)
    save_draw_probs(db, "euromillones_draw_probs", "bootstrap", None, mp, sp, "stars_probs")
    return {"total_tickets": inserted + skipped, "total_inserted": inserted, "total_skipped": skipped}


def update_euromillones_ranking(db, draw_id: str, draw_date: Optional[str],
                                 mains_probs: Dict[int, float], stars_probs: Dict[int, float]) -> int:
    """Save probability snapshot for this draw. Called after every pipeline run."""
    save_draw_probs(db, "euromillones_draw_probs", draw_id, draw_date, mains_probs, stars_probs, "stars_probs")
    logger.info("[euromillones] draw_probs saved draw_id=%s", draw_id)
    return len(mains_probs) + len(stars_probs)


def compare_euromillones_from_db(
    db, current_id: str, pre_id: str,
    main_draw: List[int], star_draw: List[int],
    prize_map: Dict[Tuple[int, int], float],
    draw_date: Optional[str], ticket_cost_eur: float = 2.50,
) -> Dict[str, Any]:
    """Stream-score tickets (disk-spill batches) — safe for full 140M pool on VPS."""
    mains_probs, stars_probs = load_draw_probs(db, "euromillones_draw_probs", pre_id, "stars_probs")
    if not mains_probs:
        raise ValueError(f"No probability snapshot found for draw_id={pre_id!r}")

    tickets_coll = db["euromillones_tickets"]
    main_set = set(main_draw)
    star_set = set(star_draw)

    def score_fn(doc: dict) -> float:
        return _compute_score(doc["mains"], doc["stars"], mains_probs, stars_probs)

    def payload_fn(doc: dict) -> Tuple[List[int], List[int]]:
        return doc["mains"], doc["stars"]

    _CATEGORY_ORDER = [(5, 2), (5, 1), (5, 0), (4, 2), (4, 1), (4, 0), (3, 2), (3, 1), (3, 0), (2, 2), (2, 1), (1, 2), (2, 0)]
    _CATEGORY_LABELS = ["1th(5+2)", "2th(5+1)", "3th(5+0)", "4th(4+2)", "5th(4+1)", "6th(4+0)",
                        "7th(3+2)", "8th(3+1)", "9th(3+0)", "10th(2+2)", "11th(2+1)", "12th(1+2)", "13th(2+0)"]

    category_stats: Dict[Tuple[int, int], Tuple[int, float]] = {}
    total_earning = 0.0
    jackpot_position: Optional[int] = None
    second_positions: List[int] = []
    third_positions: List[int] = []
    fourth_positions: List[int] = []

    logger.info("[compare-stream/euromillones] start current_id=%s pre_id=%s", current_id, pre_id)
    for rank, _, (mains, stars) in _stream_tickets_by_score(
        tickets_coll,
        "euromillones",
        {"_id": 0, "position": 1, "mains": 1, "stars": 1},
        score_fn,
        payload_fn,
    ):
        hits_main = sum(1 for n in mains if n in main_set)
        hits_star = sum(1 for n in stars if n in star_set)
        prize = prize_map.get((hits_main, hits_star), 0.0)
        total_earning += prize
        key = (hits_main, hits_star)
        prev = category_stats.get(key, (0, 0.0))
        category_stats[key] = (prev[0] + 1, prev[1] + prize)
        if hits_main == 5 and hits_star == 1:
            second_positions.append(rank)
        elif hits_main == 5 and hits_star == 0:
            third_positions.append(rank)
        elif hits_main == 4 and hits_star == 2:
            fourth_positions.append(rank)
        if hits_main == 5 and hits_star == 2:
            jackpot_position = rank
            break

    if jackpot_position is None:
        raise ValueError("Jackpot (5+2) not found")

    categories_out = []
    for i, (hm, hs) in enumerate(_CATEGORY_ORDER):
        label = _CATEGORY_LABELS[i] if i < len(_CATEGORY_LABELS) else f"{hm}+{hs}"
        count, earning = category_stats.get((hm, hs), (0, 0.0))
        categories_out.append({"category": label, "count": count, "earning": round(earning, 2)})

    logger.info("[compare-stream/euromillones] done jackpot_position=%s", jackpot_position)
    return {
        "current_id": current_id, "date": draw_date, "pre_id": pre_id,
        "jackpot_position": jackpot_position,
        "second_positions": second_positions, "third_positions": third_positions,
        "fourth_positions": fourth_positions, "categories": categories_out,
        "total_tickets": jackpot_position, "earning": round(total_earning, 2),
        "ticket_cost": round(jackpot_position * ticket_cost_eur, 2), "source": "db",
    }


# ── El Gordo ──────────────────────────────────────────────────────────────────
def _el_gordo_ticket_gen() -> Generator[Tuple, None, None]:
    pos = 0
    for mains in combinations(EL_GORDO_MAINS, 5):
        for clave in EL_GORDO_CLAVES:
            pos += 1
            yield pos, mains, clave


def bootstrap_el_gordo_tickets(db, progress_cb=None) -> Dict[str, Any]:
    def doc_builder(position, t, now):
        _, mains, clave = t
        return {"lottery": "el_gordo", "position": position,
                "mains": list(mains), "clave": int(clave),
                "tier": _ticket_tier_mains(list(mains))}

    inserted, skipped = _bootstrap_tickets(
        db, "el_gordo_tickets", "el_gordo",
        _el_gordo_ticket_gen(), doc_builder, progress_cb=progress_cb,
    )
    mp, cp = _initial_scores_from_feature(db, "el_gordo_feature", 54, 10, 54)
    save_draw_probs(db, "el_gordo_draw_probs", "bootstrap", None, mp, cp, "clave_probs")
    return {"total_tickets": inserted + skipped, "total_inserted": inserted, "total_skipped": skipped}


def update_el_gordo_ranking(db, draw_id: str, draw_date: Optional[str],
                             mains_probs: Dict[int, float], clave_probs: Dict[int, float]) -> int:
    save_draw_probs(db, "el_gordo_draw_probs", draw_id, draw_date, mains_probs, clave_probs, "clave_probs")
    logger.info("[el_gordo] draw_probs saved draw_id=%s", draw_id)
    return len(mains_probs) + len(clave_probs)


def compare_el_gordo_from_db(
    db, current_id: str, pre_id: str,
    main_draw: List[int], clave_draw: int,
    draw_date: Optional[str], ticket_cost_eur: float = 1.50,
) -> Dict[str, Any]:
    mains_probs, clave_probs = load_draw_probs(db, "el_gordo_draw_probs", pre_id, "clave_probs")
    if not mains_probs:
        raise ValueError(f"No probability snapshot found for draw_id={pre_id!r}")

    tickets_coll = db["el_gordo_tickets"]
    main_set = set(main_draw)

    def score_fn(doc: dict) -> float:
        return _compute_score(doc["mains"], doc["clave"], mains_probs, clave_probs)

    def payload_fn(doc: dict) -> Tuple[List[int], int]:
        return doc["mains"], int(doc["clave"])

    category_stats: Dict[Tuple[int, int], Tuple[int, int]] = {}
    jackpot_position = pos_2th = pos_3th = pos_4th = pos_5th = pos_6th = None
    _tier_keys = ((5, 1), (5, 0), (4, 1), (4, 0), (3, 1), (3, 0), (2, 1), (2, 0), (0, 1))

    logger.info("[compare-stream/el-gordo] start current_id=%s pre_id=%s", current_id, pre_id)
    for rank, _, (mains, clave) in _stream_tickets_by_score(
        tickets_coll,
        "el_gordo",
        {"_id": 0, "position": 1, "mains": 1, "clave": 1},
        score_fn,
        payload_fn,
    ):
        hits_main = sum(1 for n in mains if n in main_set)
        hits_clave = 1 if clave == clave_draw else 0
        key = (hits_main, hits_clave)
        prev_count, prev_first = category_stats.get(key, (0, rank))
        category_stats[key] = (prev_count + 1, prev_first if prev_count > 0 else rank)
        if hits_main == 5 and hits_clave == 1 and jackpot_position is None:
            jackpot_position = rank
        elif hits_main == 5 and hits_clave == 0 and pos_2th is None:
            pos_2th = rank
        elif hits_main == 4 and hits_clave == 1 and pos_3th is None:
            pos_3th = rank
        elif hits_main == 4 and hits_clave == 0 and pos_4th is None:
            pos_4th = rank
        elif hits_main == 3 and hits_clave == 1 and pos_5th is None:
            pos_5th = rank
        elif hits_main == 3 and hits_clave == 0 and pos_6th is None:
            pos_6th = rank
        if jackpot_position is not None and all(k in category_stats for k in _tier_keys):
            break

    if jackpot_position is None:
        raise ValueError("Jackpot not found")

    categories_out = [
        {"category": f"{hm}+{hc}", "main_hits": hm, "clave_hit": hc, "first_position": fp, "count": cnt}
        for (hm, hc), (cnt, fp) in sorted(category_stats.items(), key=lambda x: (-x[0][0], -x[0][1]))
    ]
    logger.info("[compare-stream/el-gordo] done jackpot_position=%s", jackpot_position)
    return {
        "current_id": current_id, "date": draw_date, "pre_id": pre_id,
        "jackpot_position": jackpot_position,
        "pos_2th": pos_2th, "pos_3th": pos_3th, "pos_4th": pos_4th,
        "pos_5th": pos_5th, "pos_6th": pos_6th,
        "categories": categories_out, "source": "db",
    }


# ── La Primitiva ──────────────────────────────────────────────────────────────
def _la_primitiva_ticket_gen(resume_from: int = 0) -> Generator[Tuple, None, None]:
    pos = 0
    for mains in combinations(LA_PRIMITIVA_MAINS, 6):
        block_end = pos + len(LA_PRIMITIVA_REINS)
        if block_end <= resume_from:
            pos = block_end
            continue
        for rein in LA_PRIMITIVA_REINS:
            pos += 1
            if pos <= resume_from:
                continue
            yield pos, mains, rein


def bootstrap_la_primitiva_tickets(db, progress_cb=None) -> Dict[str, Any]:
    def doc_builder(position, t, now):
        _, mains, rein = t
        return {"lottery": "la_primitiva", "position": position,
                "mains": list(mains), "reintegro": int(rein),
                "tier": _ticket_tier_mains(list(mains))}

    coll = db["la_primitiva_tickets"]
    last = coll.find_one({"lottery": "la_primitiva"}, sort=[("position", -1)], projection={"position": 1})
    resume_from = int(last["position"]) if last else 0
    logger.info("[la_primitiva] resuming from position %d", resume_from)

    inserted, skipped = _bootstrap_tickets(
        db, "la_primitiva_tickets", "la_primitiva",
        _la_primitiva_ticket_gen(resume_from=resume_from),
        doc_builder, progress_cb=progress_cb,
    )
    mp, rp = _initial_scores_from_feature(db, "la_primitiva_feature", 49, 10, 98)
    save_draw_probs(db, "la_primitiva_draw_probs", "bootstrap", None, mp, rp, "rein_probs")
    return {"total_tickets": inserted + skipped, "total_inserted": inserted, "total_skipped": skipped}


def update_la_primitiva_ranking(db, draw_id: str, draw_date: Optional[str],
                                 mains_probs: Dict[int, float], rein_probs: Dict[int, float]) -> int:
    save_draw_probs(db, "la_primitiva_draw_probs", draw_id, draw_date, mains_probs, rein_probs, "rein_probs")
    logger.info("[la_primitiva] draw_probs saved draw_id=%s", draw_id)
    return len(mains_probs) + len(rein_probs)


def compare_la_primitiva_from_db(
    db, current_id: str, pre_id: str,
    main_draw: List[int], reintegro_draw: int,
    complementario_draw: Optional[int],
    draw_date: Optional[str], ticket_cost_eur: float = 1.0,
) -> Dict[str, Any]:
    mains_probs, rein_probs = load_draw_probs(db, "la_primitiva_draw_probs", pre_id, "rein_probs")
    if not mains_probs:
        raise ValueError(f"No probability snapshot found for draw_id={pre_id!r}")

    tickets_coll = db["la_primitiva_tickets"]
    main_set = set(main_draw)
    comp_set = {int(complementario_draw)} if complementario_draw is not None else set()

    def score_fn(doc: dict) -> float:
        return _compute_score(doc["mains"], doc["reintegro"], mains_probs, rein_probs)

    def payload_fn(doc: dict) -> Tuple[List[int], int]:
        return doc["mains"], int(doc["reintegro"])

    category_stats: Dict[Tuple[int, int], Tuple[int, int]] = {}
    special_position: Optional[int] = None
    pos_1th: Optional[int] = None
    pos_2th = pos_3th = pos_4th = pos_5th = None

    logger.info("[compare-stream/la-primitiva] start current_id=%s pre_id=%s", current_id, pre_id)
    for rank, _, (mains, rein) in _stream_tickets_by_score(
        tickets_coll,
        "la_primitiva",
        {"_id": 0, "position": 1, "mains": 1, "reintegro": 1},
        score_fn,
        payload_fn,
    ):
        hits_main = sum(1 for n in mains if n in main_set)
        hits_rein = 1 if rein == reintegro_draw else 0
        has_complementario = bool(comp_set and any(n in comp_set for n in mains))

        key: Optional[Tuple[int, int]] = None
        if hits_main == 6 and hits_rein:
            key = (6, 1)
        elif hits_main == 6:
            key = (6, 0)
        elif hits_main == 5 and has_complementario:
            key = (5, 1)
        elif hits_main == 5:
            key = (5, 0)
        elif hits_main == 4:
            key = (4, 0)
        elif hits_main == 3:
            key = (3, 0)

        if key is not None:
            prev_count, prev_first = category_stats.get(key, (0, rank))
            category_stats[key] = (prev_count + 1, prev_first if prev_count > 0 else rank)

        if hits_main == 6 and hits_rein and special_position is None:
            special_position = rank
        if hits_main == 6 and pos_1th is None:
            pos_1th = rank
        elif hits_main == 5 and has_complementario and pos_2th is None:
            pos_2th = rank
        elif hits_main == 5 and pos_3th is None:
            pos_3th = rank
        elif hits_main == 4 and pos_4th is None:
            pos_4th = rank
        elif hits_main == 3 and pos_5th is None:
            pos_5th = rank

        if special_position and pos_2th and pos_3th and pos_4th and pos_5th:
            break

    if special_position is None:
        raise ValueError("Jackpot (6 mains + reintegro) not found")

    ordered_keys: List[Tuple[Tuple[int, int], str]] = [
        ((6, 1), "Especial (6 aciertos + R)"),
        ((6, 0), "1ª (6 aciertos)"),
        ((5, 1), "2ª (5 + C)"),
        ((5, 0), "3ª (5 aciertos)"),
        ((4, 0), "4ª (4 aciertos)"),
        ((3, 0), "5ª (3 aciertos)"),
    ]
    categories_out: List[Dict[str, Any]] = []
    for cat_key, label in ordered_keys:
        hm, hr = cat_key
        count, first_pos = category_stats.get(cat_key, (0, 0))
        categories_out.append(
            {
                "category": label,
                "main_hits": hm,
                "reintegro_hit": hr,
                "first_position": first_pos,
                "count": count,
            }
        )

    logger.info(
        "[compare/la-primitiva] done jackpot=%s scanned=%s",
        special_position,
        rank,
    )
    return {
        "current_id": current_id,
        "date": draw_date,
        "pre_id": pre_id,
        "jackpot_position": special_position,
        "special_position": special_position,
        "pos_1th": pos_1th,
        "pos_2th": pos_2th,
        "pos_3th": pos_3th,
        "pos_4th": pos_4th,
        "pos_5th": pos_5th,
        "categories": categories_out,
        "total_categories": len(categories_out),
        "source": "db",
    }


# ── Helpers ───────────────────────────────────────────────────────────────────
def probs_list_to_dict(probs: list) -> Dict[int, float]:
    return {int(x["number"]): float(x.get("p", 0.0))
            for x in (probs or []) if x.get("number") is not None}
