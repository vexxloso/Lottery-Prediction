import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv  # type: ignore[import-untyped]
from pymongo import ASCENDING, MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "lottery")

# El Gordo ranges
MAIN_MIN, MAIN_MAX = 1, 54
CLAVE_MIN, CLAVE_MAX = 0, 9


def _get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI)


def _load_el_gordo_feature_docs() -> List[dict]:
    """
    Load all El Gordo feature documents sorted by draw_index ascending.

    Uses collection `el_gordo_draw_features`.
    """
    client = _get_mongo_client()
    try:
        db = client[MONGO_DB]
        coll = db["el_gordo_draw_features"]
        docs = list(
            coll.find(
                {},
                projection={
                    "draw_id": 1,
                    "draw_index": 1,
                    "draw_date": 1,
                    "main_numbers": 1,
                    "clave": 1,
                    "hot_main_numbers": 1,
                    "cold_main_numbers": 1,
                    "hot_clave": 1,
                    "cold_clave": 1,
                    "main_frequency_counts": 1,
                    "clave_frequency_counts": 1,
                },
            ).sort("draw_index", ASCENDING)
        )
        return docs
    finally:
        client.close()


def _select_history_window(
    docs: List[dict],
    cutoff_draw_id: Optional[str],
) -> Tuple[List[dict], int, str]:
    """
    Select the subset of feature docs up to the cutoff draw (inclusive).

    Returns:
        (history_docs, cutoff_draw_index, cutoff_draw_date)
    """
    if not docs:
        raise RuntimeError("No El Gordo feature rows available.")

    if cutoff_draw_id is None:
        last = docs[-1]
        cutoff_index = int(last.get("draw_index", 0))
        cutoff_date = str(last.get("draw_date") or "")
        return docs, cutoff_index, cutoff_date

    cutoff_doc = None
    for d in docs:
        if str(d.get("draw_id")) == str(cutoff_draw_id):
            cutoff_doc = d
            break

    if not cutoff_doc or "draw_index" not in cutoff_doc:
        raise RuntimeError(f"Unknown El Gordo draw_id: {cutoff_draw_id}")

    cutoff_index = int(cutoff_doc["draw_index"])
    cutoff_date = str(cutoff_doc.get("draw_date") or "")

    history = [d for d in docs if int(d.get("draw_index", -1)) <= cutoff_index]
    if not history:
        raise RuntimeError("No El Gordo feature rows found up to the selected cutoff.")

    return history, cutoff_index, cutoff_date


def _build_frequency_and_gap(
    history: List[dict],
    cutoff_draw_index: int,
) -> Tuple[Dict[int, float], Dict[int, float], Dict[int, float], Dict[int, float]]:
    """
    Compute simple frequency and gap scores for main and clave numbers.

    Frequency:
      appearances / total_draws  (0..1)

    Gap:
      draws since last appearance, normalised to 0..1 where 1 means "most overdue".
    """
    total_draws = len(history)
    if total_draws <= 0:
        raise RuntimeError("Empty El Gordo history window.")

    main_counts: Dict[int, int] = {n: 0 for n in range(MAIN_MIN, MAIN_MAX + 1)}
    clave_counts: Dict[int, int] = {c: 0 for c in range(CLAVE_MIN, CLAVE_MAX + 1)}

    main_last_seen: Dict[int, Optional[int]] = {n: None for n in main_counts.keys()}
    clave_last_seen: Dict[int, Optional[int]] = {c: None for c in clave_counts.keys()}

    for doc in history:
        idx = int(doc.get("draw_index", 0))
        mains = [int(n) for n in (doc.get("main_numbers") or [])]
        for n in mains:
            if MAIN_MIN <= n <= MAIN_MAX:
                main_counts[n] += 1
                main_last_seen[n] = idx

        c_val = doc.get("clave")
        if c_val is not None:
            try:
                c_int = int(c_val)
            except (TypeError, ValueError):
                c_int = None
            if c_int is not None and CLAVE_MIN <= c_int <= CLAVE_MAX:
                clave_counts[c_int] += 1
                clave_last_seen[c_int] = idx

    # Frequency scores
    main_freq: Dict[int, float] = {
        n: main_counts[n] / float(total_draws) for n in main_counts.keys()
    }
    clave_freq: Dict[int, float] = {
        c: clave_counts[c] / float(total_draws) for c in clave_counts.keys()
    }

    # Gap scores: larger value => more overdue
    t_next = cutoff_draw_index + 1
    main_gap: Dict[int, float] = {}
    clave_gap: Dict[int, float] = {}

    for n, last in main_last_seen.items():
        if last is None:
            gap_draws = float(t_next)  # never seen -> maximal gap
        else:
            gap_draws = float(max(1, t_next - last))
        main_gap[n] = gap_draws / float(t_next)

    for c, last in clave_last_seen.items():
        if last is None:
            gap_draws = float(t_next)
        else:
            gap_draws = float(max(1, t_next - last))
        clave_gap[c] = gap_draws / float(t_next)

    return main_freq, main_gap, clave_freq, clave_gap


def _build_hot_scores(
    history: List[dict],
    cutoff_draw_index: int,
) -> Tuple[Dict[int, float], Dict[int, float]]:
    """
    Compute hot/cold scores for mains and claves using the last snapshot
    before or at cutoff_draw_index.
    """
    last_doc = None
    for d in reversed(history):
        idx = int(d.get("draw_index", 0))
        if idx <= cutoff_draw_index:
            last_doc = d
            break

    if not last_doc:
        raise RuntimeError("No El Gordo feature snapshot found for hot/cold computation.")

    hot_mains = {int(n) for n in (last_doc.get("hot_main_numbers") or [])}
    cold_mains = {int(n) for n in (last_doc.get("cold_main_numbers") or [])}
    hot_claves = {int(n) for n in (last_doc.get("hot_clave") or [])}
    cold_claves = {int(n) for n in (last_doc.get("cold_clave") or [])}

    main_freq_counts = [int(x) for x in (last_doc.get("main_frequency_counts") or [])]
    clave_freq_counts = [int(x) for x in (last_doc.get("clave_frequency_counts") or [])]

    max_main_life = max(main_freq_counts) if main_freq_counts else 1
    max_clave_life = max(clave_freq_counts) if clave_freq_counts else 1

    main_hot: Dict[int, float] = {}
    clave_hot: Dict[int, float] = {}

    def _squash(x: float) -> float:
        # Map roughly [-1, 2] into [0, 1]
        return max(0.0, min(1.0, (x + 1.0) / 3.0))

    for n in range(MAIN_MIN, MAIN_MAX + 1):
        life_idx = n - MAIN_MIN
        life_freq = (
            float(main_freq_counts[life_idx]) if 0 <= life_idx < len(main_freq_counts) else 0.0
        )
        life_norm = life_freq / float(max_main_life) if max_main_life > 0 else 0.0
        score = 0.0
        if n in hot_mains:
            score += 1.0
        if n in cold_mains:
            score -= 0.5
        score += 0.5 * life_norm
        main_hot[n] = _squash(score)

    for c in range(CLAVE_MIN, CLAVE_MAX + 1):
        life_idx = c - CLAVE_MIN
        life_freq = (
            float(clave_freq_counts[life_idx]) if 0 <= life_idx < len(clave_freq_counts) else 0.0
        )
        life_norm = life_freq / float(max_clave_life) if max_clave_life > 0 else 0.0
        score = 0.0
        if c in hot_claves:
            score += 1.0
        if c in cold_claves:
            score -= 0.5
        score += 0.5 * life_norm
        clave_hot[c] = _squash(score)

    return main_hot, clave_hot


def run_el_gordo_simple_simulation(
    cutoff_draw_id: Optional[str] = None,
) -> dict:
    """
    Compute a simple frequency / gap / hot-cold simulation for El Gordo.

    Results are persisted into `el_gordo_simulations` with fields:
      - mains:  [{number, freq, gap, hot}, ...]
      - claves: [{number, freq, gap, hot}, ...]
    """
    docs = _load_el_gordo_feature_docs()
    history, cutoff_index, cutoff_date = _select_history_window(docs, cutoff_draw_id)

    main_freq, main_gap, clave_freq, clave_gap = _build_frequency_and_gap(
        history, cutoff_index
    )
    main_hot, clave_hot = _build_hot_scores(history, cutoff_index)

    mains_out: List[dict] = []
    for n in range(MAIN_MIN, MAIN_MAX + 1):
        mains_out.append(
            {
                "number": n,
                "freq": float(main_freq.get(n, 0.0)),
                "gap": float(main_gap.get(n, 0.0)),
                "hot": float(main_hot.get(n, 0.0)),
            }
        )

    claves_out: List[dict] = []
    for c in range(CLAVE_MIN, CLAVE_MAX + 1):
        claves_out.append(
            {
                "number": c,
                "freq": float(clave_freq.get(c, 0.0)),
                "gap": float(clave_gap.get(c, 0.0)),
                "hot": float(clave_hot.get(c, 0.0)),
            }
        )

    client = _get_mongo_client()
    try:
        db = client[MONGO_DB]
        coll = db["el_gordo_simulations"]

        query: Dict[str, object] = {}
        if cutoff_draw_id is not None:
            query["cutoff_draw_id"] = cutoff_draw_id

        now = datetime.utcnow()
        existing = coll.find_one(query) if query else None

        if existing:
            coll.update_one(
                {"_id": existing["_id"]},
                {
                    "$set": {
                        "cutoff_draw_id": cutoff_draw_id,
                        "cutoff_draw_index": cutoff_index,
                        "cutoff_draw_date": cutoff_date,
                        "mains": mains_out,
                        "claves": claves_out,
                        "updated_at": now,
                    },
                },
            )
            sim_doc = coll.find_one({"_id": existing["_id"]})
        else:
            doc = {
                "created_at": now,
                "updated_at": now,
                "cutoff_draw_id": cutoff_draw_id,
                "cutoff_draw_index": cutoff_index,
                "cutoff_draw_date": cutoff_date,
                "mains": mains_out,
                "claves": claves_out,
            }
            res = coll.insert_one(doc)
            sim_doc = coll.find_one({"_id": res.inserted_id})

        if not sim_doc:
            raise RuntimeError("Failed to persist El Gordo simulation document.")

        sim_doc = dict(sim_doc)
        sim_doc.pop("_id", None)
        return sim_doc
    finally:
        client.close()

