import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv  # type: ignore[import-untyped]
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "lottery")


def _get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI)


def _load_latest_simulation(
    cutoff_draw_id: Optional[str],
) -> Tuple[Optional[dict], MongoClient]:
    """
    Load the latest el_gordo_simulations document for a given cutoff_draw_id.

    If cutoff_draw_id is None, returns the most recent simulation document overall.
    """
    client = _get_mongo_client()
    db = client[MONGO_DB]
    coll = db["el_gordo_simulations"]

    query: Dict[str, object] = {}
    if cutoff_draw_id is not None:
        query["cutoff_draw_id"] = cutoff_draw_id

    doc = coll.find_one(query, sort=[("created_at", -1)])
    return doc, client


def _normalise_weights(weights: Dict[str, float]) -> Dict[str, float]:
    total = sum(max(v, 0.0) for v in weights.values())
    if total <= 0:
        n = len(weights)
        return {k: 1.0 / n for k in weights.keys()}
    return {k: max(v, 0.0) / total for k, v in weights.items()}


def _score_items(items: List[dict], weights: Dict[str, float]) -> List[dict]:
    """
    Attach a combined score to each number row using the given weights.

    Each item is expected to have fields:
      - number (int)
      - freq (float | None)
      - gap  (float | None)
      - hot  (float | None)
    """
    w = _normalise_weights(weights)
    out: List[dict] = []
    for row in items:
        num = int(row.get("number"))
        freq = float(row.get("freq") or 0.0)
        gap = float(row.get("gap") or 0.0)
        hot = float(row.get("hot") or 0.0)
        score = (
            w.get("freq", 0.0) * freq
            + w.get("gap", 0.0) * gap
            + w.get("hot", 0.0) * hot
        )
        out.append(
            {
                "number": num,
                "freq": freq,
                "gap": gap,
                "hot": hot,
                "score": score,
            }
        )

    out.sort(key=lambda r: (-r["score"], r["number"]))
    return out


def build_el_gordo_candidate_pool(
    *,
    cutoff_draw_id: Optional[str] = None,
    k_main: int = 20,
    k_clave: int = 6,
    main_weights: Optional[Dict[str, float]] = None,
    clave_weights: Optional[Dict[str, float]] = None,
) -> dict:
    """
    Build a candidate pool of El Gordo numbers for wheeling (5 mains, 1 clave).

    Uses the `el_gordo_simulations` document (freq/gap/hot per number) and
    combines the three probabilities into a single score per number.
    """
    if main_weights is None:
        main_weights = {"freq": 0.4, "gap": 0.3, "hot": 0.3}
    if clave_weights is None:
        clave_weights = {"freq": 0.4, "gap": 0.3, "hot": 0.3}

    if k_main <= 0:
        raise ValueError("k_main must be positive")
    if k_clave <= 0:
        raise ValueError("k_clave must be positive")

    doc, client = _load_latest_simulation(cutoff_draw_id)
    try:
        if not doc:
            raise RuntimeError(
                "No El Gordo simulation found for the given cutoff_draw_id. "
                "Run the El Gordo simulation models first."
            )

        mains = doc.get("mains") or []
        claves = doc.get("claves") or []

        main_scored = _score_items(list(mains), main_weights)
        clave_scored = _score_items(list(claves), clave_weights)

        main_pool = [row["number"] for row in main_scored[:k_main]]
        clave_pool = [row["number"] for row in clave_scored[:k_clave]]

        norm_main = _normalise_weights(main_weights)
        norm_clave = _normalise_weights(clave_weights)

        result = {
            "cutoff_draw_id": doc.get("cutoff_draw_id"),
            "cutoff_draw_index": doc.get("cutoff_draw_index"),
            "cutoff_draw_date": doc.get("cutoff_draw_date"),
            "k_main": k_main,
            "k_clave": k_clave,
            "main_weights": norm_main,
            "clave_weights": norm_clave,
            "main_pool": main_pool,
            "clave_pool": clave_pool,
            "main_scored": main_scored,
            "clave_scored": clave_scored,
        }

        db = client[MONGO_DB]
        coll = db["el_gordo_simulations"]
        coll.update_one(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "candidate_pool": {
                        "k_main": k_main,
                        "k_clave": k_clave,
                        "main_weights": norm_main,
                        "clave_weights": norm_clave,
                        "main_pool": main_pool,
                        "clave_pool": clave_pool,
                        "updated_at": datetime.utcnow(),
                    }
                },
                "$currentDate": {"updated_at": True},
            },
        )

        return result
    finally:
        client.close()

