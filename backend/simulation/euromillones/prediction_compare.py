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


def _load_result_and_cutoff(
    db, result_draw_id: str
) -> Tuple[dict, dict, str, int]:
    """
    Given a Euromillones result_draw_id, load:
      - current feature doc (result)
      - previous feature doc (cutoff)
      - cutoff_draw_id
      - cutoff_draw_index
    """
    features_coll = db["euromillones_draw_features"]
    current_doc = features_coll.find_one({"draw_id": result_draw_id})
    if not current_doc:
        raise RuntimeError(
            "No se ha encontrado el sorteo real en euromillones_draw_features."
        )

    cutoff_draw_id = current_doc.get("prev_draw_id")
    if not cutoff_draw_id:
        raise RuntimeError(
            "El sorteo real no tiene información del sorteo anterior (prev_draw_id)."
        )

    prev_doc = features_coll.find_one({"draw_id": cutoff_draw_id})
    if not prev_doc:
        raise RuntimeError(
            "No se ha encontrado el sorteo anterior en euromillones_draw_features."
        )

    cutoff_index_val = prev_doc.get("draw_index")
    if not isinstance(cutoff_index_val, int):
        raise RuntimeError("El sorteo anterior no tiene un índice de sorteo válido.")
    cutoff_index = int(cutoff_index_val)

    return current_doc, prev_doc, cutoff_draw_id, cutoff_index


def _rank_numbers(
    rows: List[dict],
    key: str,
    descending: bool = True,
) -> Dict[int, int]:
    """
    Build rank map number -> 1-based rank for given key in rows.

    If key is missing, value 0 is treated.
    """
    scored = []
    for row in rows:
        num = int(row.get("number"))
        val = float(row.get(key) or 0.0)
        scored.append((num, val))
    scored.sort(key=lambda x: x[1], reverse=descending)
    rank: Dict[int, int] = {}
    r = 1
    for num, _ in scored:
        if num not in rank:
            rank[num] = r
            r += 1
    return rank


def compare_prediction_with_result(
    result_draw_id: str,
    top_k_main: int = 10,
    top_k_star: int = 6,
) -> Dict[str, object]:
    """
    Compare Euromillones per-number prediction (freq/gap/hot) against the real result.

    Saves metrics into euromillones_simulations.prediction_compare_by_result[result_draw_id].
    """
    client = _get_mongo_client()
    try:
        db = client[MONGO_DB]

        current_doc, prev_doc, cutoff_draw_id, cutoff_index = _load_result_and_cutoff(
            db, result_draw_id
        )

        sim_coll = db["euromillones_simulations"]
        sim_doc = sim_coll.find_one(
            {"cutoff_draw_id": cutoff_draw_id},
            sort=[("created_at", -1)],
        )
        if not sim_doc:
            raise RuntimeError(
                "No se ha encontrado ninguna simulación de Euromillones para el sorteo anterior."
            )

        mains = sim_doc.get("mains") or []
        stars = sim_doc.get("stars") or []
        if not mains or not stars:
            raise RuntimeError(
                "La simulación de Euromillones no contiene datos de mains/stars."
            )

        # Check cache
        sim_updated_at = sim_doc.get("updated_at")
        existing_all = (sim_doc.get("prediction_compare_by_result") or {}).get(
            result_draw_id
        )
        if existing_all and existing_all.get("source_updated_at") == sim_updated_at:
            return existing_all

        result_main_numbers = [int(n) for n in (current_doc.get("main_numbers") or [])]
        result_star_numbers = [int(s) for s in (current_doc.get("star_numbers") or [])]
        if len(result_main_numbers) != 5 or len(result_star_numbers) != 2:
            raise RuntimeError(
                "El sorteo real no tiene una combinación válida de 5+2."
            )

        # Build rank maps per metric
        rank_main_freq = _rank_numbers(mains, "freq")
        rank_main_gap = _rank_numbers(mains, "gap")
        rank_main_hot = _rank_numbers(mains, "hot")

        rank_star_freq = _rank_numbers(stars, "freq")
        rank_star_gap = _rank_numbers(stars, "gap")
        rank_star_hot = _rank_numbers(stars, "hot")

        def _top_k_hits(result_numbers: List[int], rank_map: Dict[int, int], k: int) -> int:
            hits = 0
            for n in result_numbers:
                r = rank_map.get(int(n))
                if r is not None and r <= k:
                    hits += 1
            return hits

        metrics: Dict[str, object] = {
            "top_k_main": top_k_main,
            "top_k_star": top_k_star,
            "main": {
                "freq_topk_hits": _top_k_hits(result_main_numbers, rank_main_freq, top_k_main),
                "gap_topk_hits": _top_k_hits(result_main_numbers, rank_main_gap, top_k_main),
                "hot_topk_hits": _top_k_hits(result_main_numbers, rank_main_hot, top_k_main),
            },
            "star": {
                "freq_topk_hits": _top_k_hits(result_star_numbers, rank_star_freq, top_k_star),
                "gap_topk_hits": _top_k_hits(result_star_numbers, rank_star_gap, top_k_star),
                "hot_topk_hits": _top_k_hits(result_star_numbers, rank_star_hot, top_k_star),
            },
        }

        # Optional: per-number detail (only for result numbers to keep it small)
        per_number_main: List[Dict[str, object]] = []
        for n in result_main_numbers:
            n_int = int(n)
            row = next((r for r in mains if int(r.get("number")) == n_int), None)
            per_number_main.append(
                {
                    "number": n_int,
                    "freq": float(row.get("freq") or 0.0) if row else 0.0,
                    "gap": float(row.get("gap") or 0.0) if row else 0.0,
                    "hot": float(row.get("hot") or 0.0) if row else 0.0,
                    "rank_freq": rank_main_freq.get(n_int),
                    "rank_gap": rank_main_gap.get(n_int),
                    "rank_hot": rank_main_hot.get(n_int),
                }
            )

        per_number_star: List[Dict[str, object]] = []
        for s in result_star_numbers:
            s_int = int(s)
            row = next((r for r in stars if int(r.get("number")) == s_int), None)
            per_number_star.append(
                {
                    "number": s_int,
                    "freq": float(row.get("freq") or 0.0) if row else 0.0,
                    "gap": float(row.get("gap") or 0.0) if row else 0.0,
                    "hot": float(row.get("hot") or 0.0) if row else 0.0,
                    "rank_freq": rank_star_freq.get(s_int),
                    "rank_gap": rank_star_gap.get(s_int),
                    "rank_hot": rank_star_hot.get(s_int),
                }
            )

        compare_doc: Dict[str, object] = {
            "source_updated_at": sim_updated_at,
            "cutoff_draw_id": cutoff_draw_id,
            "cutoff_draw_index": cutoff_index,
            "cutoff_draw_date": prev_doc.get("draw_date"),
            "result_draw_id": result_draw_id,
            "result_draw_date": current_doc.get("draw_date"),
            "result_main_numbers": result_main_numbers,
            "result_star_numbers": result_star_numbers,
            "metrics": metrics,
            "per_number_main": per_number_main,
            "per_number_star": per_number_star,
            "created_at": datetime.utcnow(),
        }

        sim_coll.update_one(
            {"_id": sim_doc["_id"]},
            {
                "$set": {
                    f"prediction_compare_by_result.{result_draw_id}": compare_doc,
                    "updated_at": datetime.utcnow(),
                }
            },
        )

        return compare_doc
    finally:
        client.close()

