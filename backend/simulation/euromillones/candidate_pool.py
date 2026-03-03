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
  Load the latest euromillones_simulations document for a given cutoff_draw_id.

  If cutoff_draw_id is None, returns the most recent simulation document overall.
  """
  client = _get_mongo_client()
  db = client[MONGO_DB]
  coll = db["euromillones_simulations"]

  query: Dict[str, object] = {}
  if cutoff_draw_id is not None:
    query["cutoff_draw_id"] = cutoff_draw_id

  doc = coll.find_one(query, sort=[("created_at", -1)])
  return doc, client


def _normalise_weights(weights: Dict[str, float]) -> Dict[str, float]:
  total = sum(max(v, 0.0) for v in weights.values())
  if total <= 0:
    # fall back to equal weights if all zero/negative
    n = len(weights)
    return {k: 1.0 / n for k in weights.keys()}
  return {k: max(v, 0.0) / total for k, v in weights.items()}


def _score_items(
  items: List[dict],
  weights: Dict[str, float],
) -> List[dict]:
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

  # Sort by score desc, then number asc for stable ordering
  out.sort(key=lambda r: (-r["score"], r["number"]))
  return out


def build_candidate_pool(
  *,
  cutoff_draw_id: Optional[str] = None,
  k_main: int = 15,
  k_star: int = 4,
  main_weights: Optional[Dict[str, float]] = None,
  star_weights: Optional[Dict[str, float]] = None,
) -> dict:
  """
  Build a candidate pool of Euromillones numbers for wheeling.

  Uses the unified `euromillones_simulations` document (freq/gap/hot per number)
  and combines the three probabilities into a single score per number.

  Default weights:
    mains: freq=0.4, gap=0.3, hot=0.3
    stars: freq=0.5, gap=0.25, hot=0.25

  The resulting candidate pool is also persisted back into the same
  `euromillones_simulations` document under the `candidate_pool` field.

  Returns a JSON-serialisable dict:
    {
      "cutoff_draw_id": ...,
      "cutoff_draw_index": ...,
      "cutoff_draw_date": ...,
      "k_main": k_main,
      "k_star": k_star,
      "main_weights": {...},
      "star_weights": {...},
      "main_pool": [int, ...],
      "star_pool": [int, ...],
      "main_scored": [{number, freq, gap, hot, score}, ...],
      "star_scored": [...],
    }
  """
  if main_weights is None:
    main_weights = {"freq": 0.4, "gap": 0.3, "hot": 0.3}
  if star_weights is None:
    star_weights = {"freq": 0.5, "gap": 0.25, "hot": 0.25}

  if k_main <= 0:
    raise ValueError("k_main must be positive")
  if k_star <= 0:
    raise ValueError("k_star must be positive")

  doc, client = _load_latest_simulation(cutoff_draw_id)
  try:
    if not doc:
      raise RuntimeError(
        "No Euromillones simulation found for the given cutoff_draw_id. "
        "Run the simulation models first."
      )

    mains = doc.get("mains") or []
    stars = doc.get("stars") or []

    main_scored = _score_items(list(mains), main_weights)
    star_scored = _score_items(list(stars), star_weights)

    main_pool = [row["number"] for row in main_scored[:k_main]]
    star_pool = [row["number"] for row in star_scored[:k_star]]

    norm_main = _normalise_weights(main_weights)
    norm_star = _normalise_weights(star_weights)

    result = {
      "cutoff_draw_id": doc.get("cutoff_draw_id"),
      "cutoff_draw_index": doc.get("cutoff_draw_index"),
      "cutoff_draw_date": doc.get("cutoff_draw_date"),
      "k_main": k_main,
      "k_star": k_star,
      "main_weights": norm_main,
      "star_weights": norm_star,
      "main_pool": main_pool,
      "star_pool": star_pool,
      "main_scored": main_scored,
      "star_scored": star_scored,
    }

    # Persist candidate pool into the same simulation document
    db = client[MONGO_DB]
    coll = db["euromillones_simulations"]
    coll.update_one(
      {"_id": doc["_id"]},
      {
        "$set": {
          "candidate_pool": {
            "k_main": k_main,
            "k_star": k_star,
            "main_weights": norm_main,
            "star_weights": norm_star,
            "main_pool": main_pool,
            "star_pool": star_pool,
            "updated_at": datetime.utcnow(),
          }
        },
        "$currentDate": {"updated_at": True},
      },
    )

    return result
  finally:
    client.close()

