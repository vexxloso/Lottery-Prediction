import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Literal, Tuple

import joblib  # type: ignore[import-untyped]
import pandas as pd
from dotenv import load_dotenv  # type: ignore[import-untyped]
from pymongo import ASCENDING, MongoClient
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "lottery")

MAIN_MIN, MAIN_MAX = 1, 50
STAR_MIN, STAR_MAX = 1, 12

NumberType = Literal["main", "star"]


@dataclass
class DrawRow:
    draw_index: int
    main_numbers: List[int]
    star_numbers: List[int]


def _get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI)


def _load_draw_rows() -> List[DrawRow]:
    """
    Load all Euromillones draws (minimal info) sorted by draw_index ascending.
    """
    client = _get_mongo_client()
    db = client[MONGO_DB]
    coll = db["euromillones_draw_features"]

    docs = list(
        coll.find(
            {},
            projection={"draw_index": 1, "main_numbers": 1, "star_numbers": 1},
        ).sort("draw_index", ASCENDING)
    )

    rows: List[DrawRow] = []
    for doc in docs:
        idx = int(doc.get("draw_index", 0))
        mains = [int(n) for n in (doc.get("main_numbers") or [])]
        stars = [int(s) for s in (doc.get("star_numbers") or [])]
        rows.append(DrawRow(draw_index=idx, main_numbers=mains, star_numbers=stars))

    client.close()
    return rows


def _build_gap_dataset(
    number_type: NumberType,
    min_history_draws: int = 50,
) -> pd.DataFrame:
    """
    Build training dataset for the gap-based model (Model G).

    Features:
      - gap_draws: draws since last appearance (None -> very large value with flag)
      - is_unseen: whether the number has never appeared before this draw
    Label:
      - 1 if the number appears in the current draw (main or star), else 0.
    """
    draws = _load_draw_rows()
    if not draws:
        raise RuntimeError("No Euromillones draws available for gap dataset.")

    if number_type == "main":
        num_min, num_max = MAIN_MIN, MAIN_MAX
    else:
        num_min, num_max = STAR_MIN, STAR_MAX

    # last_seen_index per number
    last_seen: Dict[int, int] = {n: -1 for n in range(num_min, num_max + 1)}

    rows_out: List[Dict[str, float]] = []

    for draw in draws:
        t = draw.draw_index
        if t <= min_history_draws:
            # Update last_seen but do not generate training rows yet
            numbers_here = (
                draw.main_numbers if number_type == "main" else draw.star_numbers
            )
            for n in numbers_here:
                if num_min <= n <= num_max:
                    last_seen[n] = t
            continue

        numbers_here = draw.main_numbers if number_type == "main" else draw.star_numbers
        present_set = set(numbers_here)

        # Build features for this draw BEFORE updating last_seen with current draw
        for n in range(num_min, num_max + 1):
            last = last_seen.get(n, -1)
            if last == -1:
                gap_draws = float(0)  # use 0 as base; flag unseen separately
                is_unseen = 1.0
            else:
                gap_draws = float(t - last)
                is_unseen = 0.0

            rows_out.append(
                {
                    "draw_index": t,
                    "number": n,
                    "gap_draws": gap_draws,
                    "is_unseen": is_unseen,
                    "label": 1.0 if n in present_set else 0.0,
                }
            )

        # Now update last_seen for next draw
        for n in numbers_here:
            if num_min <= n <= num_max:
                last_seen[n] = t

    df = pd.DataFrame(rows_out)

    # Per-draw rank of gap_draws (larger gap = more overdue)
    df["gap_rank"] = (
        df.groupby("draw_index")["gap_draws"]
        .rank(method="average", ascending=False)
        .astype(float)
    )

    return df


def _train_gap_model(number_type: NumberType, random_state: int = 42) -> None:
    """
    Train a Gradient Boosting classifier for the gap-based model.
    Stores the fitted model to disk using joblib.
    """
    df = _build_gap_dataset(number_type)

    feature_cols = ["gap_draws", "is_unseen", "gap_rank"]

    X = df[feature_cols].values
    y = df["label"].values

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=random_state, stratify=y
    )

    clf = GradientBoostingClassifier(random_state=random_state)
    clf.fit(X_train, y_train)

    val_score = clf.score(X_val, y_val)
    print(f"[gap][{number_type}] validation accuracy: {val_score:.4f}")

    model_dir = os.path.join(os.path.dirname(__file__), "..", "models", "euromillones")
    model_dir = os.path.normpath(model_dir)
    os.makedirs(model_dir, exist_ok=True)
    model_path = os.path.join(model_dir, f"gap_{number_type}.joblib")
    joblib.dump({"model": clf, "features": feature_cols}, model_path)
    print(f"[gap][{number_type}] model saved to {model_path}")


def train_all_gap_models() -> None:
    _train_gap_model("main")
    _train_gap_model("star")


def _build_current_gap_features(
    number_type: NumberType, cutoff_draw_index: int | None = None
) -> pd.DataFrame:
    """
    Build one feature row per number for the next draw after cutoff_draw_index.
    """
    draws = _load_draw_rows()
    if not draws:
        raise RuntimeError("No Euromillones draws available for gap features.")

    if cutoff_draw_index is not None:
        draws = [d for d in draws if d.draw_index <= cutoff_draw_index]
        if not draws:
            raise RuntimeError("No draws found up to the selected cutoff for gap model.")

    if number_type == "main":
        num_min, num_max = MAIN_MIN, MAIN_MAX
    else:
        num_min, num_max = STAR_MIN, STAR_MAX

    last_seen: Dict[int, int] = {n: -1 for n in range(num_min, num_max + 1)}

    # Walk through draws to update last_seen; we don't need labels here
    max_idx = -1
    for d in draws:
        t = d.draw_index
        numbers_here = d.main_numbers if number_type == "main" else d.star_numbers
        for n in numbers_here:
            if num_min <= n <= num_max:
                last_seen[n] = t
        if t > max_idx:
            max_idx = t

    if max_idx < 0:
        raise RuntimeError("Invalid Euromillones draw history for gap model.")

    t_next = max_idx + 1

    rows: List[Dict[str, float]] = []
    for n in range(num_min, num_max + 1):
        last = last_seen.get(n, -1)
        if last == -1:
            gap_draws = float(0)
            is_unseen = 1.0
        else:
            gap_draws = float(t_next - last)
            is_unseen = 0.0
        rows.append(
            {
                "draw_index": t_next,
                "number": n,
                "gap_draws": gap_draws,
                "is_unseen": is_unseen,
            }
        )

    df = pd.DataFrame(rows)
    df["gap_rank"] = (
        df.groupby("draw_index")["gap_draws"]
        .rank(method="average", ascending=False)
        .astype(float)
    )
    return df


def predict_next_gap_scores(
    cutoff_draw_id: str | None = None,
) -> Dict[str, List[Dict[str, float]]]:
    """
    Compute gap-based probabilities for the next Euromillones draw.
    """
    from .frequency_model import _get_cutoff_info_for_draw  # reuse helper

    base_model_dir = os.path.join(os.path.dirname(__file__), "..", "models", "euromillones")
    base_model_dir = os.path.normpath(base_model_dir)

    result: Dict[str, List[Dict[str, float]]] = {"mains": [], "stars": []}

    cutoff_index: int | None = None
    cutoff_date: str | None = None
    if cutoff_draw_id is not None:
        cutoff_index, cutoff_date = _get_cutoff_info_for_draw(cutoff_draw_id)

    for number_type, key in (("main", "mains"), ("star", "stars")):
        model_path = os.path.join(base_model_dir, f"gap_{number_type}.joblib")
        if not os.path.exists(model_path):
            raise RuntimeError(
                f"Gap model for {number_type} numbers not found at {model_path}. "
                "Train it first from the simulation tools."
            )
        saved = joblib.load(model_path)
        model: GradientBoostingClassifier = saved["model"]
        feature_cols: List[str] = saved["features"]

        df_features = _build_current_gap_features(
            number_type=number_type,  # type: ignore[arg-type]
            cutoff_draw_index=cutoff_index,
        )
        X = df_features[feature_cols].values
        probs = model.predict_proba(X)[:, 1]

        rows = []
        for num, p in zip(df_features["number"].tolist(), probs.tolist()):
            rows.append({"number": int(num), "p": float(p)})
        rows.sort(key=lambda x: x["p"], reverse=True)
        result[key] = rows

    result["_meta"] = {
        "cutoff_draw_id": cutoff_draw_id,
        "cutoff_draw_index": cutoff_index,
        "cutoff_draw_date": cutoff_date,
    }
    return result


def save_gap_simulation_result(scores: Dict[str, List[Dict[str, float]]]) -> str:
    """
    Persist gap-based probabilities into the shared Euromillones simulation document.
    Only the `gap` field for each number is updated.
    """
    client = _get_mongo_client()
    db = client[MONGO_DB]
    coll = db["euromillones_simulations"]

    meta = scores.get("_meta") or {}
    cutoff_draw_id = meta.get("cutoff_draw_id")
    cutoff_draw_index = meta.get("cutoff_draw_index")
    cutoff_draw_date = meta.get("cutoff_draw_date")

    mains_scores = scores.get("mains") or []
    stars_scores = scores.get("stars") or []

    main_gap: Dict[int, float] = {
        int(row["number"]): float(row["p"]) for row in mains_scores
    }
    star_gap: Dict[int, float] = {
        int(row["number"]): float(row["p"]) for row in stars_scores
    }

    existing = coll.find_one({"cutoff_draw_id": cutoff_draw_id}) if cutoff_draw_id else None

    if existing:
        mains_existing = existing.get("mains") or []
        stars_existing = existing.get("stars") or []

        mains_merged: List[Dict[str, float]] = []
        for row in mains_existing:
            num = int(row.get("number"))
            gap_val = main_gap.get(num, float(row.get("gap") or 0.0))
            mains_merged.append(
                {
                    "number": num,
                    "freq": float(row.get("freq")) if row.get("freq") is not None else None,
                    "gap": gap_val,
                    "hot": float(row.get("hot")) if row.get("hot") is not None else None,
                }
            )
        for num, p in main_gap.items():
            if not any(r["number"] == num for r in mains_merged):
                mains_merged.append({"number": num, "freq": None, "gap": p, "hot": None})

        stars_merged: List[Dict[str, float]] = []
        for row in stars_existing:
            num = int(row.get("number"))
            gap_val = star_gap.get(num, float(row.get("gap") or 0.0))
            stars_merged.append(
                {
                    "number": num,
                    "freq": float(row.get("freq")) if row.get("freq") is not None else None,
                    "gap": gap_val,
                    "hot": float(row.get("hot")) if row.get("hot") is not None else None,
                }
            )
        for num, p in star_gap.items():
            if not any(r["number"] == num for r in stars_merged):
                stars_merged.append({"number": num, "freq": None, "gap": p, "hot": None})

        coll.update_one(
            {"_id": existing["_id"]},
            {
                "$set": {
                    "cutoff_draw_id": cutoff_draw_id,
                    "cutoff_draw_index": cutoff_draw_index,
                    "cutoff_draw_date": cutoff_draw_date,
                    "mains": mains_merged,
                    "stars": stars_merged,
                    "updated_at": datetime.utcnow(),
                }
            },
        )
        sim_id = str(existing["_id"])
    else:
        mains_doc = [
            {"number": num, "freq": None, "gap": p, "hot": None} for num, p in main_gap.items()
        ]
        stars_doc = [
            {"number": num, "freq": None, "gap": p, "hot": None} for num, p in star_gap.items()
        ]
        doc = {
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "cutoff_draw_id": cutoff_draw_id,
            "cutoff_draw_index": cutoff_draw_index,
            "cutoff_draw_date": cutoff_draw_date,
            "mains": mains_doc,
            "stars": stars_doc,
        }
        res = coll.insert_one(doc)
        sim_id = str(res.inserted_id)

    client.close()
    return sim_id


if __name__ == "__main__":
    train_all_gap_models()

