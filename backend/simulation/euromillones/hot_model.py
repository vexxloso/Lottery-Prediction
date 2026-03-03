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
class FeatureDoc:
    draw_index: int
    main_numbers: List[int]
    star_numbers: List[int]
    hot_main_numbers: List[int]
    cold_main_numbers: List[int]
    hot_star_numbers: List[int]
    cold_star_numbers: List[int]
    main_frequency_counts: List[int]
    star_frequency_counts: List[int]


def _get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI)


def _load_feature_docs() -> List[FeatureDoc]:
    """
    Load Euromillones feature docs sorted by draw_index ascending.
    """
    client = _get_mongo_client()
    db = client[MONGO_DB]
    coll = db["euromillones_draw_features"]

    docs = list(
        coll.find(
            {},
            projection={
                "draw_index": 1,
                "main_numbers": 1,
                "star_numbers": 1,
                "hot_main_numbers": 1,
                "cold_main_numbers": 1,
                "hot_star_numbers": 1,
                "cold_star_numbers": 1,
                "main_frequency_counts": 1,
                "star_frequency_counts": 1,
            },
        ).sort("draw_index", ASCENDING)
    )

    out: List[FeatureDoc] = []
    for doc in docs:
        out.append(
            FeatureDoc(
                draw_index=int(doc.get("draw_index", 0)),
                main_numbers=[int(n) for n in (doc.get("main_numbers") or [])],
                star_numbers=[int(s) for s in (doc.get("star_numbers") or [])],
                hot_main_numbers=[int(n) for n in (doc.get("hot_main_numbers") or [])],
                cold_main_numbers=[int(n) for n in (doc.get("cold_main_numbers") or [])],
                hot_star_numbers=[int(n) for n in (doc.get("hot_star_numbers") or [])],
                cold_star_numbers=[int(n) for n in (doc.get("cold_star_numbers") or [])],
                main_frequency_counts=[
                    int(x) for x in (doc.get("main_frequency_counts") or [])
                ],
                star_frequency_counts=[
                    int(x) for x in (doc.get("star_frequency_counts") or [])
                ],
            )
        )

    client.close()
    return out


def _build_hot_dataset(number_type: NumberType) -> pd.DataFrame:
    """
    Build training dataset for the hot/cold-based model.

    For each draw t > 0, use the feature snapshot at t-1 as input, and ask
    whether the number appears in draw t.
    """
    docs = _load_feature_docs()
    if len(docs) < 2:
        raise RuntimeError("Not enough Euromillones feature rows to build hot/cold dataset.")

    rows: List[Dict[str, float]] = []

    for i in range(1, len(docs)):
        prev = docs[i - 1]
        curr = docs[i]
        t = curr.draw_index

        if number_type == "main":
            num_min, num_max = MAIN_MIN, MAIN_MAX
            hot_set = set(prev.hot_main_numbers)
            cold_set = set(prev.cold_main_numbers)
            life_counts = prev.main_frequency_counts
            present_set = set(curr.main_numbers)
        else:
            num_min, num_max = STAR_MIN, STAR_MAX
            hot_set = set(prev.hot_star_numbers)
            cold_set = set(prev.cold_star_numbers)
            life_counts = prev.star_frequency_counts
            present_set = set(curr.star_numbers)

        # Build lifetime frequency ranks once per draw
        life_series = pd.Series(
            {n: life_counts[n - num_min] if 0 <= n - num_min < len(life_counts) else 0 for n in range(num_min, num_max + 1)}
        )
        life_rank = life_series.rank(ascending=False, method="average")

        for n in range(num_min, num_max + 1):
            is_hot = 1.0 if n in hot_set else 0.0
            is_cold = 1.0 if n in cold_set else 0.0

            lf = float(life_series.loc[n])
            lr = float(life_rank.loc[n])

            rows.append(
                {
                    "draw_index": t,
                    "number": n,
                    "is_hot": is_hot,
                    "is_cold": is_cold,
                    "life_freq": lf,
                    "life_rank": lr,
                    "label": 1.0 if n in present_set else 0.0,
                }
            )

    return pd.DataFrame(rows)


def _train_hot_model(number_type: NumberType, random_state: int = 42) -> None:
    """
    Train a Gradient Boosting classifier for the hot/cold-based model.
    """
    df = _build_hot_dataset(number_type)

    feature_cols = ["is_hot", "is_cold", "life_freq", "life_rank"]

    X = df[feature_cols].values
    y = df["label"].values

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=random_state, stratify=y
    )

    clf = GradientBoostingClassifier(random_state=random_state)
    clf.fit(X_train, y_train)

    val_score = clf.score(X_val, y_val)
    print(f"[hot][{number_type}] validation accuracy: {val_score:.4f}")

    model_dir = os.path.join(os.path.dirname(__file__), "..", "models", "euromillones")
    model_dir = os.path.normpath(model_dir)
    os.makedirs(model_dir, exist_ok=True)
    model_path = os.path.join(model_dir, f"hot_{number_type}.joblib")
    joblib.dump({"model": clf, "features": feature_cols}, model_path)
    print(f"[hot][{number_type}] model saved to {model_path}")


def train_all_hot_models() -> None:
    _train_hot_model("main")
    _train_hot_model("star")


def _build_current_hot_features(
    number_type: NumberType, cutoff_draw_index: int | None = None
) -> pd.DataFrame:
    """
    Build hot/cold features for the draw after cutoff_draw_index.
    Uses the last available feature snapshot as "previous" state.
    """
    docs = _load_feature_docs()
    if not docs:
        raise RuntimeError("No Euromillones feature rows available for hot/cold model.")

    if cutoff_draw_index is not None:
        docs = [d for d in docs if d.draw_index <= cutoff_draw_index]
        if not docs:
            raise RuntimeError("No feature rows found up to the selected cutoff.")

    prev = docs[-1]

    if number_type == "main":
        num_min, num_max = MAIN_MIN, MAIN_MAX
        hot_set = set(prev.hot_main_numbers)
        cold_set = set(prev.cold_main_numbers)
        life_counts = prev.main_frequency_counts
    else:
        num_min, num_max = STAR_MIN, STAR_MAX
        hot_set = set(prev.hot_star_numbers)
        cold_set = set(prev.cold_star_numbers)
        life_counts = prev.star_frequency_counts

    life_series = pd.Series(
        {n: life_counts[n - num_min] if 0 <= n - num_min < len(life_counts) else 0 for n in range(num_min, num_max + 1)}
    )
    life_rank = life_series.rank(ascending=False, method="average")

    rows: List[Dict[str, float]] = []
    for n in range(num_min, num_max + 1):
        is_hot = 1.0 if n in hot_set else 0.0
        is_cold = 1.0 if n in cold_set else 0.0
        lf = float(life_series.loc[n])
        lr = float(life_rank.loc[n])
        rows.append(
            {
                "number": n,
                "is_hot": is_hot,
                "is_cold": is_cold,
                "life_freq": lf,
                "life_rank": lr,
            }
        )

    return pd.DataFrame(rows)


def predict_next_hot_scores(
    cutoff_draw_id: str | None = None,
) -> Dict[str, List[Dict[str, float]]]:
    """
    Compute hot/cold-based probabilities for the next Euromillones draw.
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
        model_path = os.path.join(base_model_dir, f"hot_{number_type}.joblib")
        if not os.path.exists(model_path):
            raise RuntimeError(
                f"Hot/cold model for {number_type} numbers not found at {model_path}. "
                "Train it first from the simulation tools."
            )
        saved = joblib.load(model_path)
        model: GradientBoostingClassifier = saved["model"]
        feature_cols: List[str] = saved["features"]

        df_features = _build_current_hot_features(
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


def save_hot_simulation_result(scores: Dict[str, List[Dict[str, float]]]) -> str:
    """
    Persist hot/cold-based probabilities into the shared Euromillones simulation document.
    Only the `hot` field for each number is updated.
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

    main_hot: Dict[int, float] = {
        int(row["number"]): float(row["p"]) for row in mains_scores
    }
    star_hot: Dict[int, float] = {
        int(row["number"]): float(row["p"]) for row in stars_scores
    }

    existing = coll.find_one({"cutoff_draw_id": cutoff_draw_id}) if cutoff_draw_id else None

    if existing:
        mains_existing = existing.get("mains") or []
        stars_existing = existing.get("stars") or []

        mains_merged: List[Dict[str, float]] = []
        for row in mains_existing:
            num = int(row.get("number"))
            hot_val = main_hot.get(num, float(row.get("hot") or 0.0))
            mains_merged.append(
                {
                    "number": num,
                    "freq": float(row.get("freq")) if row.get("freq") is not None else None,
                    "gap": float(row.get("gap")) if row.get("gap") is not None else None,
                    "hot": hot_val,
                }
            )
        for num, p in main_hot.items():
            if not any(r["number"] == num for r in mains_merged):
                mains_merged.append({"number": num, "freq": None, "gap": None, "hot": p})

        stars_merged: List[Dict[str, float]] = []
        for row in stars_existing:
            num = int(row.get("number"))
            hot_val = star_hot.get(num, float(row.get("hot") or 0.0))
            stars_merged.append(
                {
                    "number": num,
                    "freq": float(row.get("freq")) if row.get("freq") is not None else None,
                    "gap": float(row.get("gap")) if row.get("gap") is not None else None,
                    "hot": hot_val,
                }
            )
        for num, p in star_hot.items():
            if not any(r["number"] == num for r in stars_merged):
                stars_merged.append({"number": num, "freq": None, "gap": None, "hot": p})

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
            {"number": num, "freq": None, "gap": None, "hot": p} for num, p in main_hot.items()
        ]
        stars_doc = [
            {"number": num, "freq": None, "gap": None, "hot": p} for num, p in star_hot.items()
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
    train_all_hot_models()

