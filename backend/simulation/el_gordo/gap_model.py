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

MAIN_MIN, MAIN_MAX = 1, 54
CLAVE_MIN, CLAVE_MAX = 0, 9

NumberType = Literal["main", "clave"]


@dataclass
class DrawRow:
    draw_index: int
    main_numbers: List[int]
    clave: int | None


def _get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI)


def _load_draw_rows() -> List[DrawRow]:
    """
    Load all El Gordo draws (minimal info) sorted by draw_index ascending.
    """
    client = _get_mongo_client()
    db = client[MONGO_DB]
    coll = db["el_gordo_draw_features"]

    docs = list(
        coll.find(
            {},
            projection={"draw_index": 1, "main_numbers": 1, "clave": 1},
        ).sort("draw_index", ASCENDING)
    )

    rows: List[DrawRow] = []
    for doc in docs:
        idx = int(doc.get("draw_index", 0))
        mains = [int(n) for n in (doc.get("main_numbers") or [])]
        clave_val = doc.get("clave")
        try:
            clave_int = int(clave_val) if clave_val is not None else None
        except (TypeError, ValueError):
            clave_int = None
        rows.append(DrawRow(draw_index=idx, main_numbers=mains, clave=clave_int))

    client.close()
    return rows


def _build_gap_dataset(
    number_type: NumberType,
    min_history_draws: int = 50,
) -> pd.DataFrame:
    """
    Build training dataset for the gap-based model (Model G) for El Gordo.
    """
    draws = _load_draw_rows()
    if not draws:
        raise RuntimeError("No El Gordo draws available for gap dataset.")

    if number_type == "main":
        num_min, num_max = MAIN_MIN, MAIN_MAX
    else:
        num_min, num_max = CLAVE_MIN, CLAVE_MAX

    last_seen: Dict[int, int] = {n: -1 for n in range(num_min, num_max + 1)}

    rows_out: List[Dict[str, float]] = []

    for draw in draws:
        t = draw.draw_index
        if t <= min_history_draws:
            numbers_here = (
                draw.main_numbers if number_type == "main" else ([draw.clave] if draw.clave is not None else [])
            )
            for n in numbers_here:
                if num_min <= n <= num_max:
                    last_seen[n] = t
            continue

        numbers_here = (
            draw.main_numbers if number_type == "main" else ([draw.clave] if draw.clave is not None else [])
        )
        present_set = set(n for n in numbers_here if num_min <= n <= num_max)

        for n in range(num_min, num_max + 1):
            last = last_seen.get(n, -1)
            if last == -1:
                gap_draws = float(0)
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

        for n in numbers_here:
            if num_min <= n <= num_max:
                last_seen[n] = t

    df = pd.DataFrame(rows_out)

    df["gap_rank"] = (
        df.groupby("draw_index")["gap_draws"]
        .rank(method="average", ascending=False)
        .astype(float)
    )

    return df


def _train_gap_model(number_type: NumberType, random_state: int = 42) -> None:
    """
    Train a Gradient Boosting classifier for the gap-based model (El Gordo).
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
    print(f"[el_gordo][gap][{number_type}] validation accuracy: {val_score:.4f}")

    model_dir = os.path.join(os.path.dirname(__file__), "..", "models", "el_gordo")
    model_dir = os.path.normpath(model_dir)
    os.makedirs(model_dir, exist_ok=True)
    model_path = os.path.join(model_dir, f"gap_{number_type}.joblib")
    joblib.dump({"model": clf, "features": feature_cols}, model_path)
    print(f"[el_gordo][gap][{number_type}] model saved to {model_path}")


def train_all_el_gordo_gap_models() -> None:
    _train_gap_model("main")
    _train_gap_model("clave")


def _build_current_gap_features(
    number_type: NumberType, cutoff_draw_index: int | None = None
) -> pd.DataFrame:
    """
    Build one feature row per number for the next draw after cutoff_draw_index.
    """
    draws = _load_draw_rows()
    if not draws:
        raise RuntimeError("No El Gordo draws available for gap features.")

    if cutoff_draw_index is not None:
        draws = [d for d in draws if d.draw_index <= cutoff_draw_index]
        if not draws:
            raise RuntimeError("No draws found up to the selected cutoff for gap model.")

    if number_type == "main":
        num_min, num_max = MAIN_MIN, MAIN_MAX
    else:
        num_min, num_max = CLAVE_MIN, CLAVE_MAX

    last_seen: Dict[int, int] = {n: -1 for n in range(num_min, num_max + 1)}

    max_idx = -1
    for d in draws:
        t = d.draw_index
        numbers_here = (
            d.main_numbers if number_type == "main" else ([d.clave] if d.clave is not None else [])
        )
        for n in numbers_here:
            if num_min <= n <= num_max:
                last_seen[n] = t
        if t > max_idx:
            max_idx = t

    if max_idx < 0:
        raise RuntimeError("Invalid El Gordo draw history for gap model.")

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


def predict_next_el_gordo_gap_scores(
    cutoff_draw_id: str | None = None,
) -> Dict[str, List[Dict[str, float]]]:
    """
    Compute gap-based probabilities for the next El Gordo draw.
    """
    from .frequency_model import _get_cutoff_info_for_draw  # reuse helper

    base_model_dir = os.path.join(os.path.dirname(__file__), "..", "models", "el_gordo")
    base_model_dir = os.path.normpath(base_model_dir)

    result: Dict[str, List[Dict[str, float]]] = {"mains": [], "claves": []}

    cutoff_index: int | None = None
    cutoff_date: str | None = None
    if cutoff_draw_id is not None:
        cutoff_index, cutoff_date = _get_cutoff_info_for_draw(cutoff_draw_id)

    for number_type, key in (("main", "mains"), ("clave", "claves")):
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


def save_el_gordo_gap_simulation_result(
    scores: Dict[str, List[Dict[str, float]]],
) -> str:
    """
    Persist gap-based probabilities into the shared El Gordo simulation document.
    Only the `gap` field for each number is updated.
    """
    client = _get_mongo_client()
    db = client[MONGO_DB]
    coll = db["el_gordo_simulations"]

    meta = scores.get("_meta") or {}
    cutoff_draw_id = meta.get("cutoff_draw_id")
    cutoff_draw_index = meta.get("cutoff_draw_index")
    cutoff_draw_date = meta.get("cutoff_draw_date")

    mains_scores = scores.get("mains") or []
    claves_scores = scores.get("claves") or []

    main_gap: Dict[int, float] = {
        int(row["number"]): float(row["p"]) for row in mains_scores
    }
    clave_gap: Dict[int, float] = {
        int(row["number"]): float(row["p"]) for row in claves_scores
    }

    existing = coll.find_one({"cutoff_draw_id": cutoff_draw_id}) if cutoff_draw_id else None

    if existing:
        mains_existing = existing.get("mains") or []
        claves_existing = existing.get("claves") or []

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

        claves_merged: List[Dict[str, float]] = []
        for row in claves_existing:
            num = int(row.get("number"))
            gap_val = clave_gap.get(num, float(row.get("gap") or 0.0))
            claves_merged.append(
                {
                    "number": num,
                    "freq": float(row.get("freq")) if row.get("freq") is not None else None,
                    "gap": gap_val,
                    "hot": float(row.get("hot")) if row.get("hot") is not None else None,
                }
            )
        for num, p in clave_gap.items():
            if not any(r["number"] == num for r in claves_merged):
                claves_merged.append({"number": num, "freq": None, "gap": p, "hot": None})

        coll.update_one(
            {"_id": existing["_id"]},
            {
                "$set": {
                    "cutoff_draw_id": cutoff_draw_id,
                    "cutoff_draw_index": cutoff_draw_index,
                    "cutoff_draw_date": cutoff_draw_date,
                    "mains": mains_merged,
                    "claves": claves_merged,
                    "updated_at": datetime.utcnow(),
                }
            },
        )
        sim_id = str(existing["_id"])
    else:
        mains_doc = [
            {"number": num, "freq": None, "gap": p, "hot": None} for num, p in main_gap.items()
        ]
        claves_doc = [
            {"number": num, "freq": None, "gap": p, "hot": None} for num, p in clave_gap.items()
        ]
        doc = {
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "cutoff_draw_id": cutoff_draw_id,
            "cutoff_draw_index": cutoff_draw_index,
            "cutoff_draw_date": cutoff_draw_date,
            "mains": mains_doc,
            "claves": claves_doc,
        }
        res = coll.insert_one(doc)
        sim_id = str(res.inserted_id)

    client.close()
    return sim_id


if __name__ == "__main__":
    train_all_el_gordo_gap_models()

