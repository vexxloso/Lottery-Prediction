import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Literal, Tuple

import joblib  # type: ignore[import-untyped]
import numpy as np
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
class DrawFeatures:
    draw_index: int
    main_numbers: List[int]
    clave: int | None


def _get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI)


def _load_el_gordo_draws() -> Tuple[List[DrawFeatures], int]:
    """
    Load all El Gordo draw feature rows sorted by draw_index ascending.

    Uses collection `el_gordo_draw_features`.
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

    draws: List[DrawFeatures] = []
    max_index = -1
    for doc in docs:
        idx = int(doc.get("draw_index", 0))
        mains = [int(n) for n in (doc.get("main_numbers") or [])]
        clave_val = doc.get("clave")
        try:
            clave_int = int(clave_val) if clave_val is not None else None
        except (TypeError, ValueError):
            clave_int = None
        draws.append(
            DrawFeatures(draw_index=idx, main_numbers=mains, clave=clave_int)
        )
        if idx > max_index:
            max_index = idx

    client.close()
    return draws, max_index


def _build_presence_index(
    draws: List[DrawFeatures],
) -> Tuple[Dict[int, List[int]], Dict[int, List[int]]]:
    """
    Build per-number appearance indices for mains and claves.

    Returns:
        main_indices[number] -> sorted list of draw_index where number appears as main.
        clave_indices[number] -> sorted list of draw_index where number appears as clave.
    """
    main_indices: Dict[int, List[int]] = {
        n: [] for n in range(MAIN_MIN, MAIN_MAX + 1)
    }
    clave_indices: Dict[int, List[int]] = {
        c: [] for c in range(CLAVE_MIN, CLAVE_MAX + 1)
    }

    for d in draws:
        for n in d.main_numbers:
            if MAIN_MIN <= n <= MAIN_MAX:
                main_indices[n].append(d.draw_index)
        if d.clave is not None and CLAVE_MIN <= d.clave <= CLAVE_MAX:
            clave_indices[d.clave].append(d.draw_index)

    return main_indices, clave_indices


def _count_in_window(indices: List[int], start_idx: int, end_idx: int) -> int:
    """
    Count how many appearances fall in [start_idx, end_idx] (both inclusive).
    Assumes `indices` is sorted ascending.
    """
    if not indices:
        return 0
    return sum(1 for idx in indices if start_idx <= idx <= end_idx)


def _build_frequency_dataset(
    number_type: NumberType,
    min_history_draws: int = 120,
) -> pd.DataFrame:
    """
    Build training dataset for the frequency-based model (Model F) for El Gordo.

    number_type:
        "main" -> numbers 1..54 using main_numbers labels
        "clave" -> numbers 0..9 using clave labels
    """
    draws, max_index = _load_el_gordo_draws()
    if max_index < min_history_draws + 1:
        raise RuntimeError(
            "Not enough El Gordo draw history to build frequency dataset."
        )

    main_indices, clave_indices = _build_presence_index(draws)

    mains_by_index: Dict[int, set[int]] = {}
    claves_by_index: Dict[int, set[int]] = {}
    for d in draws:
        mains_by_index[d.draw_index] = set(d.main_numbers)
        claves_by_index[d.draw_index] = (
            {d.clave} if d.clave is not None else set()
        )

    if number_type == "main":
        num_min, num_max = MAIN_MIN, MAIN_MAX
        indices_by_number = main_indices
    else:
        num_min, num_max = CLAVE_MIN, CLAVE_MAX
        indices_by_number = clave_indices

    rows: List[Dict[str, float]] = []

    for draw in draws:
        t = draw.draw_index
        if t <= min_history_draws:
            continue

        # windows based on t-1 (history prior to prediction draw)
        start_10 = t - 10
        start_30 = t - 30
        start_50 = t - 50
        start_100 = t - 100
        start_20_recent = t - 20
        start_40_prev = t - 40

        for n in range(num_min, num_max + 1):
            idx_list = indices_by_number.get(n, [])
            if not idx_list:
                freq_all = 0
                freq_10 = freq_30 = freq_50 = freq_100 = 0
                freq_recent_20 = freq_prev_20 = 0
            else:
                freq_all = _count_in_window(idx_list, 0, t - 1)
                freq_10 = _count_in_window(idx_list, max(start_10, 0), t - 1)
                freq_30 = _count_in_window(idx_list, max(start_30, 0), t - 1)
                freq_50 = _count_in_window(idx_list, max(start_50, 0), t - 1)
                freq_100 = _count_in_window(idx_list, max(start_100, 0), t - 1)
                freq_recent_20 = _count_in_window(
                    idx_list, max(start_20_recent, 0), t - 1
                )
                freq_prev_20 = _count_in_window(
                    idx_list, max(start_40_prev, 0), max(t - 21, 0)
                )

            rate_10 = freq_10 / 10.0
            rate_30 = freq_30 / 30.0
            rate_50 = freq_50 / 50.0
            rate_100 = freq_100 / 100.0
            rate_all = freq_all / float(t)

            trend_20 = freq_recent_20 - freq_prev_20
            trend_20_rate = trend_20 / 20.0

            row: Dict[str, float] = {
                "draw_index": float(t),
                "number": float(n),
                "freq_10": float(freq_10),
                "freq_30": float(freq_30),
                "freq_50": float(freq_50),
                "freq_100": float(freq_100),
                "freq_all": float(freq_all),
                "rate_10": float(rate_10),
                "rate_30": float(rate_30),
                "rate_50": float(rate_50),
                "rate_100": float(rate_100),
                "rate_all": float(rate_all),
                "trend_20": float(trend_20),
                "trend_20_rate": float(trend_20_rate),
            }

            if number_type == "main":
                label_set = mains_by_index.get(t, set())
            else:
                label_set = claves_by_index.get(t, set())
            row["label"] = 1.0 if n in label_set else 0.0

            rows.append(row)

    df = pd.DataFrame(rows)

    # Per-draw ranks
    for col in ("freq_30", "freq_100", "freq_all"):
        rank_col = f"rank_{col}"
        df[rank_col] = (
            df.groupby("draw_index")[col]
            .rank(method="average", ascending=False)
            .astype(float)
        )

    df = df.rename(
        columns={
            "rank_freq_30": "rank_freq_30",
            "rank_freq_100": "rank_freq_100",
            "rank_freq_all": "rank_freq_all",
        }
    )

    return df


def _train_frequency_model(number_type: NumberType, random_state: int = 42) -> None:
    """
    Train a Gradient Boosting classifier for the frequency-based model (El Gordo).
    """
    df = _build_frequency_dataset(number_type)

    feature_cols = [
        "freq_10",
        "freq_30",
        "freq_50",
        "freq_100",
        "freq_all",
        "rate_10",
        "rate_30",
        "rate_50",
        "rate_100",
        "rate_all",
        "trend_20",
        "trend_20_rate",
        "rank_freq_30",
        "rank_freq_100",
        "rank_freq_all",
    ]

    X = df[feature_cols].values
    y = df["label"].values

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=random_state, stratify=y
    )

    clf = GradientBoostingClassifier(random_state=random_state)
    clf.fit(X_train, y_train)

    val_score = clf.score(X_val, y_val)
    print(f"[el_gordo][frequency][{number_type}] validation accuracy: {val_score:.4f}")

    model_dir = os.path.join(os.path.dirname(__file__), "..", "models", "el_gordo")
    model_dir = os.path.normpath(model_dir)
    os.makedirs(model_dir, exist_ok=True)
    model_path = os.path.join(model_dir, f"freq_{number_type}.joblib")
    joblib.dump({"model": clf, "features": feature_cols}, model_path)
    print(f"[el_gordo][frequency][{number_type}] model saved to {model_path}")


def train_all_el_gordo_frequency_models() -> None:
    """
    Train frequency-based models for El Gordo (mains + claves).
    """
    _train_frequency_model("main")
    _train_frequency_model("clave")


def _get_cutoff_info_for_draw(draw_id: str) -> Tuple[int, str | None]:
    """
    Given an El Gordo draw_id, return its draw_index and draw_date.
    """
    client = _get_mongo_client()
    db = client[MONGO_DB]
    coll = db["el_gordo_draw_features"]
    doc = coll.find_one(
        {"draw_id": draw_id}, projection={"draw_index": 1, "draw_date": 1}
    )
    client.close()
    if not doc or "draw_index" not in doc:
        raise RuntimeError(f"Unknown El Gordo draw_id: {draw_id}")
    idx = int(doc["draw_index"])
    date_str = str(doc.get("draw_date") or "") or None
    return idx, date_str


def _build_current_frequency_features(
    number_type: NumberType,
    cutoff_draw_index: int | None = None,
) -> pd.DataFrame:
    """
    Build one feature row per number for the *next* draw (t = max_index + 1),
    using the same frequency features as the training dataset, but without labels.
    """
    draws, max_index = _load_el_gordo_draws()
    if max_index < 1:
        raise RuntimeError("Not enough El Gordo draw history to build features.")

    if cutoff_draw_index is not None:
        draws = [d for d in draws if d.draw_index <= cutoff_draw_index]
        if not draws:
            raise RuntimeError("No El Gordo draws found up to the selected cutoff.")
        max_index = max(d.draw_index for d in draws)

    main_indices, clave_indices = _build_presence_index(draws)

    if number_type == "main":
        num_min, num_max = MAIN_MIN, MAIN_MAX
        indices_by_number = main_indices
    else:
        num_min, num_max = CLAVE_MIN, CLAVE_MAX
        indices_by_number = clave_indices

    t = max_index + 1

    start_10 = t - 10
    start_30 = t - 30
    start_50 = t - 50
    start_100 = t - 100
    start_20_recent = t - 20
    start_40_prev = t - 40

    rows: List[Dict[str, float]] = []

    for n in range(num_min, num_max + 1):
        idx_list = indices_by_number.get(n, [])
        if not idx_list:
            freq_all = 0
            freq_10 = freq_30 = freq_50 = freq_100 = 0
            freq_recent_20 = freq_prev_20 = 0
        else:
            freq_all = _count_in_window(idx_list, 0, t - 1)
            freq_10 = _count_in_window(idx_list, max(start_10, 0), t - 1)
            freq_30 = _count_in_window(idx_list, max(start_30, 0), t - 1)
            freq_50 = _count_in_window(idx_list, max(start_50, 0), t - 1)
            freq_100 = _count_in_window(idx_list, max(start_100, 0), t - 1)
            freq_recent_20 = _count_in_window(
                idx_list, max(start_20_recent, 0), t - 1
            )
            freq_prev_20 = _count_in_window(
                idx_list, max(start_40_prev, 0), max(t - 21, 0)
            )

        rate_10 = freq_10 / 10.0
        rate_30 = freq_30 / 30.0
        rate_50 = freq_50 / 50.0
        rate_100 = freq_100 / 100.0
        rate_all = freq_all / float(t)

        trend_20 = freq_recent_20 - freq_prev_20
        trend_20_rate = trend_20 / 20.0

        rows.append(
            {
                "draw_index": float(t),
                "number": float(n),
                "freq_10": float(freq_10),
                "freq_30": float(freq_30),
                "freq_50": float(freq_50),
                "freq_100": float(freq_100),
                "freq_all": float(freq_all),
                "rate_10": float(rate_10),
                "rate_30": float(rate_30),
                "rate_50": float(rate_50),
                "rate_100": float(rate_100),
                "rate_all": float(rate_all),
                "trend_20": float(trend_20),
                "trend_20_rate": float(trend_20_rate),
            }
        )

    df = pd.DataFrame(rows)

    for col in ("freq_30", "freq_100", "freq_all"):
        rank_col = f"rank_{col}"
        df[rank_col] = (
            df.groupby("draw_index")[col]
            .rank(method="average", ascending=False)
            .astype(float)
        )

    df = df.rename(
        columns={
            "rank_freq_30": "rank_freq_30",
            "rank_freq_100": "rank_freq_100",
            "rank_freq_all": "rank_freq_all",
        }
    )

    return df


def predict_next_el_gordo_frequency_scores(
    cutoff_draw_id: str | None = None,
) -> Dict[str, List[Dict[str, float]]]:
    """
    Compute frequency-based probabilities for the next El Gordo draw.
    """
    base_model_dir = os.path.join(os.path.dirname(__file__), "..", "models", "el_gordo")
    base_model_dir = os.path.normpath(base_model_dir)

    result: Dict[str, List[Dict[str, float]]] = {"mains": [], "claves": []}

    cutoff_index: int | None = None
    cutoff_date: str | None = None
    if cutoff_draw_id is not None:
        cutoff_index, cutoff_date = _get_cutoff_info_for_draw(cutoff_draw_id)

    for number_type, key in (("main", "mains"), ("clave", "claves")):
        model_path = os.path.join(base_model_dir, f"freq_{number_type}.joblib")
        if not os.path.exists(model_path):
            raise RuntimeError(
                f"Frequency model for {number_type} numbers not found at {model_path}. "
                "Train it first from the simulation tools."
            )

        saved = joblib.load(model_path)
        model: GradientBoostingClassifier = saved["model"]
        feature_cols: List[str] = saved["features"]

        df_features = _build_current_frequency_features(
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


def save_el_gordo_frequency_simulation_result(
    scores: Dict[str, List[Dict[str, float]]],
) -> str:
    """
    Persist frequency-based probabilities into an El Gordo simulation document.

    For each cutoff_draw_id there is a single document in collection
    `el_gordo_simulations`. This sets/updates the `freq` field for each number.
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

    main_freq: Dict[int, float] = {
        int(row["number"]): float(row["p"]) for row in mains_scores
    }
    clave_freq: Dict[int, float] = {
        int(row["number"]): float(row["p"]) for row in claves_scores
    }

    existing = coll.find_one({"cutoff_draw_id": cutoff_draw_id}) if cutoff_draw_id else None

    if existing:
        mains_existing = existing.get("mains") or []
        claves_existing = existing.get("claves") or []

        mains_merged: List[Dict[str, float]] = []
        for row in mains_existing:
            num = int(row.get("number"))
            freq_val = main_freq.get(num, float(row.get("freq") or 0.0))
            mains_merged.append(
                {
                    "number": num,
                    "freq": freq_val,
                    "gap": float(row.get("gap")) if row.get("gap") is not None else None,
                    "hot": float(row.get("hot")) if row.get("hot") is not None else None,
                }
            )
        for num, p in main_freq.items():
            if not any(r["number"] == num for r in mains_merged):
                mains_merged.append({"number": num, "freq": p, "gap": None, "hot": None})

        claves_merged: List[Dict[str, float]] = []
        for row in claves_existing:
            num = int(row.get("number"))
            freq_val = clave_freq.get(num, float(row.get("freq") or 0.0))
            claves_merged.append(
                {
                    "number": num,
                    "freq": freq_val,
                    "gap": float(row.get("gap")) if row.get("gap") is not None else None,
                    "hot": float(row.get("hot")) if row.get("hot") is not None else None,
                }
            )
        for num, p in clave_freq.items():
            if not any(r["number"] == num for r in claves_merged):
                claves_merged.append({"number": num, "freq": p, "gap": None, "hot": None})

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
            {"number": num, "freq": p, "gap": None, "hot": None}
            for num, p in main_freq.items()
        ]
        claves_doc = [
            {"number": num, "freq": p, "gap": None, "hot": None}
            for num, p in clave_freq.items()
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
    train_all_el_gordo_frequency_models()

