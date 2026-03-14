"""
Training utilities for Euromillones ML models (step 2 of new_flow).

This script currently focuses on **building the per-number training dataset**
from `euromillones_feature`. It does NOT train models yet; that will be added
in the next step.

Usage (from project root, after Mongo is populated and `build_euromillones_feature.py` has run):

    python scripts/train_euromillones_model.py

This will:
  - Read all rows from `euromillones_feature` sorted by `source_index`.
  - Build per-number feature rows X_t for mains (1..50) and stars (1..12),
    with labels y_{t+1} indicating whether that number appears in the *next* draw.
  - Save two CSV files:
        data/euromillones/euromillones_main_dataset.csv
        data/euromillones/euromillones_star_dataset.csv
"""

from __future__ import annotations

import argparse
import os
from typing import Dict, List, Tuple

import joblib  # type: ignore[import-untyped]
import pandas as pd  # type: ignore[import-untyped]
from dotenv import load_dotenv  # type: ignore[import-untyped]
from pymongo import ASCENDING, MongoClient
from sklearn.ensemble import GradientBoostingClassifier  # type: ignore[import-untyped]
from sklearn.utils.class_weight import compute_sample_weight  # type: ignore[import-untyped]


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

load_dotenv()  # Load MONGO_URI / MONGO_DB if present

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "lottery")

FEATURE_COLLECTION = "euromillones_feature"

MAIN_MIN, MAIN_MAX = 1, 50
STAR_MIN, STAR_MAX = 1, 12

MODEL_DIR_DEFAULT = os.path.join(BASE_DIR, "backend", "models", "euromillones_ml")


def _get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI)


def _weekday_to_index(name: str) -> int:
    """
    Map weekday name (English) to index 0..6.

    `build_euromillones_feature.py` stores `dia_semana` using datetime.strftime("%A").
    """
    name = (name or "").strip()
    mapping: Dict[str, int] = {
        "Monday": 0,
        "Tuesday": 1,
        "Wednesday": 2,
        "Thursday": 3,
        "Friday": 4,
        "Saturday": 5,
        "Sunday": 6,
    }
    return mapping.get(name, -1)


def _load_feature_rows() -> List[dict]:
    """
    Load all rows from `euromillones_feature` sorted by source_index ascending.

    Each document (row t) contains:
      - id_sorteo, fecha_sorteo, dia_semana
      - main_number (list of 5 ints), star_number (list of 2 ints)
      - main_dx (length 50), star_dx (length 12)
      - frequency (length 50 + 12)
      - gap (length 50 + 12)
      - source_index (int, 0-based chronological index)
    """
    client = _get_mongo_client()
    db = client[MONGO_DB]
    coll = db[FEATURE_COLLECTION]

    docs = list(
        coll.find(
            {},
            projection={
                "id_sorteo": 1,
                "fecha_sorteo": 1,
                "dia_semana": 1,
                "main_number": 1,
                "star_number": 1,
                "main_dx": 1,
                "star_dx": 1,
                "frequency": 1,
                "gap": 1,
                "source_index": 1,
            },
        ).sort("source_index", ASCENDING)
    )

    client.close()
    return docs


def build_per_number_datasets(
    cutoff_draw_id: str | None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build per-number training datasets (mains and stars) from `euromillones_feature`.

    For each row t (except the very last one), and for each candidate number n:
      - Features X_t(n) are derived from the row t:
            - frequency for that number at time t
            - gap for that number at time t
            - indicator whether it appeared in the *current* draw (main_dx / star_dx)
            - weekday index (0..6) of the current draw
      - Label y_{t+1}(n) = 1 if n appears in the *next* draw (t+1), else 0.

    Returns:
        (df_main, df_star)
            df_main: one row per (t, main number 1..50)
            df_star: one row per (t, star number 1..12)
    """
    docs = _load_feature_rows()
    if cutoff_draw_id:
        cutoff_draw_id_norm = str(cutoff_draw_id).strip()
        cutoff_idx = -1
        for i, d in enumerate(docs):
            cur_id = str(d.get("id_sorteo") or "").strip()
            if cur_id == cutoff_draw_id_norm:
                cutoff_idx = i
                break
        if cutoff_idx == -1:
            raise RuntimeError(
                f"cutoff_draw_id {cutoff_draw_id!r} not found in euromillones_feature"
            )
        # keep rows from first draw up to and including cutoff draw
        docs = docs[: cutoff_idx + 1]
    if len(docs) < 2:
        raise RuntimeError(
            "Need at least 2 rows in euromillones_feature to build (t, t+1) dataset."
        )

    main_rows: List[Dict[str, object]] = []
    star_rows: List[Dict[str, object]] = []

    for idx in range(len(docs) - 1):
        cur = docs[idx]
        nxt = docs[idx + 1]

        cur_id = str(cur.get("id_sorteo") or "").strip()
        cur_fecha = str(cur.get("fecha_sorteo") or "").strip()
        cur_dia = str(cur.get("dia_semana") or "").strip()
        weekday_idx = _weekday_to_index(cur_dia)

        source_index = int(cur.get("source_index", idx))

        main_numbers_next = {
            int(n) for n in (nxt.get("main_number") or []) if isinstance(n, int)
        }
        star_numbers_next = {
            int(s) for s in (nxt.get("star_number") or []) if isinstance(s, int)
        }

        main_dx = list(cur.get("main_dx") or [])
        star_dx = list(cur.get("star_dx") or [])
        frequency = list(cur.get("frequency") or [])
        gap = list(cur.get("gap") or [])
        cur_mains = [int(x) for x in (cur.get("main_number") or []) if isinstance(x, int)]
        cur_stars = [int(x) for x in (cur.get("star_number") or []) if isinstance(x, int)]

        total_draws = source_index + 1
        draw_sum_mains = sum(cur_mains) if cur_mains else 0
        draw_even_mains = sum(1 for x in cur_mains if x % 2 == 0)
        draw_sum_stars = sum(cur_stars) if cur_stars else 0
        draw_even_stars = sum(1 for x in cur_stars if x % 2 == 0)

        # Build rows for all main numbers 1..50
        for n in range(MAIN_MIN, MAIN_MAX + 1):
            idx_main = n - 1
            # frequency/gap arrays are [50 mains] + [12 stars]
            freq_val = frequency[idx_main] if idx_main < len(frequency) else 0
            gap_raw = gap[idx_main] if idx_main < len(gap) else None
            # Use -1 for "never seen" (gap None) so models can treat it as a large missing gap
            gap_val = -1 if gap_raw is None else int(gap_raw)
            freq_norm = (int(freq_val) / total_draws) if total_draws > 0 else 0.0
            gap_cap = min(gap_val, 100) if gap_val >= 0 else -1

            is_current_main = 0
            if idx_main < len(main_dx):
                try:
                    is_current_main = 1 if int(main_dx[idx_main]) != 0 else 0
                except Exception:
                    is_current_main = 0

            label_next = 1 if n in main_numbers_next else 0

            row: Dict[str, object] = {
                "source_index": source_index,
                "id_sorteo": cur_id,
                "fecha_sorteo": cur_fecha,
                "weekday_idx": weekday_idx,
                "number": n,
                "freq": int(freq_val),
                "gap": gap_val,
                "freq_norm": freq_norm,
                "gap_cap": gap_cap,
                "draw_sum_mains": draw_sum_mains,
                "draw_even_mains": draw_even_mains,
                "draw_sum_stars": draw_sum_stars,
                "draw_even_stars": draw_even_stars,
                "is_current_main": is_current_main,
                "label_next_appears": label_next,
            }
            main_rows.append(row)

        # Build rows for all star numbers 1..12
        for s in range(STAR_MIN, STAR_MAX + 1):
            idx_star = s - 1
            star_offset = 50  # stars start after 50 mains in frequency/gap arrays
            freq_idx = star_offset + idx_star
            gap_idx = star_offset + idx_star

            freq_val = frequency[freq_idx] if freq_idx < len(frequency) else 0
            gap_raw = gap[gap_idx] if gap_idx < len(gap) else None
            gap_val = -1 if gap_raw is None else int(gap_raw)
            freq_norm = (int(freq_val) / total_draws) if total_draws > 0 else 0.0
            gap_cap = min(gap_val, 100) if gap_val >= 0 else -1

            is_current_star = 0
            if idx_star < len(star_dx):
                try:
                    is_current_star = 1 if int(star_dx[idx_star]) != 0 else 0
                except Exception:
                    is_current_star = 0

            label_next = 1 if s in star_numbers_next else 0

            row_s: Dict[str, object] = {
                "source_index": source_index,
                "id_sorteo": cur_id,
                "fecha_sorteo": cur_fecha,
                "weekday_idx": weekday_idx,
                "number": s,
                "freq": int(freq_val),
                "gap": gap_val,
                "freq_norm": freq_norm,
                "gap_cap": gap_cap,
                "draw_sum_mains": draw_sum_mains,
                "draw_even_mains": draw_even_mains,
                "draw_sum_stars": draw_sum_stars,
                "draw_even_stars": draw_even_stars,
                "is_current_star": is_current_star,
                "label_next_appears": label_next,
            }
            star_rows.append(row_s)

    df_main = pd.DataFrame(main_rows)
    df_star = pd.DataFrame(star_rows)
    return df_main, df_star


def _default_output_dir() -> str:
    return os.path.join(BASE_DIR, "data", "euromillones")


def prepare_euromillones_dataset(
    cutoff_draw_id: str | None = None,
    out_dir: str | None = None,
) -> Dict[str, object]:
    """
    Build and persist the Euromillones per-number datasets.

    Shared entry point for both the CLI script and the FastAPI backend.
    Writes two CSV files and returns basic metadata.

    Returns a dict with:
      - out_dir: output directory used
      - main_path / star_path: CSV paths
      - main_rows / star_rows: row counts
    """
    df_main, df_star = build_per_number_datasets(cutoff_draw_id=cutoff_draw_id)

    if out_dir is None:
        out_dir = _default_output_dir()
    os.makedirs(out_dir, exist_ok=True)

    main_path = os.path.join(out_dir, "euromillones_main_dataset.csv")
    star_path = os.path.join(out_dir, "euromillones_star_dataset.csv")

    df_main.to_csv(main_path, index=False)
    df_star.to_csv(star_path, index=False)

    return {
        "cutoff_draw_id": cutoff_draw_id,
        "out_dir": out_dir,
        "main_path": main_path,
        "star_path": star_path,
        "main_rows": int(df_main.shape[0]),
        "star_rows": int(df_star.shape[0]),
    }


def train_euromillones_models(
    cutoff_draw_id: str | None = None,
    dataset_dir: str | None = None,
    model_dir: str | None = None,
) -> Dict[str, object]:
    """
    Train Gradient Boosting models for Euromillones mains and stars using the
    per-number dataset. Returns basic metrics and model paths.

    If dataset_dir is not provided, it will be created/refreshed first via
    `prepare_euromillones_dataset`.
    """
    if dataset_dir is None:
        # Ensure dataset exists (build from first draw up to cutoff_draw_id if given)
        ds_info = prepare_euromillones_dataset(cutoff_draw_id=cutoff_draw_id, out_dir=None)
        dataset_dir = ds_info["out_dir"]
    main_path = os.path.join(dataset_dir, "euromillones_main_dataset.csv")
    star_path = os.path.join(dataset_dir, "euromillones_star_dataset.csv")

    if model_dir is None:
        model_dir = MODEL_DIR_DEFAULT
    os.makedirs(model_dir, exist_ok=True)

    df_main = pd.read_csv(main_path)
    df_star = pd.read_csv(star_path)

    results: Dict[str, object] = {
        "cutoff_draw_id": cutoff_draw_id,
        "dataset_dir": dataset_dir,
        "model_dir": model_dir,
    }

    # Time-based split: last 20% of draws by source_index (no shuffle, no future leakage)
    def time_split(df: pd.DataFrame, feature_cols: List[str]) -> Tuple:
        idx_vals = df["source_index"].unique()
        idx_vals = sorted(idx_vals)
        n_val = max(1, int(len(idx_vals) * 0.2))
        val_indices = set(idx_vals[-n_val:])
        train_mask = ~df["source_index"].isin(val_indices)
        X_train = df.loc[train_mask, feature_cols].values
        y_train = df.loc[train_mask, "label_next_appears"].values
        X_val = df.loc[~train_mask, feature_cols].values
        y_val = df.loc[~train_mask, "label_next_appears"].values
        return X_train, X_val, y_train, y_val

    # Features: number, dx (is_current_*), frequency, gap, plus normalized/capped and draw-level (sum/even-odd)
    main_features = [
        "weekday_idx", "number", "freq", "gap", "freq_norm", "gap_cap",
        "draw_sum_mains", "draw_even_mains", "draw_sum_stars", "draw_even_stars",
        "is_current_main",
    ]
    star_features = [
        "weekday_idx", "number", "freq", "gap", "freq_norm", "gap_cap",
        "draw_sum_mains", "draw_even_mains", "draw_sum_stars", "draw_even_stars",
        "is_current_star",
    ]

    X_train_m, X_val_m, y_train_m, y_val_m = time_split(df_main, main_features)
    sw_m = compute_sample_weight("balanced", y_train_m)
    # Lighter params for VPS: n_estimators=100 (La Primitiva default) to avoid OOM/timeout
    clf_main = GradientBoostingClassifier(
        n_estimators=100,
        max_depth=5,
        learning_rate=0.08,
        random_state=42,
    )
    clf_main.fit(X_train_m, y_train_m, sample_weight=sw_m)
    main_acc = float(clf_main.score(X_val_m, y_val_m))
    main_model_path = os.path.join(model_dir, "euromillones_main_gb.joblib")
    joblib.dump({"model": clf_main, "features": main_features}, main_model_path)

    X_train_s, X_val_s, y_train_s, y_val_s = time_split(df_star, star_features)
    sw_s = compute_sample_weight("balanced", y_train_s)
    clf_star = GradientBoostingClassifier(
        n_estimators=100,
        max_depth=5,
        learning_rate=0.08,
        random_state=42,
    )
    clf_star.fit(X_train_s, y_train_s, sample_weight=sw_s)
    star_acc = float(clf_star.score(X_val_s, y_val_s))
    star_model_path = os.path.join(model_dir, "euromillones_star_gb.joblib")
    joblib.dump({"model": clf_star, "features": star_features}, star_model_path)

    results.update(
        {
            "main_model_path": main_model_path,
            "star_model_path": star_model_path,
            "main_accuracy": main_acc,
            "star_accuracy": star_acc,
        }
    )
    return results


def compute_euromillones_probabilities(
    cutoff_draw_id: str | None = None,
) -> Dict[str, object]:
    """
    Compute per-number probabilities for the *next* Euromillones draw
    (mains 1..50, stars 1..12) using the trained Gradient Boosting models.

    cutoff_draw_id:
        - If provided, use the feature row with this id_sorteo as the cutoff draw.
        - If None, use the latest row in `euromillones_feature`.
    """
    client = _get_mongo_client()
    db = client[MONGO_DB]
    coll = db[FEATURE_COLLECTION]

    if cutoff_draw_id:
        doc = coll.find_one({"id_sorteo": str(cutoff_draw_id).strip()})
        if not doc:
            client.close()
            raise RuntimeError(f"cutoff_draw_id {cutoff_draw_id!r} not found in euromillones_feature")
    else:
        doc = coll.find_one(sort=[("fecha_sorteo", -1)])
        if not doc:
            client.close()
            raise RuntimeError("No rows found in euromillones_feature")

    draw_id = str(doc.get("id_sorteo") or "").strip()
    fecha = str(doc.get("fecha_sorteo") or "").strip()
    dia = str(doc.get("dia_semana") or "").strip()
    weekday_idx = _weekday_to_index(dia)

    main_dx = list(doc.get("main_dx") or [])
    star_dx = list(doc.get("star_dx") or [])
    frequency = list(doc.get("frequency") or [])
    gap = list(doc.get("gap") or [])
    cur_mains = [int(x) for x in (doc.get("main_number") or []) if isinstance(x, int)]
    cur_stars = [int(x) for x in (doc.get("star_number") or []) if isinstance(x, int)]
    source_index = int(doc.get("source_index", 0))
    total_draws = source_index + 1
    draw_sum_mains = sum(cur_mains) if cur_mains else 0
    draw_even_mains = sum(1 for x in cur_mains if x % 2 == 0)
    draw_sum_stars = sum(cur_stars) if cur_stars else 0
    draw_even_stars = sum(1 for x in cur_stars if x % 2 == 0)

    # Build feature rows for mains (same columns as training)
    main_rows = []
    for n in range(MAIN_MIN, MAIN_MAX + 1):
        idx_main = n - 1
        freq_val = frequency[idx_main] if idx_main < len(frequency) else 0
        gap_raw = gap[idx_main] if idx_main < len(gap) else None
        gap_val = -1 if gap_raw is None else int(gap_raw)
        freq_norm = (int(freq_val) / total_draws) if total_draws > 0 else 0.0
        gap_cap = min(gap_val, 100) if gap_val >= 0 else -1
        is_current_main = 0
        if idx_main < len(main_dx):
            try:
                is_current_main = 1 if int(main_dx[idx_main]) != 0 else 0
            except Exception:
                pass
        main_rows.append({
            "weekday_idx": weekday_idx,
            "number": n,
            "freq": int(freq_val),
            "gap": gap_val,
            "freq_norm": freq_norm,
            "gap_cap": gap_cap,
            "draw_sum_mains": draw_sum_mains,
            "draw_even_mains": draw_even_mains,
            "draw_sum_stars": draw_sum_stars,
            "draw_even_stars": draw_even_stars,
            "is_current_main": is_current_main,
        })

    # Build feature rows for stars
    star_rows = []
    star_offset = 50
    for s in range(STAR_MIN, STAR_MAX + 1):
        idx_star = s - 1
        freq_idx = star_offset + idx_star
        gap_idx = star_offset + idx_star
        freq_val = frequency[freq_idx] if freq_idx < len(frequency) else 0
        gap_raw = gap[gap_idx] if gap_idx < len(gap) else None
        gap_val = -1 if gap_raw is None else int(gap_raw)
        freq_norm = (int(freq_val) / total_draws) if total_draws > 0 else 0.0
        gap_cap = min(gap_val, 100) if gap_val >= 0 else -1
        is_current_star = 0
        if idx_star < len(star_dx):
            try:
                is_current_star = 1 if int(star_dx[idx_star]) != 0 else 0
            except Exception:
                pass
        star_rows.append({
            "weekday_idx": weekday_idx,
            "number": s,
            "freq": int(freq_val),
            "gap": gap_val,
            "freq_norm": freq_norm,
            "gap_cap": gap_cap,
            "draw_sum_mains": draw_sum_mains,
            "draw_even_mains": draw_even_mains,
            "draw_sum_stars": draw_sum_stars,
            "draw_even_stars": draw_even_stars,
            "is_current_star": is_current_star,
        })

    client.close()

    df_main = pd.DataFrame(main_rows)
    df_star = pd.DataFrame(star_rows)

    model_dir = MODEL_DIR_DEFAULT
    main_model_path = os.path.join(model_dir, "euromillones_main_gb.joblib")
    star_model_path = os.path.join(model_dir, "euromillones_star_gb.joblib")

    if not os.path.exists(main_model_path) or not os.path.exists(star_model_path):
        raise RuntimeError(
            "Euromillones ML models not found. Train them first from the training tools."
        )

    saved_main = joblib.load(main_model_path)
    main_model: GradientBoostingClassifier = saved_main["model"]
    main_features: List[str] = saved_main["features"]
    X_main = df_main[main_features].values
    main_probs = main_model.predict_proba(X_main)[:, 1]

    saved_star = joblib.load(star_model_path)
    star_model: GradientBoostingClassifier = saved_star["model"]
    star_features: List[str] = saved_star["features"]
    X_star = df_star[star_features].values
    star_probs = star_model.predict_proba(X_star)[:, 1]

    mains = [
        {"number": int(n), "p": float(p)}
        for n, p in zip(df_main["number"].tolist(), main_probs.tolist())
    ]
    stars = [
        {"number": int(n), "p": float(p)}
        for n, p in zip(df_star["number"].tolist(), star_probs.tolist())
    ]
    mains.sort(key=lambda x: x["p"], reverse=True)
    stars.sort(key=lambda x: x["p"], reverse=True)

    return {
        "cutoff_draw_id": cutoff_draw_id or draw_id,
        "draw_id": draw_id,
        "fecha_sorteo": fecha,
        "mains": mains,
        "stars": stars,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build per-number training datasets for Euromillones from euromillones_feature."
    )
    parser.add_argument(
        "--out-dir",
        type=str,
        default=_default_output_dir(),
        help="Output directory for CSV files (default: data/euromillones under project root).",
    )
    parser.add_argument(
        "--cutoff-draw-id",
        type=str,
        default=None,
        help="Optional id_sorteo; only draws up to this one (inclusive) are used.",
    )
    args = parser.parse_args()

    info = prepare_euromillones_dataset(
        cutoff_draw_id=args.cutoff_draw_id,
        out_dir=args.out_dir,
    )

    print(f"Main dataset: {info['main_rows']} rows -> {info['main_path']}")
    print(f"Star dataset: {info['star_rows']} rows -> {info['star_path']}")


if __name__ == "__main__":
    main()

