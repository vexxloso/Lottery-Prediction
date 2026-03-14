"""
Training utilities for El Gordo ML models (step 2 of new_flow).

This first version focuses on **building the per-number training dataset**
from `el_gordo_feature`. It does NOT train models yet.

El Gordo specifics:
  - 5 main numbers in [1, 54]
  - 1 clave number in [0, 9]

For each draw t (row t in `el_gordo_feature`) we build:
  - Features X_t(n) for every candidate main n in 1..54
  - Features X_t(c) for every candidate clave c in 0..9
  - Labels y_{t+1}(n) / y_{t+1}(c) indicating whether that number appears in draw t+1.

Usage (after `build_el_gordo_feature.py` has run):

    python scripts/train_el_gordo_model.py --cutoff_draw_id <id_sorteo_optional>

This will:
  - Read all rows from `el_gordo_feature` sorted by `source_index`
  - Build two DataFrames:
        df_main: one row per (t, main number 1..54)
        df_clave: one row per (t, clave 0..9)
  - (Later we will add code to save these to CSV and train models.)
"""

from __future__ import annotations

import argparse
import os
from typing import Dict, List, Tuple

import joblib  # type: ignore[import-untyped]
import pandas as pd  # type: ignore[import-untyped]
from dotenv import load_dotenv  # type: ignore[import-untyped]
from pymongo import MongoClient  # type: ignore[import-untyped]
from sklearn.ensemble import GradientBoostingClassifier  # type: ignore[import-untyped]
from sklearn.utils.class_weight import compute_sample_weight  # type: ignore[import-untyped]


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

load_dotenv()  # Load MONGO_URI / MONGO_DB if present

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "lottery")

FEATURE_COLLECTION = "el_gordo_feature"

MAIN_MIN, MAIN_MAX = 1, 54
CLAVE_MIN, CLAVE_MAX = 0, 9

MODEL_DIR_DEFAULT = os.path.join(BASE_DIR, "backend", "models", "el_gordo_ml")


def _default_output_dir() -> str:
    return os.path.join(BASE_DIR, "data", "el_gordo")


def _get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI)


def _weekday_to_index(name: str) -> int:
    """
    Map weekday name (English) to index 0..6.

    `build_el_gordo_feature.py` stores `dia_semana` using datetime.strftime("%A").
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
    Load all rows from `el_gordo_feature` sorted by source_index ascending.

    Each document (row t) contains:
      - id_sorteo, fecha_sorteo, dia_semana
      - main_number (list of 5 ints)
      - clave (single int 0–9 or None)
      - main_dx (length 54)
      - clave_dx (length 10)
      - frequency (length 54 + 10)
      - gap (length 54 + 10)
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
                "clave": 1,
                "main_dx": 1,
                "clave_dx": 1,
                "frequency": 1,
                "gap": 1,
                "source_index": 1,
            },
        ).sort("source_index", 1)
    )

    client.close()
    return docs


def build_per_number_datasets(
    cutoff_draw_id: str | None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build per-number training datasets (mains and clave) from `el_gordo_feature`.

    For each row t (except the very last one), and for each candidate number:
      - Features X_t(main n) / X_t(clave c) are derived from row t:
            - frequency for that number at time t
            - gap for that number at time t
            - indicator whether it appeared in the *current* draw (main_dx / clave_dx)
            - weekday index (0..6) of the current draw
            - simple draw-level stats (sum/evens for mains and clave)
      - Label y_{t+1}(n/c) = 1 if that number appears in the *next* draw (t+1), else 0.

    Returns:
        (df_main, df_clave)
            df_main: one row per (t, main number 1..54)
            df_clave: one row per (t, clave 0..9)
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
                f"cutoff_draw_id {cutoff_draw_id!r} not found in el_gordo_feature"
            )
        # keep rows from first draw up to and including cutoff draw
        docs = docs[: cutoff_idx + 1]
    if len(docs) < 2:
        raise RuntimeError(
            "Need at least 2 rows in el_gordo_feature to build (t, t+1) dataset."
        )

    main_rows: List[Dict[str, object]] = []
    clave_rows: List[Dict[str, object]] = []

    for idx in range(len(docs) - 1):
        cur = docs[idx]
        nxt = docs[idx + 1]

        cur_id = str(cur.get("id_sorteo") or "").strip()
        cur_fecha = str(cur.get("fecha_sorteo") or "").strip()
        cur_dia = str(cur.get("dia_semana") or "").strip()
        weekday_idx = _weekday_to_index(cur_dia)

        source_index = int(cur.get("source_index", idx))

        # Next-draw mains and clave (labels)
        main_numbers_next = {
            int(n) for n in (nxt.get("main_number") or []) if isinstance(n, int)
        }
        clave_next_raw = nxt.get("clave")
        clave_next = int(clave_next_raw) if isinstance(clave_next_raw, int) else None

        # Current row features
        main_dx = list(cur.get("main_dx") or [])
        clave_dx = list(cur.get("clave_dx") or [])
        frequency = list(cur.get("frequency") or [])
        gap = list(cur.get("gap") or [])
        cur_mains = [
            int(x) for x in (cur.get("main_number") or []) if isinstance(x, int)
        ]
        cur_clave_raw = cur.get("clave")
        cur_clave = int(cur_clave_raw) if isinstance(cur_clave_raw, int) else None

        total_draws = source_index + 1
        draw_sum_mains = sum(cur_mains) if cur_mains else 0
        draw_even_mains = sum(1 for x in cur_mains if x % 2 == 0)
        draw_clave = cur_clave if cur_clave is not None else -1

        # ----- Main numbers 1..54 -----
        for n in range(MAIN_MIN, MAIN_MAX + 1):
            idx_main = n - MAIN_MIN
            # frequency/gap arrays are [54 mains] + [10 clave]
            freq_val = frequency[idx_main] if idx_main < len(frequency) else 0
            gap_raw = gap[idx_main] if idx_main < len(gap) else None
            # Use -1 for "never seen" so models can treat it as a large missing gap
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

            row_main: Dict[str, object] = {
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
                "draw_clave": draw_clave,
                "is_current_main": is_current_main,
                "label_next_appears": label_next,
            }
            main_rows.append(row_main)

        # ----- Clave numbers 0..9 -----
        for c in range(CLAVE_MIN, CLAVE_MAX + 1):
            idx_clave = c - CLAVE_MIN
            clave_offset = MAIN_MAX - MAIN_MIN + 1  # 54 mains first
            freq_idx = clave_offset + idx_clave
            gap_idx = clave_offset + idx_clave

            freq_val = frequency[freq_idx] if freq_idx < len(frequency) else 0
            gap_raw = gap[gap_idx] if gap_idx < len(gap) else None
            gap_val = -1 if gap_raw is None else int(gap_raw)
            freq_norm = (int(freq_val) / total_draws) if total_draws > 0 else 0.0
            gap_cap = min(gap_val, 100) if gap_val >= 0 else -1

            is_current_clave = 0
            if idx_clave < len(clave_dx):
                try:
                    is_current_clave = 1 if int(clave_dx[idx_clave]) != 0 else 0
                except Exception:
                    is_current_clave = 0

            label_next = 1 if (clave_next is not None and c == clave_next) else 0

            row_clave: Dict[str, object] = {
                "source_index": source_index,
                "id_sorteo": cur_id,
                "fecha_sorteo": cur_fecha,
                "weekday_idx": weekday_idx,
                "number": c,
                "freq": int(freq_val),
                "gap": gap_val,
                "freq_norm": freq_norm,
                "gap_cap": gap_cap,
                "draw_sum_mains": draw_sum_mains,
                "draw_even_mains": draw_even_mains,
                "draw_clave": draw_clave,
                "is_current_clave": is_current_clave,
                "label_next_appears": label_next,
            }
            clave_rows.append(row_clave)

    df_main = pd.DataFrame(main_rows)
    df_clave = pd.DataFrame(clave_rows)
    return df_main, df_clave


def prepare_el_gordo_dataset(
    cutoff_draw_id: str | None = None,
    out_dir: str | None = None,
) -> Dict[str, object]:
    """
    Build and persist the El Gordo per-number datasets.

    Shared entry point for both the CLI script and the FastAPI backend.
    Writes two CSV files and returns basic metadata.

    Returns a dict with:
      - out_dir: output directory used
      - main_path / clave_path: CSV paths
      - main_rows / clave_rows: row counts
    """
    df_main, df_clave = build_per_number_datasets(cutoff_draw_id=cutoff_draw_id)

    if out_dir is None:
      out_dir = _default_output_dir()
    os.makedirs(out_dir, exist_ok=True)

    main_path = os.path.join(out_dir, "el_gordo_main_dataset.csv")
    clave_path = os.path.join(out_dir, "el_gordo_clave_dataset.csv")

    df_main.to_csv(main_path, index=False)
    df_clave.to_csv(clave_path, index=False)

    return {
        "cutoff_draw_id": cutoff_draw_id,
        "out_dir": out_dir,
        "main_path": main_path,
        "clave_path": clave_path,
        "main_rows": int(df_main.shape[0]),
        "clave_rows": int(df_clave.shape[0]),
    }


def train_el_gordo_models(
    cutoff_draw_id: str | None = None,
    dataset_dir: str | None = None,
    model_dir: str | None = None,
) -> Dict[str, object]:
    """
    Train Gradient Boosting models for El Gordo mains and clave using the
    per-number dataset. Returns basic metrics and model paths.

    If dataset_dir is not provided, it will be created/refreshed first via
    `prepare_el_gordo_dataset`.
    """
    if dataset_dir is None:
        ds_info = prepare_el_gordo_dataset(cutoff_draw_id=cutoff_draw_id, out_dir=None)
        dataset_dir = ds_info["out_dir"]

    main_path = os.path.join(dataset_dir, "el_gordo_main_dataset.csv")
    clave_path = os.path.join(dataset_dir, "el_gordo_clave_dataset.csv")

    if model_dir is None:
        model_dir = MODEL_DIR_DEFAULT
    os.makedirs(model_dir, exist_ok=True)

    df_main = pd.read_csv(main_path)
    df_clave = pd.read_csv(clave_path)

    results: Dict[str, object] = {
        "cutoff_draw_id": cutoff_draw_id,
        "dataset_dir": dataset_dir,
        "model_dir": model_dir,
    }

    def time_split(df: pd.DataFrame, feature_cols: List[str]) -> Tuple:
        idx_vals = sorted(df["source_index"].unique())
        n_val = max(1, int(len(idx_vals) * 0.2))
        val_indices = set(idx_vals[-n_val:])
        train_mask = ~df["source_index"].isin(val_indices)
        X_train = df.loc[train_mask, feature_cols].values
        y_train = df.loc[train_mask, "label_next_appears"].values
        X_val = df.loc[~train_mask, feature_cols].values
        y_val = df.loc[~train_mask, "label_next_appears"].values
        return X_train, X_val, y_train, y_val

    main_features = [
        "weekday_idx",
        "number",
        "freq",
        "gap",
        "freq_norm",
        "gap_cap",
        "draw_sum_mains",
        "draw_even_mains",
        "draw_clave",
        "is_current_main",
    ]
    clave_features = [
        "weekday_idx",
        "number",
        "freq",
        "gap",
        "freq_norm",
        "gap_cap",
        "draw_sum_mains",
        "draw_even_mains",
        "draw_clave",
        "is_current_clave",
    ]

    X_train_m, X_val_m, y_train_m, y_val_m = time_split(df_main, main_features)
    sw_m = compute_sample_weight("balanced", y_train_m)
    # Lighter params for VPS: n_estimators=100 to avoid OOM/timeout (match La Primitiva load)
    clf_main = GradientBoostingClassifier(
        n_estimators=100,
        max_depth=5,
        learning_rate=0.08,
        random_state=42,
    )
    clf_main.fit(X_train_m, y_train_m, sample_weight=sw_m)
    main_acc = float(clf_main.score(X_val_m, y_val_m))
    main_model_path = os.path.join(model_dir, "el_gordo_main_gb.joblib")
    joblib.dump({"model": clf_main, "features": main_features}, main_model_path)

    X_train_c, X_val_c, y_train_c, y_val_c = time_split(df_clave, clave_features)
    sw_c = compute_sample_weight("balanced", y_train_c)
    clf_clave = GradientBoostingClassifier(
        n_estimators=100,
        max_depth=5,
        learning_rate=0.08,
        random_state=42,
    )
    clf_clave.fit(X_train_c, y_train_c, sample_weight=sw_c)
    clave_acc = float(clf_clave.score(X_val_c, y_val_c))
    clave_model_path = os.path.join(model_dir, "el_gordo_clave_gb.joblib")
    joblib.dump({"model": clf_clave, "features": clave_features}, clave_model_path)

    results.update(
        {
            "main_accuracy": main_acc,
            "clave_accuracy": clave_acc,
            "main_model_path": main_model_path,
            "clave_model_path": clave_model_path,
        }
    )
    return results


def compute_el_gordo_probabilities(
    cutoff_draw_id: str | None = None,
) -> Dict[str, object]:
    """
    Compute per-number probabilities for the *next* El Gordo draw
    (mains 1..54, clave 0..9) using the trained Gradient Boosting models.

    cutoff_draw_id:
        - If provided, use the feature row with this id_sorteo as the cutoff draw.
        - If None, use the latest row in `el_gordo_feature`.
    """
    client = _get_mongo_client()
    db = client[MONGO_DB]
    coll = db[FEATURE_COLLECTION]

    if cutoff_draw_id:
        doc = coll.find_one({"id_sorteo": str(cutoff_draw_id).strip()})
        if not doc:
            client.close()
            raise RuntimeError(f"cutoff_draw_id {cutoff_draw_id!r} not found in el_gordo_feature")
    else:
        doc = coll.find_one(sort=[("fecha_sorteo", -1)])
        if not doc:
            client.close()
            raise RuntimeError("No rows found in el_gordo_feature")

    draw_id = str(doc.get("id_sorteo") or "").strip()
    fecha = str(doc.get("fecha_sorteo") or "").strip()
    dia = str(doc.get("dia_semana") or "").strip()
    weekday_idx = _weekday_to_index(dia)

    main_dx = list(doc.get("main_dx") or [])
    clave_dx = list(doc.get("clave_dx") or [])
    frequency = list(doc.get("frequency") or [])
    gap = list(doc.get("gap") or [])
    cur_mains = [int(x) for x in (doc.get("main_number") or []) if isinstance(x, int)]
    cur_clave_raw = doc.get("clave")
    cur_clave = int(cur_clave_raw) if isinstance(cur_clave_raw, int) else None
    source_index = int(doc.get("source_index", 0))
    total_draws = source_index + 1
    draw_sum_mains = sum(cur_mains) if cur_mains else 0
    draw_even_mains = sum(1 for x in cur_mains if x % 2 == 0)
    draw_clave = cur_clave if cur_clave is not None else -1

    # Build feature rows for mains (same columns as training)
    main_rows = []
    for n in range(MAIN_MIN, MAIN_MAX + 1):
        idx_main = n - MAIN_MIN
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
        main_rows.append(
            {
                "weekday_idx": weekday_idx,
                "number": n,
                "freq": int(freq_val),
                "gap": gap_val,
                "freq_norm": freq_norm,
                "gap_cap": gap_cap,
                "draw_sum_mains": draw_sum_mains,
                "draw_even_mains": draw_even_mains,
                "draw_clave": draw_clave,
                "is_current_main": is_current_main,
            }
        )

    # Build feature rows for clave 0..9
    clave_rows = []
    clave_offset = MAIN_MAX - MAIN_MIN + 1  # 54 mains first
    for c in range(CLAVE_MIN, CLAVE_MAX + 1):
        idx_clave = c - CLAVE_MIN
        freq_idx = clave_offset + idx_clave
        gap_idx = clave_offset + idx_clave
        freq_val = frequency[freq_idx] if freq_idx < len(frequency) else 0
        gap_raw = gap[gap_idx] if gap_idx < len(gap) else None
        gap_val = -1 if gap_raw is None else int(gap_raw)
        freq_norm = (int(freq_val) / total_draws) if total_draws > 0 else 0.0
        gap_cap = min(gap_val, 100) if gap_val >= 0 else -1
        is_current_clave = 0
        if idx_clave < len(clave_dx):
            try:
                is_current_clave = 1 if int(clave_dx[idx_clave]) != 0 else 0
            except Exception:
                pass
        clave_rows.append(
            {
                "weekday_idx": weekday_idx,
                "number": c,
                "freq": int(freq_val),
                "gap": gap_val,
                "freq_norm": freq_norm,
                "gap_cap": gap_cap,
                "draw_sum_mains": draw_sum_mains,
                "draw_even_mains": draw_even_mains,
                "draw_clave": draw_clave,
                "is_current_clave": is_current_clave,
            }
        )

    client.close()

    df_main = pd.DataFrame(main_rows)
    df_clave = pd.DataFrame(clave_rows)

    model_dir = MODEL_DIR_DEFAULT
    main_model_path = os.path.join(model_dir, "el_gordo_main_gb.joblib")
    clave_model_path = os.path.join(model_dir, "el_gordo_clave_gb.joblib")

    if not os.path.exists(main_model_path) or not os.path.exists(clave_model_path):
        raise RuntimeError("El Gordo ML models not found. Train them first.")

    saved_main = joblib.load(main_model_path)
    main_model: GradientBoostingClassifier = saved_main["model"]
    main_features: List[str] = saved_main["features"]
    X_main = df_main[main_features].values
    main_probs = main_model.predict_proba(X_main)[:, 1]

    saved_clave = joblib.load(clave_model_path)
    clave_model: GradientBoostingClassifier = saved_clave["model"]
    clave_features: List[str] = saved_clave["features"]
    X_clave = df_clave[clave_features].values
    clave_probs = clave_model.predict_proba(X_clave)[:, 1]

    mains = [
        {"number": int(n), "p": float(p)}
        for n, p in zip(df_main["number"].tolist(), main_probs.tolist())
    ]
    claves = [
        {"number": int(n), "p": float(p)}
        for n, p in zip(df_clave["number"].tolist(), clave_probs.tolist())
    ]
    mains.sort(key=lambda x: x["p"], reverse=True)
    claves.sort(key=lambda x: x["p"], reverse=True)

    return {
        "cutoff_draw_id": cutoff_draw_id or draw_id,
        "draw_id": draw_id,
        "fecha_sorteo": fecha,
        "mains": mains,
        "claves": claves,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cutoff_draw_id",
        type=str,
        default="",
        help="Optional id_sorteo; only draws up to this one (inclusive) are used.",
    )
    args = parser.parse_args()
    cutoff = args.cutoff_draw_id or None
    info = prepare_el_gordo_dataset(cutoff_draw_id=cutoff, out_dir=None)
    print("El Gordo dataset prepared:")
    print("  out_dir:", info["out_dir"])
    print("  main_rows:", info["main_rows"])
    print("  clave_rows:", info["clave_rows"])

