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

import pandas as pd  # type: ignore[import-untyped]
from dotenv import load_dotenv  # type: ignore[import-untyped]
from pymongo import MongoClient  # type: ignore[import-untyped]


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

load_dotenv()  # Load MONGO_URI / MONGO_DB if present

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "lottery")

FEATURE_COLLECTION = "el_gordo_feature"

MAIN_MIN, MAIN_MAX = 1, 54
CLAVE_MIN, CLAVE_MAX = 0, 9


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
    df_main, df_clave = build_per_number_datasets(cutoff_draw_id=cutoff)
    print("Main dataset shape:", df_main.shape)
    print("Clave dataset shape:", df_clave.shape)

