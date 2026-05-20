"""
Online Learning with Weight Updates after each draw result.

Architecture (as specified by client):
  BEFORE draw:
    1. Current model generates complete ranking (full wheel TXT)
    2. .orc file is generated alongside the .txt (binary model snapshot)
    3. Both files share the same SHA-256 hash stored in MongoDB

  AFTER draw result:
    1. Load .orc from the pre-draw snapshot
    2. Compare actual draw result with the predicted ranking
    3. Calculate error (distance between predicted position and actual jackpot position)
    4. Apply feedback loop: update model weights using the error signal
    5. Save updated model — knowledge of all previous draws is encoded in weights
    6. Cleanup: .orc can be archived; .txt can be deleted after new one is generated

  NEXT draw:
    Updated model generates new ranking. Cycle repeats.
    Model does NOT need to review old data — knowledge is in the weights.

Usage:
    # Before draw: generate .orc + hash
    python3 scripts/online_learning.py pre-draw --lottery euromillones --cutoff-draw-id 1234

    # After draw result: update model weights
    python3 scripts/online_learning.py post-draw --lottery euromillones --current-id 1235 --pre-id 1234

    # Via API: POST /api/online-learning/pre-draw
    #          POST /api/online-learning/post-draw
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import struct
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import joblib
import numpy as np

try:
    from dotenv import load_dotenv
    _scripts_dir = os.path.dirname(os.path.abspath(__file__))
    for _p in [os.path.join(_scripts_dir, "..", "backend", ".env"),
               os.path.join(_scripts_dir, "..", ".env")]:
        if os.path.isfile(_p):
            load_dotenv(_p)
            break
except ImportError:
    pass

from pymongo import MongoClient, ASCENDING

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "lottery")
BASE_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ── ORC collection ────────────────────────────────────────────────────────────
ORC_COLLECTION = "model_orc_snapshots"

# ── Lottery config ────────────────────────────────────────────────────────────
LOTTERY_CONFIG = {
    "euromillones": {
        "model_dir":    os.path.join(BASE_DIR, "backend", "models", "euromillones_ml"),
        "gbm_main":     "euromillones_main_gb.joblib",
        "gbm_sec":      "euromillones_star_gb.joblib",
        "rf_main":      "euromillones_main_rf.joblib",
        "rf_sec":       "euromillones_star_rf.joblib",
        "lstm_main":    "euromillones_main_lstm.pt",
        "lstm_sec":     "euromillones_star_lstm.pt",
        "orc_dir":      os.path.join(BASE_DIR, "euromillones_pools"),
        "progress_col": "euromillones_train_progress",
        "compare_col":  "euromillones_compare_results",
        "feature_col":  "euromillones_feature",
    },
    "el-gordo": {
        "model_dir":    os.path.join(BASE_DIR, "backend", "models", "el_gordo_ml"),
        "gbm_main":     "el_gordo_main_gb.joblib",
        "gbm_sec":      "el_gordo_clave_gb.joblib",
        "rf_main":      "el_gordo_main_rf.joblib",
        "rf_sec":       "el_gordo_clave_rf.joblib",
        "lstm_main":    "el_gordo_main_lstm.pt",
        "lstm_sec":     "el_gordo_clave_lstm.pt",
        "orc_dir":      os.path.join(BASE_DIR, "el_gordo_pools"),
        "progress_col": "el_gordo_train_progress",
        "compare_col":  "el_gordo_compare_results",
        "feature_col":  "el_gordo_feature",
    },
    "la-primitiva": {
        "model_dir":    os.path.join(BASE_DIR, "backend", "models", "la_primitiva_ml"),
        "gbm_main":     "la_primitiva_main_gb.pkl",
        "gbm_sec":      "la_primitiva_reintegro_gb.pkl",
        "rf_main":      "la_primitiva_main_rf.pkl",
        "rf_sec":       "la_primitiva_reintegro_rf.pkl",
        "lstm_main":    "la_primitiva_main_lstm.pt",
        "lstm_sec":     "la_primitiva_reintegro_lstm.pt",
        "orc_dir":      os.path.join(BASE_DIR, "la_primitiva_pools"),
        "progress_col": "la_primitiva_train_progress",
        "compare_col":  "la_primitiva_compare_results",
        "feature_col":  "la_primitiva_feature",
    },
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── SHA-256 helpers ───────────────────────────────────────────────────────────

def sha256_file(path: str) -> str:
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ── ORC file: binary model snapshot ──────────────────────────────────────────

def _collect_model_bytes(cfg: dict) -> bytes:
    """
    Read all model files for this lottery and concatenate their bytes.
    This forms the binary content of the .orc snapshot.
    """
    mdir = cfg["model_dir"]
    parts = []
    for key in ["gbm_main", "gbm_sec", "rf_main", "rf_sec", "lstm_main", "lstm_sec"]:
        fname = cfg.get(key, "")
        if not fname:
            continue
        path = os.path.join(mdir, fname)
        if os.path.isfile(path):
            with open(path, "rb") as f:
                data = f.read()
            # Prefix each file with its name length + name + data length + data
            name_bytes = fname.encode("utf-8")
            parts.append(struct.pack(">I", len(name_bytes)))
            parts.append(name_bytes)
            parts.append(struct.pack(">Q", len(data)))
            parts.append(data)
    return b"".join(parts)


def generate_orc(lottery: str, draw_id: str, txt_path: Optional[str] = None) -> dict:
    """
    Generate .orc file (binary model snapshot) for a draw.
    Also computes SHA-256 of both .orc and .txt (if provided).
    Stores metadata in MongoDB orc_snapshots collection.

    Returns:
        {orc_path, orc_hash, txt_hash (if txt provided), draw_id, lottery, created_at}
    """
    cfg = LOTTERY_CONFIG.get(lottery)
    if not cfg:
        raise ValueError(f"Unknown lottery: {lottery}")

    orc_dir = Path(cfg["orc_dir"])
    orc_dir.mkdir(parents=True, exist_ok=True)
    orc_path = str(orc_dir / f"{lottery.replace('-','_')}_{draw_id}.orc")

    # Collect model bytes and write .orc
    model_bytes = _collect_model_bytes(cfg)
    if not model_bytes:
        raise RuntimeError(f"No model files found for {lottery} in {cfg['model_dir']}")

    with open(orc_path, "wb") as f:
        f.write(model_bytes)

    orc_hash = sha256_file(orc_path)
    txt_hash = sha256_file(txt_path) if txt_path and os.path.isfile(txt_path) else None

    result = {
        "lottery":    lottery,
        "draw_id":    draw_id,
        "orc_path":   orc_path,
        "orc_hash":   orc_hash,
        "txt_path":   txt_path,
        "txt_hash":   txt_hash,
        "created_at": _now_iso(),
    }

    # Persist to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    db[ORC_COLLECTION].replace_one(
        {"lottery": lottery, "draw_id": draw_id},
        result,
        upsert=True,
    )
    client.close()

    print(f"[orc] Generated {orc_path}")
    print(f"[orc] ORC hash:  {orc_hash}")
    if txt_hash:
        print(f"[orc] TXT hash:  {txt_hash}")

    return result


def validate_hashes(lottery: str, draw_id: str) -> dict:
    """
    Validate that the .orc and .txt files on disk still match their stored hashes.
    Returns {valid: bool, orc_match: bool, txt_match: bool, details: str}
    """
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    doc = db[ORC_COLLECTION].find_one({"lottery": lottery, "draw_id": draw_id})
    client.close()

    if not doc:
        return {"valid": False, "orc_match": False, "txt_match": False,
                "details": f"No ORC snapshot found for {lottery} draw_id={draw_id}"}

    orc_path = doc.get("orc_path", "")
    txt_path = doc.get("txt_path", "")
    stored_orc_hash = doc.get("orc_hash", "")
    stored_txt_hash = doc.get("txt_hash", "")

    orc_match = False
    txt_match = True  # default True if no txt was stored

    if orc_path and os.path.isfile(orc_path):
        current_orc_hash = sha256_file(orc_path)
        orc_match = (current_orc_hash == stored_orc_hash)
    else:
        orc_match = False

    if stored_txt_hash and txt_path and os.path.isfile(txt_path):
        current_txt_hash = sha256_file(txt_path)
        txt_match = (current_txt_hash == stored_txt_hash)

    valid = orc_match and txt_match
    details = []
    if not orc_match:
        details.append("ORC hash mismatch — model file may have been modified")
    if not txt_match:
        details.append("TXT hash mismatch — full wheel file may have been modified")
    if valid:
        details.append("All hashes valid")

    return {
        "valid":     valid,
        "orc_match": orc_match,
        "txt_match": txt_match,
        "details":   "; ".join(details),
        "draw_id":   draw_id,
        "lottery":   lottery,
    }


# ── Online learning: feedback loop ───────────────────────────────────────────

def _load_orc_models(orc_path: str, cfg: dict) -> dict:
    """
    Restore model files from .orc binary snapshot back to the model directory.
    Returns dict of {filename: bytes} that were restored.
    """
    mdir = cfg["model_dir"]
    os.makedirs(mdir, exist_ok=True)
    restored = {}

    with open(orc_path, "rb") as f:
        while True:
            name_len_bytes = f.read(4)
            if not name_len_bytes or len(name_len_bytes) < 4:
                break
            name_len = struct.unpack(">I", name_len_bytes)[0]
            fname = f.read(name_len).decode("utf-8")
            data_len_bytes = f.read(8)
            if len(data_len_bytes) < 8:
                break
            data_len = struct.unpack(">Q", data_len_bytes)[0]
            data = f.read(data_len)
            out_path = os.path.join(mdir, fname)
            with open(out_path, "wb") as out:
                out.write(data)
            restored[fname] = len(data)

    return restored


def _build_feedback_features(
    number_range: range,
    target_set: set,
    features: List[str],
    error_rate: float,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Build a one-row-per-number feedback dataset.
    Label = 1 if number appeared in the draw, 0 otherwise.
    Sample weight = 3.0 for winning numbers (error correction signal).
    Feature vector is padded/truncated to match the model's expected feature count.
    """
    X_fb, y_fb, w_fb = [], [], []
    for n in number_range:
        label = 1 if n in target_set else 0
        row = [float(n), float(label), float(error_rate)]
        while len(row) < len(features):
            row.append(0.0)
        X_fb.append(row[:len(features)])
        y_fb.append(label)
        w_fb.append(3.0 if label == 1 else 1.0)
    return (
        np.array(X_fb, dtype=np.float32),
        np.array(y_fb, dtype=np.float32),
        np.array(w_fb, dtype=np.float32),
    )


def _update_gbm(model_path: str, target_set: set, number_range: range,
                error_rate: float) -> Optional[dict]:
    """
    Incremental GBM update: warm-start with 10 more estimators weighted toward
    the numbers that appeared in this draw. Knowledge accumulates in the tree
    ensemble without reviewing old data.
    """
    if not os.path.isfile(model_path):
        return None
    try:
        from sklearn.ensemble import GradientBoostingClassifier
        saved = joblib.load(model_path)
        model, features = saved["model"], saved["features"]
        X_fb, y_fb, w_fb = _build_feedback_features(number_range, target_set, features, error_rate)
        n_before = model.n_estimators_
        model.set_params(n_estimators=n_before + 10, warm_start=True)
        model.fit(X_fb, y_fb.astype(int), sample_weight=w_fb)
        joblib.dump({"model": model, "features": features}, model_path)
        print(f"[feedback] GBM {os.path.basename(model_path)}: {n_before} → {model.n_estimators_} estimators")
        return {"model": os.path.basename(model_path), "type": "gbm",
                "added_estimators": 10, "total_estimators": model.n_estimators_}
    except Exception as e:
        print(f"[feedback] WARNING: GBM update failed for {model_path}: {e}")
        return None


def _update_lstm(model_path: str, target_set: set, number_range: range,
                 error_rate: float) -> Optional[dict]:
    """
    Incremental LSTM update: one gradient step on the feedback signal.

    This is the core of the client's "neural network weights retain knowledge"
    requirement. The LSTM receives a single-step sequence (current draw features)
    and updates its weights toward the correct numbers using BCELoss.

    The learning rate is scaled by the error_rate so larger errors produce
    stronger corrections — the model learns more from bad predictions.

    Knowledge is encoded in the LSTM weights and persists across draws
    without needing to replay historical data.
    """
    if not os.path.isfile(model_path):
        return None
    try:
        import torch
        import torch.nn as nn
    except ImportError:
        print(f"[feedback] WARNING: PyTorch not available — LSTM update skipped for {model_path}")
        return None

    try:
        ckpt = torch.load(model_path, map_location="cpu")
        n_features = ckpt["n_features"]
        hidden     = ckpt["hidden"]
        seq_len    = ckpt.get("seq_len", 20)

        class LSTMModel(nn.Module):
            def __init__(self, nf: int, h: int):
                super().__init__()
                self.lstm = nn.LSTM(nf, h, batch_first=True)
                self.fc   = nn.Linear(h, 1)
            def forward(self, x):
                _, (h, _) = self.lstm(x)
                return torch.sigmoid(self.fc(h[-1])).squeeze(1)

        model = LSTMModel(n_features, hidden)
        model.load_state_dict(ckpt["state_dict"])
        model.train()

        # Build feedback tensors: one row per number, repeated seq_len times
        # to form a minimal sequence. This lets the LSTM process the signal
        # without needing the full historical sequence.
        X_rows, y_rows, w_rows = [], [], []
        for n in number_range:
            label = 1.0 if n in target_set else 0.0
            row = [float(n), label, error_rate] + [0.0] * max(0, n_features - 3)
            row = row[:n_features]
            # Repeat as a sequence of length seq_len
            X_rows.append([row] * seq_len)
            y_rows.append(label)
            w_rows.append(3.0 if label == 1.0 else 1.0)

        X_t = torch.tensor(X_rows, dtype=torch.float32)   # (N, seq_len, n_features)
        y_t = torch.tensor(y_rows, dtype=torch.float32)   # (N,)
        w_t = torch.tensor(w_rows, dtype=torch.float32)   # (N,)

        # Adaptive learning rate: scale by error_rate so bad predictions
        # trigger stronger corrections. Clamp to [1e-5, 5e-3].
        lr = float(np.clip(1e-3 * (1.0 + error_rate), 1e-5, 5e-3))
        optimizer = torch.optim.Adam(model.parameters(), lr=lr)
        loss_fn   = nn.BCELoss(reduction="none")

        optimizer.zero_grad()
        preds = model(X_t)                          # (N,)
        loss  = (loss_fn(preds, y_t) * w_t).mean()
        loss.backward()
        optimizer.step()

        # Save updated weights back to the same .pt file
        torch.save({
            "state_dict": model.state_dict(),
            "n_features": n_features,
            "hidden":     hidden,
            "seq_len":    seq_len,
        }, model_path)

        loss_val = float(loss.item())
        print(f"[feedback] LSTM {os.path.basename(model_path)}: "
              f"1 gradient step, lr={lr:.2e}, loss={loss_val:.4f}")
        return {"model": os.path.basename(model_path), "type": "lstm",
                "gradient_steps": 1, "lr": round(lr, 6), "loss": round(loss_val, 6)}
    except Exception as e:
        print(f"[feedback] WARNING: LSTM update failed for {model_path}: {e}")
        return None


def apply_feedback_loop(
    lottery: str,
    draw_id: str,
    pre_draw_id: str,
    actual_jackpot_position: int,
    actual_main_numbers: List[int],
    actual_secondary: "int | List[int]",
    *,
    draw_date: str | None = None,
) -> dict:
    """
    Online learning feedback loop — updates ALL three model types after each draw.

    Steps:
    1. Load the .orc snapshot from pre_draw_id (model state before the draw)
    2. Restore GBM, RF, and LSTM model files from .orc
    3. Compute error_rate = jackpot_position / total_tickets (0=perfect, 1=worst)
    4. Update GBM (main + secondary): warm-start +10 estimators weighted toward
       winning numbers — incremental tree learning without reviewing old data
    5. Update LSTM (main + secondary): one gradient step with adaptive lr scaled
       by error_rate — neural network weights retain knowledge across draws
       without needing to replay historical sequences
    6. RF is not updated incrementally (sklearn RF does not support warm_start
       for individual trees); it is retrained from scratch during the daily
       pipeline run which already uses all accumulated draws
    7. Save updated models, generate new .orc for next draw cycle
    8. Store feedback record in MongoDB model_feedback_log

    Returns: {lottery, draw_id, pre_draw_id, error_rate, updated_at, feedback_records}
    """
    cfg = LOTTERY_CONFIG.get(lottery)
    if not cfg:
        raise ValueError(f"Unknown lottery: {lottery}")

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    # 1. Find the .orc snapshot for pre_draw_id
    orc_doc = db[ORC_COLLECTION].find_one({"lottery": lottery, "draw_id": pre_draw_id})
    if not orc_doc:
        client.close()
        raise RuntimeError(f"No ORC snapshot found for {lottery} pre_draw_id={pre_draw_id}")

    orc_path = orc_doc.get("orc_path", "")
    if not os.path.isfile(orc_path):
        client.close()
        raise RuntimeError(f"ORC file not found on disk: {orc_path}")

    # 2. Restore all model files from .orc
    restored = _load_orc_models(orc_path, cfg)
    print(f"[feedback] Restored {len(restored)} model files from {orc_path}")

    # 3. Compute normalised error rate
    total_map = {
        "euromillones": 139_838_160,
        "el-gordo":     31_625_100,
        "la-primitiva": 139_838_160,
    }
    total      = total_map.get(lottery, 139_838_160)
    error_rate = actual_jackpot_position / total   # 0 = perfect, 1 = worst
    print(f"[feedback] jackpot_position={actual_jackpot_position:,} / {total:,} = error_rate={error_rate:.6f}")

    mdir = cfg["model_dir"]

    # Number ranges per lottery
    main_range = range(1, 55 if lottery == "el-gordo" else 51)
    sec_range  = range(0, 10) if lottery in ("el-gordo", "la-primitiva") else range(1, 13)

    main_set = set(int(x) for x in actual_main_numbers)
    sec_list = actual_secondary if isinstance(actual_secondary, list) else [actual_secondary]
    sec_set  = set(int(x) for x in sec_list)

    feedback_records: List[dict] = []

    # 4. Update GBM models (main + secondary)
    for model_key, target_set, num_range in [
        (cfg["gbm_main"], main_set, main_range),
        (cfg["gbm_sec"],  sec_set,  sec_range),
    ]:
        rec = _update_gbm(os.path.join(mdir, model_key), target_set, num_range, error_rate)
        if rec:
            feedback_records.append(rec)

    # 5. Update LSTM models (main + secondary) — neural network weight retention
    for model_key, target_set, num_range in [
        (cfg["lstm_main"], main_set, main_range),
        (cfg["lstm_sec"],  sec_set,  sec_range),
    ]:
        rec = _update_lstm(os.path.join(mdir, model_key), target_set, num_range, error_rate)
        if rec:
            feedback_records.append(rec)

    # 6. Generate new .orc snapshot for the next draw cycle
    new_orc = generate_orc(lottery, draw_id)

    # 7. Store feedback record
    draw_date_str = str(draw_date or "").strip()[:10] or None
    feedback_doc = {
        "lottery":                 lottery,
        "draw_id":                 draw_id,
        "pre_draw_id":             pre_draw_id,
        "draw_date":               draw_date_str,
        "actual_jackpot_position": actual_jackpot_position,
        "actual_main_numbers":     actual_main_numbers,
        "actual_secondary":        sec_list,
        "error_rate":              round(error_rate, 6),
        "feedback_records":        feedback_records,
        "new_orc_path":            new_orc.get("orc_path"),
        "new_orc_hash":            new_orc.get("orc_hash"),
        "updated_at":              _now_iso(),
    }
    db["model_feedback_log"].replace_one(
        {"lottery": lottery, "draw_id": draw_id},
        feedback_doc,
        upsert=True,
    )
    client.close()

    updated_types = list({r["type"] for r in feedback_records})
    print(f"[feedback] Done. error_rate={error_rate:.6f} "
          f"models_updated={updated_types} new_orc={new_orc.get('orc_path')}")
    return feedback_doc


# ── Pre-draw workflow ─────────────────────────────────────────────────────────

def pre_draw(lottery: str, cutoff_draw_id: str) -> dict:
    """
    Pre-draw step: generate .orc snapshot for the current model state.
    Call this AFTER the full wheel TXT has been generated.
    """
    cfg = LOTTERY_CONFIG.get(lottery)
    if not cfg:
        raise ValueError(f"Unknown lottery: {lottery}")

    # Find the TXT file path from train_progress
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    prog = db[cfg["progress_col"]].find_one({"cutoff_draw_id": cutoff_draw_id})
    client.close()

    txt_path = None
    if prog:
        txt_path = (prog.get("full_wheel_file_path") or "").strip() or None

    result = generate_orc(lottery, cutoff_draw_id, txt_path=txt_path)
    return result


# ── Post-draw workflow ────────────────────────────────────────────────────────

def post_draw(lottery: str, current_id: str, pre_id: str) -> dict:
    """
    Post-draw step: load compare result, apply feedback loop, update model weights.
    Call this AFTER the compare has been computed for (current_id, pre_id).
    """
    cfg = LOTTERY_CONFIG.get(lottery)
    if not cfg:
        raise ValueError(f"Unknown lottery: {lottery}")

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    compare = db[cfg["compare_col"]].find_one({"current_id": current_id, "pre_id": pre_id})
    client.close()

    if not compare:
        raise RuntimeError(f"No compare result found for {lottery} current_id={current_id} pre_id={pre_id}")

    jackpot_pos = compare.get("jackpot_position")
    if not jackpot_pos:
        raise RuntimeError(f"jackpot_position missing in compare result for {lottery} {current_id}")

    # Extract actual draw numbers from compare result categories
    # We use the categories to infer which numbers hit
    # For simplicity, we read from the draw collection directly
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    draw_col_map = {"euromillones": "euromillones", "el-gordo": "el_gordo", "la-primitiva": "la_primitiva"}
    draw_col = draw_col_map.get(lottery, lottery.replace("-", "_"))
    draw_doc = db[draw_col].find_one({"id_sorteo": current_id})
    if draw_doc is None and str(current_id).isdigit():
        draw_doc = db[draw_col].find_one({"id_sorteo": int(current_id)})
    client.close()

    if not draw_doc:
        raise RuntimeError(f"Draw document not found for {lottery} id_sorteo={current_id}")

    draw_date = str(compare.get("date") or draw_doc.get("fecha_sorteo") or "").strip()[:10] or None

    numbers = draw_doc.get("numbers") or []
    reintegro = draw_doc.get("reintegro")

    if lottery == "euromillones":
        main_numbers = [int(x) for x in numbers[:5]]
        secondary = [int(x) for x in numbers[5:7]] if len(numbers) >= 7 else []
    elif lottery == "el-gordo":
        main_numbers = [int(x) for x in numbers[:5]]
        secondary = int(reintegro) if reintegro is not None else 0
    else:  # la-primitiva
        main_numbers = [int(x) for x in numbers[:6]]
        secondary = int(reintegro) if reintegro is not None else 0

    result = apply_feedback_loop(
        lottery=lottery,
        draw_id=current_id,
        pre_draw_id=pre_id,
        actual_jackpot_position=jackpot_pos,
        actual_main_numbers=main_numbers,
        actual_secondary=secondary,
        draw_date=draw_date,
    )
    return result


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Online learning: pre-draw and post-draw weight updates")
    sub = parser.add_subparsers(dest="command")

    p1 = sub.add_parser("pre-draw", help="Generate .orc snapshot before draw")
    p1.add_argument("--lottery", required=True, choices=list(LOTTERY_CONFIG.keys()))
    p1.add_argument("--cutoff-draw-id", required=True)

    p2 = sub.add_parser("post-draw", help="Apply feedback loop after draw result")
    p2.add_argument("--lottery", required=True, choices=list(LOTTERY_CONFIG.keys()))
    p2.add_argument("--current-id", required=True)
    p2.add_argument("--pre-id", required=True)

    p3 = sub.add_parser("validate", help="Validate .orc and .txt hashes")
    p3.add_argument("--lottery", required=True, choices=list(LOTTERY_CONFIG.keys()))
    p3.add_argument("--draw-id", required=True)

    args = parser.parse_args()

    if args.command == "pre-draw":
        result = pre_draw(args.lottery, args.cutoff_draw_id)
        print(json.dumps(result, indent=2))
    elif args.command == "post-draw":
        result = post_draw(args.lottery, args.current_id, args.pre_id)
        print(json.dumps({k: v for k, v in result.items() if k != "feedback_records"}, indent=2))
    elif args.command == "validate":
        result = validate_hashes(args.lottery, args.draw_id)
        print(json.dumps(result, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
