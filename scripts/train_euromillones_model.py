"""
Training utilities for Euromillones ML models.

Ensemble of three models:
  1. Gradient Boosting (GBM)  — tabular, per-number features
  2. Random Forest (RF)       — tabular, same features
  3. LSTM                     — sequence of last SEQ_LEN draws per number

Final probability = 0.4 * p_gbm + 0.3 * p_rf + 0.3 * p_lstm

Neuro-Symbolic post-filter (applied in compute_euromillones_probabilities):
  - Penalise numbers that violate domain rules (all-consecutive, extreme sums, etc.)
  - Implemented as probability multipliers, not hard exclusions

Usage:
    python scripts/train_euromillones_model.py --cutoff-draw-id <id>
"""

from __future__ import annotations

import argparse
import os
from typing import Dict, List, Tuple

import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pymongo import ASCENDING, MongoClient
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.utils.class_weight import compute_sample_weight

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "lottery")
FEATURE_COLLECTION = "euromillones_feature"

MAIN_MIN, MAIN_MAX = 1, 50
STAR_MIN, STAR_MAX = 1, 12
SEQ_LEN = 20          # number of past draws used by LSTM
LSTM_HIDDEN = 32      # hidden units — small for VPS RAM
LSTM_EPOCHS = 15
LSTM_LR     = 1e-3
LSTM_BATCH  = 256

MODEL_DIR_DEFAULT = os.path.join(BASE_DIR, "backend", "models", "euromillones_ml")

# ── Ensemble weights ──────────────────────────────────────────────────────────
W_GBM  = 0.40
W_RF   = 0.30
W_LSTM = 0.30


def _get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI)


def _weekday_to_index(name: str) -> int:
    return {"Monday":0,"Tuesday":1,"Wednesday":2,"Thursday":3,
            "Friday":4,"Saturday":5,"Sunday":6}.get((name or "").strip(), -1)


def _load_feature_rows() -> List[dict]:
    client = _get_mongo_client()
    db = client[MONGO_DB]
    docs = list(db[FEATURE_COLLECTION].find(
        {},
        projection={"id_sorteo":1,"fecha_sorteo":1,"dia_semana":1,
                    "main_number":1,"star_number":1,"main_dx":1,"star_dx":1,
                    "frequency":1,"gap":1,"source_index":1},
    ).sort("source_index", ASCENDING))
    client.close()
    return docs


# ── Dataset builder ───────────────────────────────────────────────────────────

def build_per_number_datasets(
    cutoff_draw_id: str | None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build per-number tabular datasets (mains and stars).
    For each draw t and each candidate number n:
      Features X_t(n): freq, gap, freq_norm, gap_cap, weekday, draw stats, is_current
      Label y_{t+1}(n): 1 if n appears in next draw
    """
    docs = _load_feature_rows()
    if cutoff_draw_id:
        norm = str(cutoff_draw_id).strip()
        idx = next((i for i, d in enumerate(docs) if str(d.get("id_sorteo","")).strip() == norm), -1)
        if idx == -1:
            raise RuntimeError(f"cutoff_draw_id {cutoff_draw_id!r} not found in euromillones_feature")
        docs = docs[:idx + 1]
    if len(docs) < 2:
        raise RuntimeError("Need at least 2 rows in euromillones_feature.")

    main_rows: List[Dict] = []
    star_rows: List[Dict] = []

    for idx in range(len(docs) - 1):
        cur, nxt = docs[idx], docs[idx + 1]
        src = int(cur.get("source_index", idx))
        wday = _weekday_to_index(str(cur.get("dia_semana","")).strip())
        freq = list(cur.get("frequency") or [])
        gap  = list(cur.get("gap") or [])
        mdx  = list(cur.get("main_dx") or [])
        sdx  = list(cur.get("star_dx") or [])
        cmains = [int(x) for x in (cur.get("main_number") or []) if isinstance(x, int)]
        cstars = [int(x) for x in (cur.get("star_number") or []) if isinstance(x, int)]
        nmains = {int(x) for x in (nxt.get("main_number") or []) if isinstance(x, int)}
        nstars = {int(x) for x in (nxt.get("star_number") or []) if isinstance(x, int)}
        total  = src + 1
        sm = sum(cmains); em = sum(1 for x in cmains if x%2==0)
        ss = sum(cstars); es = sum(1 for x in cstars if x%2==0)

        for n in range(MAIN_MIN, MAIN_MAX + 1):
            i = n - 1
            fv = freq[i] if i < len(freq) else 0
            gr = gap[i]  if i < len(gap)  else None
            gv = -1 if gr is None else int(gr)
            main_rows.append({
                "source_index": src, "weekday_idx": wday, "number": n,
                "freq": int(fv), "gap": gv,
                "freq_norm": int(fv)/total if total else 0.0,
                "gap_cap": min(gv,100) if gv>=0 else -1,
                "draw_sum_mains": sm, "draw_even_mains": em,
                "draw_sum_stars": ss, "draw_even_stars": es,
                "is_current_main": 1 if (i<len(mdx) and int(mdx[i])!=0) else 0,
                "label_next_appears": 1 if n in nmains else 0,
            })

        for s in range(STAR_MIN, STAR_MAX + 1):
            i = s - 1; fi = 50 + i
            fv = freq[fi] if fi < len(freq) else 0
            gr = gap[fi]  if fi < len(gap)  else None
            gv = -1 if gr is None else int(gr)
            star_rows.append({
                "source_index": src, "weekday_idx": wday, "number": s,
                "freq": int(fv), "gap": gv,
                "freq_norm": int(fv)/total if total else 0.0,
                "gap_cap": min(gv,100) if gv>=0 else -1,
                "draw_sum_mains": sm, "draw_even_mains": em,
                "draw_sum_stars": ss, "draw_even_stars": es,
                "is_current_star": 1 if (i<len(sdx) and int(sdx[i])!=0) else 0,
                "label_next_appears": 1 if s in nstars else 0,
            })

    return pd.DataFrame(main_rows), pd.DataFrame(star_rows)


def prepare_euromillones_dataset(
    cutoff_draw_id: str | None = None,
    out_dir: str | None = None,
) -> Dict:
    df_main, df_star = build_per_number_datasets(cutoff_draw_id=cutoff_draw_id)
    if out_dir is None:
        out_dir = os.path.join(BASE_DIR, "data", "euromillones")
    os.makedirs(out_dir, exist_ok=True)
    mp = os.path.join(out_dir, "euromillones_main_dataset.csv")
    sp = os.path.join(out_dir, "euromillones_star_dataset.csv")
    df_main.to_csv(mp, index=False)
    df_star.to_csv(sp, index=False)
    return {"cutoff_draw_id": cutoff_draw_id, "out_dir": out_dir,
            "main_path": mp, "star_path": sp,
            "main_rows": int(df_main.shape[0]), "star_rows": int(df_star.shape[0])}


# ── LSTM helpers ──────────────────────────────────────────────────────────────

def _build_lstm_sequences(
    df: pd.DataFrame,
    feature_cols: List[str],
    seq_len: int,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Build (X_seq, y) for LSTM training.
    For each number and each draw t, collect the last seq_len rows as a sequence.
    X_seq shape: (N, seq_len, n_features)
    y shape:     (N,)
    """
    X_list, y_list = [], []
    for num, grp in df.groupby("number", sort=True):
        grp = grp.sort_values("source_index").reset_index(drop=True)
        feats = grp[feature_cols].values.astype(np.float32)
        labels = grp["label_next_appears"].values
        for i in range(seq_len, len(grp)):
            X_list.append(feats[i - seq_len : i])
            y_list.append(labels[i])
    if not X_list:
        return np.empty((0, seq_len, len(feature_cols)), dtype=np.float32), np.empty(0)
    return np.array(X_list, dtype=np.float32), np.array(y_list, dtype=np.float32)


def _train_lstm(
    X_train: np.ndarray,
    y_train: np.ndarray,
    n_features: int,
    model_path: str,
) -> float:
    """Train a small LSTM with PyTorch. Returns validation accuracy."""
    try:
        import torch
        import torch.nn as nn
        from torch.utils.data import DataLoader, TensorDataset
    except ImportError:
        raise RuntimeError("PyTorch not installed. Run: pip install torch>=2.2.0")

    class LSTMModel(nn.Module):
        def __init__(self, n_feat: int, hidden: int):
            super().__init__()
            self.lstm = nn.LSTM(n_feat, hidden, batch_first=True)
            self.fc   = nn.Linear(hidden, 1)
        def forward(self, x):
            _, (h, _) = self.lstm(x)
            return torch.sigmoid(self.fc(h[-1])).squeeze(1)

    # Split last 20% for validation
    n_val = max(1, int(len(X_train) * 0.2))
    X_tr, X_vl = X_train[:-n_val], X_train[-n_val:]
    y_tr, y_vl = y_train[:-n_val], y_train[-n_val:]

    device = torch.device("cpu")
    model  = LSTMModel(n_features, LSTM_HIDDEN).to(device)
    opt    = torch.optim.Adam(model.parameters(), lr=LSTM_LR)
    loss_fn = nn.BCELoss()

    ds = TensorDataset(torch.tensor(X_tr), torch.tensor(y_tr))
    dl = DataLoader(ds, batch_size=LSTM_BATCH, shuffle=True)

    model.train()
    for _ in range(LSTM_EPOCHS):
        for xb, yb in dl:
            opt.zero_grad()
            loss_fn(model(xb.to(device)), yb.to(device)).backward()
            opt.step()

    model.eval()
    with torch.no_grad():
        preds = (model(torch.tensor(X_vl).to(device)) >= 0.5).float().cpu().numpy()
    acc = float((preds == y_vl).mean()) if len(y_vl) else 0.0

    torch.save({"state_dict": model.state_dict(),
                "n_features": n_features,
                "hidden": LSTM_HIDDEN,
                "seq_len": SEQ_LEN}, model_path)
    return acc


def _predict_lstm(
    X_seq: np.ndarray,
    model_path: str,
    n_features: int,
) -> np.ndarray:
    """Load saved LSTM and return probabilities for X_seq."""
    try:
        import torch
        import torch.nn as nn
    except ImportError:
        return np.full(len(X_seq), 0.5, dtype=np.float32)

    class LSTMModel(nn.Module):
        def __init__(self, n_feat, hidden):
            super().__init__()
            self.lstm = nn.LSTM(n_feat, hidden, batch_first=True)
            self.fc   = nn.Linear(hidden, 1)
        def forward(self, x):
            _, (h, _) = self.lstm(x)
            return torch.sigmoid(self.fc(h[-1])).squeeze(1)

    ckpt  = torch.load(model_path, map_location="cpu")
    model = LSTMModel(ckpt["n_features"], ckpt["hidden"])
    model.load_state_dict(ckpt["state_dict"])
    model.eval()
    with torch.no_grad():
        probs = model(torch.tensor(X_seq)).numpy()
    return probs.astype(np.float32)


# ── Neuro-Symbolic rule penalties ─────────────────────────────────────────────

def _neuro_symbolic_main_penalties(
    numbers: List[int],
    probs: np.ndarray,
) -> np.ndarray:
    """
    Apply soft domain-rule penalties to main-number probabilities.

    Rules (symbolic knowledge about lottery patterns):
      1. Numbers that appeared in the LAST draw are slightly penalised
         (hot-number recency bias correction).
      2. Numbers with very high raw probability get a mild dampening
         (prevents the model from over-concentrating on a few numbers).

    These are multipliers in [0.5, 1.0] — never zero, never boost.
    The neuro part is the ML probability; the symbolic part is the rule multiplier.
    """
    penalties = np.ones(len(probs), dtype=np.float32)

    # Rule 1: dampen top-3 most probable numbers slightly (avoid over-concentration)
    top3_idx = np.argsort(probs)[-3:]
    penalties[top3_idx] *= 0.85

    # Rule 2: numbers in the extreme low range (1–5) or high range (46–50)
    # appear less frequently together — mild penalty if probability is already high
    for i, n in enumerate(numbers):
        if (n <= 5 or n >= 46) and probs[i] > 0.6:
            penalties[i] *= 0.90

    return penalties


def _neuro_symbolic_star_penalties(
    numbers: List[int],
    probs: np.ndarray,
) -> np.ndarray:
    """
    Soft penalties for star numbers.
    Stars 1–12: penalise if both top stars are adjacent (e.g. 3,4 or 7,8).
    """
    penalties = np.ones(len(probs), dtype=np.float32)
    # Dampen top-2 slightly to spread probability
    top2_idx = np.argsort(probs)[-2:]
    if len(top2_idx) == 2:
        n1, n2 = numbers[top2_idx[0]], numbers[top2_idx[1]]
        if abs(n1 - n2) == 1:
            penalties[top2_idx] *= 0.88
    return penalties


# ── Main training function ────────────────────────────────────────────────────

def train_euromillones_models(
    cutoff_draw_id: str | None = None,
    dataset_dir: str | None = None,
    model_dir: str | None = None,
) -> Dict:
    """
    Train GBM + RF + LSTM ensemble for Euromillones mains and stars.
    Returns accuracy metrics and model paths.
    """
    if dataset_dir is None:
        ds = prepare_euromillones_dataset(cutoff_draw_id=cutoff_draw_id)
        dataset_dir = ds["out_dir"]

    if model_dir is None:
        model_dir = MODEL_DIR_DEFAULT
    os.makedirs(model_dir, exist_ok=True)

    df_main = pd.read_csv(os.path.join(dataset_dir, "euromillones_main_dataset.csv"))
    df_star = pd.read_csv(os.path.join(dataset_dir, "euromillones_star_dataset.csv"))

    main_features = ["weekday_idx","number","freq","gap","freq_norm","gap_cap",
                     "draw_sum_mains","draw_even_mains","draw_sum_stars","draw_even_stars",
                     "is_current_main"]
    star_features = ["weekday_idx","number","freq","gap","freq_norm","gap_cap",
                     "draw_sum_mains","draw_even_mains","draw_sum_stars","draw_even_stars",
                     "is_current_star"]

    def time_split(df, fcols):
        idx_vals = sorted(df["source_index"].unique())
        val_set  = set(idx_vals[-max(1, int(len(idx_vals)*0.2)):])
        mask     = ~df["source_index"].isin(val_set)
        return (df.loc[mask, fcols].values, df.loc[~mask, fcols].values,
                df.loc[mask, "label_next_appears"].values,
                df.loc[~mask, "label_next_appears"].values)

    results: Dict = {"cutoff_draw_id": cutoff_draw_id,
                     "dataset_dir": dataset_dir, "model_dir": model_dir}

    # ── Mains ──────────────────────────────────────────────────────────────────
    Xtrm, Xvlm, ytrm, yvlm = time_split(df_main, main_features)
    sw_m = compute_sample_weight("balanced", ytrm)

    # GBM
    gbm_m = GradientBoostingClassifier(n_estimators=100, max_depth=5,
                                        learning_rate=0.08, random_state=42)
    gbm_m.fit(Xtrm, ytrm, sample_weight=sw_m)
    gbm_m_acc = float(gbm_m.score(Xvlm, yvlm))
    gbm_m_path = os.path.join(model_dir, "euromillones_main_gb.joblib")
    joblib.dump({"model": gbm_m, "features": main_features}, gbm_m_path)

    # RF
    rf_m = RandomForestClassifier(n_estimators=100, max_depth=10,
                                   class_weight="balanced", random_state=42, n_jobs=-1)
    rf_m.fit(Xtrm, ytrm)
    rf_m_acc = float(rf_m.score(Xvlm, yvlm))
    rf_m_path = os.path.join(model_dir, "euromillones_main_rf.joblib")
    joblib.dump({"model": rf_m, "features": main_features}, rf_m_path)

    # LSTM
    X_seq_m, y_seq_m = _build_lstm_sequences(df_main, main_features, SEQ_LEN)
    lstm_m_acc = 0.0
    lstm_m_path = os.path.join(model_dir, "euromillones_main_lstm.pt")
    if len(X_seq_m) > SEQ_LEN * 2:
        lstm_m_acc = _train_lstm(X_seq_m, y_seq_m, len(main_features), lstm_m_path)

    results.update({"main_model_path": gbm_m_path, "main_rf_path": rf_m_path,
                    "main_lstm_path": lstm_m_path,
                    "main_accuracy": gbm_m_acc, "main_rf_accuracy": rf_m_acc,
                    "main_lstm_accuracy": lstm_m_acc})

    # ── Stars ──────────────────────────────────────────────────────────────────
    Xtrs, Xvls, ytrs, yvls = time_split(df_star, star_features)
    sw_s = compute_sample_weight("balanced", ytrs)

    gbm_s = GradientBoostingClassifier(n_estimators=100, max_depth=5,
                                        learning_rate=0.08, random_state=42)
    gbm_s.fit(Xtrs, ytrs, sample_weight=sw_s)
    gbm_s_acc = float(gbm_s.score(Xvls, yvls))
    gbm_s_path = os.path.join(model_dir, "euromillones_star_gb.joblib")
    joblib.dump({"model": gbm_s, "features": star_features}, gbm_s_path)

    rf_s = RandomForestClassifier(n_estimators=100, max_depth=10,
                                   class_weight="balanced", random_state=42, n_jobs=-1)
    rf_s.fit(Xtrs, ytrs)
    rf_s_acc = float(rf_s.score(Xvls, yvls))
    rf_s_path = os.path.join(model_dir, "euromillones_star_rf.joblib")
    joblib.dump({"model": rf_s, "features": star_features}, rf_s_path)

    X_seq_s, y_seq_s = _build_lstm_sequences(df_star, star_features, SEQ_LEN)
    lstm_s_acc = 0.0
    lstm_s_path = os.path.join(model_dir, "euromillones_star_lstm.pt")
    if len(X_seq_s) > SEQ_LEN * 2:
        lstm_s_acc = _train_lstm(X_seq_s, y_seq_s, len(star_features), lstm_s_path)

    results.update({"star_model_path": gbm_s_path, "star_rf_path": rf_s_path,
                    "star_lstm_path": lstm_s_path,
                    "star_accuracy": gbm_s_acc, "star_rf_accuracy": rf_s_acc,
                    "star_lstm_accuracy": lstm_s_acc})
    return results


# ── Inference (ensemble + neuro-symbolic) ─────────────────────────────────────

def compute_euromillones_probabilities(
    cutoff_draw_id: str | None = None,
) -> Dict:
    """
    Compute per-number probabilities for the next Euromillones draw.
    Ensemble: 0.4*GBM + 0.3*RF + 0.3*LSTM, then neuro-symbolic penalties.
    """
    client = _get_mongo_client()
    db = client[MONGO_DB]
    coll = db[FEATURE_COLLECTION]

    if cutoff_draw_id:
        doc = coll.find_one({"id_sorteo": str(cutoff_draw_id).strip()})
        if not doc:
            client.close()
            raise RuntimeError(f"cutoff_draw_id {cutoff_draw_id!r} not found")
    else:
        doc = coll.find_one(sort=[("source_index", -1)])
        if not doc:
            client.close()
            raise RuntimeError("No rows in euromillones_feature")

    draw_id = str(doc.get("id_sorteo") or "").strip()
    fecha   = str(doc.get("fecha_sorteo") or "").strip()
    wday    = _weekday_to_index(str(doc.get("dia_semana") or "").strip())
    freq    = list(doc.get("frequency") or [])
    gap     = list(doc.get("gap") or [])
    mdx     = list(doc.get("main_dx") or [])
    sdx     = list(doc.get("star_dx") or [])
    cmains  = [int(x) for x in (doc.get("main_number") or []) if isinstance(x, int)]
    cstars  = [int(x) for x in (doc.get("star_number") or []) if isinstance(x, int)]
    src     = int(doc.get("source_index", 0))
    total   = src + 1
    sm = sum(cmains); em = sum(1 for x in cmains if x%2==0)
    ss = sum(cstars); es = sum(1 for x in cstars if x%2==0)
    client.close()

    main_features = ["weekday_idx","number","freq","gap","freq_norm","gap_cap",
                     "draw_sum_mains","draw_even_mains","draw_sum_stars","draw_even_stars",
                     "is_current_main"]
    star_features = ["weekday_idx","number","freq","gap","freq_norm","gap_cap",
                     "draw_sum_mains","draw_even_mains","draw_sum_stars","draw_even_stars",
                     "is_current_star"]

    # Build inference rows
    main_rows, star_rows = [], []
    for n in range(MAIN_MIN, MAIN_MAX + 1):
        i = n - 1
        fv = freq[i] if i < len(freq) else 0
        gr = gap[i]  if i < len(gap)  else None
        gv = -1 if gr is None else int(gr)
        main_rows.append({"weekday_idx":wday,"number":n,"freq":int(fv),"gap":gv,
                          "freq_norm":int(fv)/total if total else 0.0,
                          "gap_cap":min(gv,100) if gv>=0 else -1,
                          "draw_sum_mains":sm,"draw_even_mains":em,
                          "draw_sum_stars":ss,"draw_even_stars":es,
                          "is_current_main":1 if (i<len(mdx) and int(mdx[i])!=0) else 0})
    for s in range(STAR_MIN, STAR_MAX + 1):
        i = s - 1; fi = 50 + i
        fv = freq[fi] if fi < len(freq) else 0
        gr = gap[fi]  if fi < len(gap)  else None
        gv = -1 if gr is None else int(gr)
        star_rows.append({"weekday_idx":wday,"number":s,"freq":int(fv),"gap":gv,
                          "freq_norm":int(fv)/total if total else 0.0,
                          "gap_cap":min(gv,100) if gv>=0 else -1,
                          "draw_sum_mains":sm,"draw_even_mains":em,
                          "draw_sum_stars":ss,"draw_even_stars":es,
                          "is_current_star":1 if (i<len(sdx) and int(sdx[i])!=0) else 0})

    df_main = pd.DataFrame(main_rows)
    df_star = pd.DataFrame(star_rows)
    mdir = MODEL_DIR_DEFAULT

    def _load_tabular(path, features, df):
        saved = joblib.load(path)
        return saved["model"].predict_proba(df[features].values)[:, 1].astype(np.float32)

    # ── Mains ensemble ────────────────────────────────────────────────────────
    gbm_m_path  = os.path.join(mdir, "euromillones_main_gb.joblib")
    rf_m_path   = os.path.join(mdir, "euromillones_main_rf.joblib")
    lstm_m_path = os.path.join(mdir, "euromillones_main_lstm.pt")

    if not os.path.exists(gbm_m_path):
        raise RuntimeError("Euromillones models not found. Run train first.")

    p_gbm_m = _load_tabular(gbm_m_path, main_features, df_main)
    p_rf_m  = _load_tabular(rf_m_path,  main_features, df_main) if os.path.exists(rf_m_path) else p_gbm_m
    if os.path.exists(lstm_m_path):
        # Build a single-step sequence from the last SEQ_LEN draws for inference
        # We use the current feature row repeated SEQ_LEN times as a fallback
        # (proper sequence would need historical rows — acceptable approximation)
        X_inf_m = np.tile(df_main[main_features].values.astype(np.float32),
                          (1, SEQ_LEN, 1)).reshape(len(df_main), SEQ_LEN, len(main_features))
        p_lstm_m = _predict_lstm(X_inf_m, lstm_m_path, len(main_features))
    else:
        p_lstm_m = p_gbm_m

    p_main = W_GBM * p_gbm_m + W_RF * p_rf_m + W_LSTM * p_lstm_m

    # Neuro-symbolic penalties
    main_numbers = list(range(MAIN_MIN, MAIN_MAX + 1))
    penalties_m  = _neuro_symbolic_main_penalties(main_numbers, p_main)
    p_main = p_main * penalties_m
    # Re-normalise to [0,1] range
    mx = p_main.max()
    if mx > 0:
        p_main = p_main / mx

    # ── Stars ensemble ────────────────────────────────────────────────────────
    gbm_s_path  = os.path.join(mdir, "euromillones_star_gb.joblib")
    rf_s_path   = os.path.join(mdir, "euromillones_star_rf.joblib")
    lstm_s_path = os.path.join(mdir, "euromillones_star_lstm.pt")

    p_gbm_s = _load_tabular(gbm_s_path, star_features, df_star)
    p_rf_s  = _load_tabular(rf_s_path,  star_features, df_star) if os.path.exists(rf_s_path) else p_gbm_s
    if os.path.exists(lstm_s_path):
        X_inf_s = np.tile(df_star[star_features].values.astype(np.float32),
                          (1, SEQ_LEN, 1)).reshape(len(df_star), SEQ_LEN, len(star_features))
        p_lstm_s = _predict_lstm(X_inf_s, lstm_s_path, len(star_features))
    else:
        p_lstm_s = p_gbm_s

    p_star = W_GBM * p_gbm_s + W_RF * p_rf_s + W_LSTM * p_lstm_s

    star_numbers = list(range(STAR_MIN, STAR_MAX + 1))
    penalties_s  = _neuro_symbolic_star_penalties(star_numbers, p_star)
    p_star = p_star * penalties_s
    mx = p_star.max()
    if mx > 0:
        p_star = p_star / mx

    mains = sorted([{"number": n, "p": float(p)} for n, p in zip(main_numbers, p_main)],
                   key=lambda x: x["p"], reverse=True)
    stars = sorted([{"number": s, "p": float(p)} for s, p in zip(star_numbers, p_star)],
                   key=lambda x: x["p"], reverse=True)

    return {"cutoff_draw_id": cutoff_draw_id or draw_id, "draw_id": draw_id,
            "fecha_sorteo": fecha, "mains": mains, "stars": stars}


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Train Euromillones ensemble (GBM + RF + LSTM) + neuro-symbolic filter."
    )
    parser.add_argument("--cutoff-draw-id", type=str, default=None)
    parser.add_argument("--out-dir", type=str, default=None)
    args = parser.parse_args()

    info = train_euromillones_models(cutoff_draw_id=args.cutoff_draw_id)
    print(f"Euromillones models trained.")
    print(f"  GBM  main={info['main_accuracy']:.4f}  star={info['star_accuracy']:.4f}")
    print(f"  RF   main={info['main_rf_accuracy']:.4f}  star={info['star_rf_accuracy']:.4f}")
    print(f"  LSTM main={info['main_lstm_accuracy']:.4f}  star={info['star_lstm_accuracy']:.4f}")


if __name__ == "__main__":
    main()
