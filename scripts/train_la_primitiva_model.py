"""
Training utilities for La Primitiva ML models.

Ensemble of three models:
  1. Gradient Boosting (GBM)
  2. Random Forest (RF)
  3. LSTM

Final probability = 0.4 * p_gbm + 0.3 * p_rf + 0.3 * p_lstm
Neuro-symbolic penalties applied after ensemble.

La Primitiva: 6 mains from 1–49, 1 reintegro from 0–9.
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
FEATURE_COLLECTION = "la_primitiva_feature"

MAIN_MIN, MAIN_MAX = 1, 49
REIN_MIN, REIN_MAX = 0, 9
SEQ_LEN     = 20
LSTM_HIDDEN = 32
LSTM_EPOCHS = 15
LSTM_LR     = 1e-3
LSTM_BATCH  = 256

MODEL_DIR_DEFAULT = os.path.join(BASE_DIR, "backend", "models", "la_primitiva_ml")

W_GBM  = 0.40
W_RF   = 0.30
W_LSTM = 0.30


def _get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI)


def _weekday_to_index(name: str) -> int:
    return {"Monday":0,"Tuesday":1,"Wednesday":2,"Thursday":3,
            "Friday":4,"Saturday":5,"Sunday":6}.get((name or "").strip(), -1)


def _default_output_dir() -> str:
    return os.path.join(BASE_DIR, "data", "la_primitiva")


def _load_feature_rows() -> List[dict]:
    client = _get_mongo_client()
    db = client[MONGO_DB]
    docs = list(db[FEATURE_COLLECTION].find(
        {},
        projection={"id_sorteo":1,"fecha_sorteo":1,"dia_semana":1,
                    "main_number":1,"complementario":1,"reintegro":1,
                    "main_dx":1,"complementario_dx":1,"reintegro_dx":1,
                    "frequency":1,"gap":1,"source_index":1},
    ).sort("source_index", ASCENDING))
    client.close()
    return docs


def build_per_number_datasets(
    cutoff_draw_id: str | None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    docs = _load_feature_rows()
    if cutoff_draw_id:
        norm = str(cutoff_draw_id).strip()
        idx = next((i for i, d in enumerate(docs) if str(d.get("id_sorteo","")).strip() == norm), -1)
        if idx == -1:
            raise RuntimeError(f"cutoff_draw_id {cutoff_draw_id!r} not found in la_primitiva_feature")
        docs = docs[:idx + 1]
    if len(docs) < 2:
        raise RuntimeError("Need at least 2 rows in la_primitiva_feature.")

    main_rows: List[Dict] = []
    rein_rows: List[Dict] = []

    num_mains = MAIN_MAX - MAIN_MIN + 1  # 49
    num_comp  = num_mains                # 49
    rein_offset = num_mains + num_comp   # 98

    for idx in range(len(docs) - 1):
        cur, nxt = docs[idx], docs[idx + 1]
        src  = int(cur.get("source_index", idx))
        wday = _weekday_to_index(str(cur.get("dia_semana","")).strip())
        freq = list(cur.get("frequency") or [])
        gap  = list(cur.get("gap") or [])
        mdx  = list(cur.get("main_dx") or [])
        rdx  = list(cur.get("reintegro_dx") or [])
        cmains = [int(x) for x in (cur.get("main_number") or []) if isinstance(x, int)]
        ccomp_raw = cur.get("complementario")
        ccomp = int(ccomp_raw) if isinstance(ccomp_raw, int) else -1
        crein_raw = cur.get("reintegro")
        crein = int(crein_raw) if isinstance(crein_raw, int) else -1
        nmains = {int(x) for x in (nxt.get("main_number") or []) if isinstance(x, int)}
        nrein_raw = nxt.get("reintegro")
        nrein = int(nrein_raw) if isinstance(nrein_raw, int) else None
        total  = src + 1
        sm = sum(cmains); em = sum(1 for x in cmains if x%2==0)

        for n in range(MAIN_MIN, MAIN_MAX + 1):
            i = n - MAIN_MIN
            fv = freq[i] if i < len(freq) else 0
            gr = gap[i]  if i < len(gap)  else None
            gv = -1 if gr is None else int(gr)
            main_rows.append({
                "source_index":src,"weekday_idx":wday,"number":n,
                "freq":int(fv),"gap":gv,
                "freq_norm":int(fv)/total if total else 0.0,
                "gap_cap":min(gv,100) if gv>=0 else -1,
                "draw_sum_mains":sm,"draw_even_mains":em,
                "draw_complementario":ccomp,"draw_reintegro":crein,
                "is_current_main":1 if (i<len(mdx) and int(mdx[i])!=0) else 0,
                "label_next_appears":1 if n in nmains else 0,
            })

        for r in range(REIN_MIN, REIN_MAX + 1):
            ir = r - REIN_MIN; fi = rein_offset + ir
            fv = freq[fi] if fi < len(freq) else 0
            gr = gap[fi]  if fi < len(gap)  else None
            gv = -1 if gr is None else int(gr)
            rein_rows.append({
                "source_index":src,"weekday_idx":wday,"number":r,
                "freq":int(fv),"gap":gv,
                "freq_norm":int(fv)/total if total else 0.0,
                "gap_cap":min(gv,100) if gv>=0 else -1,
                "draw_sum_mains":sm,"draw_even_mains":em,
                "draw_complementario":ccomp,"draw_reintegro":crein,
                "is_current_reintegro":1 if (ir<len(rdx) and int(rdx[ir])!=0) else 0,
                "label_next_appears":1 if (nrein is not None and r==nrein) else 0,
            })

    return pd.DataFrame(main_rows), pd.DataFrame(rein_rows)


def prepare_la_primitiva_dataset(
    cutoff_draw_id: str | None = None,
    out_dir: str | None = None,
) -> Dict:
    df_main, df_rein = build_per_number_datasets(cutoff_draw_id=cutoff_draw_id)
    if out_dir is None:
        out_dir = _default_output_dir()
    os.makedirs(out_dir, exist_ok=True)
    mp = os.path.join(out_dir, "la_primitiva_main_dataset.csv")
    rp = os.path.join(out_dir, "la_primitiva_reintegro_dataset.csv")
    df_main.to_csv(mp, index=False)
    df_rein.to_csv(rp, index=False)
    return {"cutoff_draw_id":cutoff_draw_id,"out_dir":out_dir,
            "main_path":mp,"reintegro_path":rp,
            "main_rows":int(df_main.shape[0]),"reintegro_rows":int(df_rein.shape[0])}


# ── LSTM helpers ──────────────────────────────────────────────────────────────

def _build_lstm_sequences(df, feature_cols, seq_len):
    X_list, y_list = [], []
    for _, grp in df.groupby("number", sort=True):
        grp = grp.sort_values("source_index").reset_index(drop=True)
        feats  = grp[feature_cols].values.astype(np.float32)
        labels = grp["label_next_appears"].values
        for i in range(seq_len, len(grp)):
            X_list.append(feats[i-seq_len:i])
            y_list.append(labels[i])
    if not X_list:
        return np.empty((0,seq_len,len(feature_cols)),dtype=np.float32), np.empty(0)
    return np.array(X_list,dtype=np.float32), np.array(y_list,dtype=np.float32)


def _train_lstm(X_train, y_train, n_features, model_path):
    try:
        import torch, torch.nn as nn
        from torch.utils.data import DataLoader, TensorDataset
    except ImportError:
        raise RuntimeError("PyTorch not installed.")

    class LSTMModel(nn.Module):
        def __init__(self,nf,h):
            super().__init__()
            self.lstm=nn.LSTM(nf,h,batch_first=True)
            self.fc=nn.Linear(h,1)
        def forward(self,x):
            _,(h,_)=self.lstm(x)
            return torch.sigmoid(self.fc(h[-1])).squeeze(1)

    n_val=max(1,int(len(X_train)*0.2))
    Xtr,Xvl=X_train[:-n_val],X_train[-n_val:]
    ytr,yvl=y_train[:-n_val],y_train[-n_val:]
    model=LSTMModel(n_features,LSTM_HIDDEN)
    opt=torch.optim.Adam(model.parameters(),lr=LSTM_LR)
    loss_fn=nn.BCELoss()
    dl=DataLoader(TensorDataset(torch.tensor(Xtr),torch.tensor(ytr)),
                  batch_size=LSTM_BATCH,shuffle=True)
    model.train()
    for _ in range(LSTM_EPOCHS):
        for xb,yb in dl:
            opt.zero_grad(); loss_fn(model(xb),yb).backward(); opt.step()
    model.eval()
    with torch.no_grad():
        preds=(model(torch.tensor(Xvl))>=0.5).float().numpy()
    acc=float((preds==yvl).mean()) if len(yvl) else 0.0
    torch.save({"state_dict":model.state_dict(),"n_features":n_features,
                "hidden":LSTM_HIDDEN,"seq_len":SEQ_LEN},model_path)
    return acc


def _predict_lstm(X_seq, model_path, n_features):
    try:
        import torch, torch.nn as nn
    except ImportError:
        return np.full(len(X_seq),0.5,dtype=np.float32)

    class LSTMModel(nn.Module):
        def __init__(self,nf,h):
            super().__init__()
            self.lstm=nn.LSTM(nf,h,batch_first=True)
            self.fc=nn.Linear(h,1)
        def forward(self,x):
            _,(h,_)=self.lstm(x)
            return torch.sigmoid(self.fc(h[-1])).squeeze(1)

    ckpt=torch.load(model_path,map_location="cpu")
    m=LSTMModel(ckpt["n_features"],ckpt["hidden"])
    m.load_state_dict(ckpt["state_dict"]); m.eval()
    with torch.no_grad():
        return m(torch.tensor(X_seq)).numpy().astype(np.float32)


# ── Neuro-symbolic penalties ──────────────────────────────────────────────────

def _ns_main_penalties(numbers, probs):
    p = np.ones(len(probs), dtype=np.float32)
    p[np.argsort(probs)[-3:]] *= 0.85
    for i, n in enumerate(numbers):
        if (n <= 5 or n >= 45) and probs[i] > 0.6:
            p[i] *= 0.90
    return p


def _ns_rein_penalties(numbers, probs):
    p = np.ones(len(probs), dtype=np.float32)
    p[np.argmax(probs)] *= 0.90
    return p


# ── Training ──────────────────────────────────────────────────────────────────

def train_la_primitiva_models(
    cutoff_draw_id: str | None = None,
    dataset_dir: str | None = None,
    model_dir: str | None = None,
) -> Dict:
    if dataset_dir is None:
        ds = prepare_la_primitiva_dataset(cutoff_draw_id=cutoff_draw_id)
        dataset_dir = ds["out_dir"]
    if model_dir is None:
        model_dir = MODEL_DIR_DEFAULT
    os.makedirs(model_dir, exist_ok=True)

    df_main = pd.read_csv(os.path.join(dataset_dir, "la_primitiva_main_dataset.csv"))
    df_rein = pd.read_csv(os.path.join(dataset_dir, "la_primitiva_reintegro_dataset.csv"))

    mf = ["weekday_idx","number","freq","gap","freq_norm","gap_cap",
          "draw_sum_mains","draw_even_mains","draw_complementario","draw_reintegro","is_current_main"]
    rf_cols = ["weekday_idx","number","freq","gap","freq_norm","gap_cap",
               "draw_sum_mains","draw_even_mains","draw_complementario","draw_reintegro","is_current_reintegro"]

    def split(df, fcols):
        iv=sorted(df["source_index"].unique())
        vs=set(iv[-max(1,int(len(iv)*0.2)):])
        mk=~df["source_index"].isin(vs)
        return (df.loc[mk,fcols].values,df.loc[~mk,fcols].values,
                df.loc[mk,"label_next_appears"].values,df.loc[~mk,"label_next_appears"].values)

    res: Dict = {"cutoff_draw_id":cutoff_draw_id,"dataset_dir":dataset_dir,"model_dir":model_dir}

    # Mains
    Xtrm,Xvlm,ytrm,yvlm = split(df_main, mf)
    sw = compute_sample_weight("balanced", ytrm)
    gbm_m = GradientBoostingClassifier(random_state=42)
    gbm_m.fit(Xtrm,ytrm,sample_weight=sw)
    gbm_m_p = os.path.join(model_dir,"la_primitiva_main_gb.pkl")
    joblib.dump({"model":gbm_m,"features":mf},gbm_m_p)

    rf_m = RandomForestClassifier(n_estimators=100,max_depth=10,class_weight="balanced",random_state=42,n_jobs=-1)
    rf_m.fit(Xtrm,ytrm)
    rf_m_p = os.path.join(model_dir,"la_primitiva_main_rf.pkl")
    joblib.dump({"model":rf_m,"features":mf},rf_m_p)

    Xs_m,ys_m = _build_lstm_sequences(df_main,mf,SEQ_LEN)
    lstm_m_p = os.path.join(model_dir,"la_primitiva_main_lstm.pt")
    lstm_m_acc = _train_lstm(Xs_m,ys_m,len(mf),lstm_m_p) if len(Xs_m)>SEQ_LEN*2 else 0.0

    res.update({"main_accuracy":float(gbm_m.score(Xvlm,yvlm)),
                "main_rf_accuracy":float(rf_m.score(Xvlm,yvlm)),
                "main_lstm_accuracy":lstm_m_acc,
                "main_model_path":gbm_m_p,"main_rf_path":rf_m_p,"main_lstm_path":lstm_m_p})

    # Reintegro
    Xtrr,Xvlr,ytrr,yvlr = split(df_rein, rf_cols)
    sw_r = compute_sample_weight("balanced", ytrr)
    gbm_r = GradientBoostingClassifier(random_state=42)
    gbm_r.fit(Xtrr,ytrr,sample_weight=sw_r)
    gbm_r_p = os.path.join(model_dir,"la_primitiva_reintegro_gb.pkl")
    joblib.dump({"model":gbm_r,"features":rf_cols},gbm_r_p)

    rf_r = RandomForestClassifier(n_estimators=100,max_depth=10,class_weight="balanced",random_state=42,n_jobs=-1)
    rf_r.fit(Xtrr,ytrr)
    rf_r_p = os.path.join(model_dir,"la_primitiva_reintegro_rf.pkl")
    joblib.dump({"model":rf_r,"features":rf_cols},rf_r_p)

    Xs_r,ys_r = _build_lstm_sequences(df_rein,rf_cols,SEQ_LEN)
    lstm_r_p = os.path.join(model_dir,"la_primitiva_reintegro_lstm.pt")
    lstm_r_acc = _train_lstm(Xs_r,ys_r,len(rf_cols),lstm_r_p) if len(Xs_r)>SEQ_LEN*2 else 0.0

    res.update({"reintegro_accuracy":float(gbm_r.score(Xvlr,yvlr)),
                "reintegro_rf_accuracy":float(rf_r.score(Xvlr,yvlr)),
                "reintegro_lstm_accuracy":lstm_r_acc,
                "reintegro_model_path":gbm_r_p,"reintegro_rf_path":rf_r_p,"reintegro_lstm_path":lstm_r_p})
    return res


# ── Inference ─────────────────────────────────────────────────────────────────

def compute_la_primitiva_probabilities(
    cutoff_draw_id: str | None = None,
) -> Dict:
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
            raise RuntimeError("No rows in la_primitiva_feature")

    draw_id = str(doc.get("id_sorteo") or "").strip()
    fecha   = str(doc.get("fecha_sorteo") or "").strip()
    wday    = _weekday_to_index(str(doc.get("dia_semana") or "").strip())
    freq    = list(doc.get("frequency") or [])
    gap     = list(doc.get("gap") or [])
    mdx     = list(doc.get("main_dx") or [])
    rdx     = list(doc.get("reintegro_dx") or [])
    cmains  = [int(x) for x in (doc.get("main_number") or []) if isinstance(x, int)]
    ccomp_raw = doc.get("complementario")
    ccomp   = int(ccomp_raw) if isinstance(ccomp_raw, int) else -1
    crein_raw = doc.get("reintegro")
    crein   = int(crein_raw) if isinstance(crein_raw, int) else -1
    src     = int(doc.get("source_index", 0))
    total   = src + 1
    sm = sum(cmains); em = sum(1 for x in cmains if x%2==0)
    client.close()

    num_mains = MAIN_MAX - MAIN_MIN + 1
    rein_offset = num_mains + num_mains  # 98

    mf = ["weekday_idx","number","freq","gap","freq_norm","gap_cap",
          "draw_sum_mains","draw_even_mains","draw_complementario","draw_reintegro","is_current_main"]
    rf_cols = ["weekday_idx","number","freq","gap","freq_norm","gap_cap",
               "draw_sum_mains","draw_even_mains","draw_complementario","draw_reintegro","is_current_reintegro"]

    main_rows, rein_rows = [], []
    for n in range(MAIN_MIN, MAIN_MAX + 1):
        i = n - MAIN_MIN
        fv = freq[i] if i < len(freq) else 0
        gr = gap[i]  if i < len(gap)  else None
        gv = -1 if gr is None else int(gr)
        main_rows.append({"weekday_idx":wday,"number":n,"freq":int(fv),"gap":gv,
                          "freq_norm":int(fv)/total if total else 0.0,
                          "gap_cap":min(gv,100) if gv>=0 else -1,
                          "draw_sum_mains":sm,"draw_even_mains":em,
                          "draw_complementario":ccomp,"draw_reintegro":crein,
                          "is_current_main":1 if (i<len(mdx) and int(mdx[i])!=0) else 0})

    for r in range(REIN_MIN, REIN_MAX + 1):
        ir = r - REIN_MIN; fi = rein_offset + ir
        fv = freq[fi] if fi < len(freq) else 0
        gr = gap[fi]  if fi < len(gap)  else None
        gv = -1 if gr is None else int(gr)
        rein_rows.append({"weekday_idx":wday,"number":r,"freq":int(fv),"gap":gv,
                          "freq_norm":int(fv)/total if total else 0.0,
                          "gap_cap":min(gv,100) if gv>=0 else -1,
                          "draw_sum_mains":sm,"draw_even_mains":em,
                          "draw_complementario":ccomp,"draw_reintegro":crein,
                          "is_current_reintegro":1 if (ir<len(rdx) and int(rdx[ir])!=0) else 0})

    df_m = pd.DataFrame(main_rows)
    df_r = pd.DataFrame(rein_rows)
    mdir = MODEL_DIR_DEFAULT

    def _tab(path, fcols, df):
        s = joblib.load(path)
        return s["model"].predict_proba(df[fcols].values)[:,1].astype(np.float32)

    gbm_m_p = os.path.join(mdir,"la_primitiva_main_gb.pkl")
    if not os.path.exists(gbm_m_p):
        raise RuntimeError("La Primitiva models not found. Run train first.")

    p_gbm_m = _tab(gbm_m_p, mf, df_m)
    rf_m_p  = os.path.join(mdir,"la_primitiva_main_rf.pkl")
    p_rf_m  = _tab(rf_m_p, mf, df_m) if os.path.exists(rf_m_p) else p_gbm_m
    lstm_m_p = os.path.join(mdir,"la_primitiva_main_lstm.pt")
    if os.path.exists(lstm_m_p):
        Xi = np.tile(df_m[mf].values.astype(np.float32),(1,SEQ_LEN,1)).reshape(len(df_m),SEQ_LEN,len(mf))
        p_lstm_m = _predict_lstm(Xi, lstm_m_p, len(mf))
    else:
        p_lstm_m = p_gbm_m
    p_main = W_GBM*p_gbm_m + W_RF*p_rf_m + W_LSTM*p_lstm_m
    mn = list(range(MAIN_MIN, MAIN_MAX+1))
    p_main = p_main * _ns_main_penalties(mn, p_main)
    mx = p_main.max(); p_main = p_main/mx if mx>0 else p_main

    gbm_r_p = os.path.join(mdir,"la_primitiva_reintegro_gb.pkl")
    p_gbm_r = _tab(gbm_r_p, rf_cols, df_r)
    rf_r_p  = os.path.join(mdir,"la_primitiva_reintegro_rf.pkl")
    p_rf_r  = _tab(rf_r_p, rf_cols, df_r) if os.path.exists(rf_r_p) else p_gbm_r
    lstm_r_p = os.path.join(mdir,"la_primitiva_reintegro_lstm.pt")
    if os.path.exists(lstm_r_p):
        Xi = np.tile(df_r[rf_cols].values.astype(np.float32),(1,SEQ_LEN,1)).reshape(len(df_r),SEQ_LEN,len(rf_cols))
        p_lstm_r = _predict_lstm(Xi, lstm_r_p, len(rf_cols))
    else:
        p_lstm_r = p_gbm_r
    p_rein = W_GBM*p_gbm_r + W_RF*p_rf_r + W_LSTM*p_lstm_r
    rn = list(range(REIN_MIN, REIN_MAX+1))
    p_rein = p_rein * _ns_rein_penalties(rn, p_rein)
    mx = p_rein.max(); p_rein = p_rein/mx if mx>0 else p_rein

    mains      = sorted([{"number":n,"p":float(p)} for n,p in zip(mn,p_main)], key=lambda x:x["p"],reverse=True)
    reintegros = sorted([{"number":r,"p":float(p)} for r,p in zip(rn,p_rein)], key=lambda x:x["p"],reverse=True)

    return {"cutoff_draw_id":cutoff_draw_id or draw_id,"draw_id":draw_id,
            "fecha_sorteo":fecha,"mains":mains,"reintegros":reintegros}


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Train La Primitiva ensemble (GBM + RF + LSTM).")
    parser.add_argument("--cutoff_draw_id", type=str, default=None)
    parser.add_argument("--out_dir", type=str, default=None)
    parser.add_argument("--train", action="store_true")
    args = parser.parse_args()

    if args.train:
        info = train_la_primitiva_models(cutoff_draw_id=args.cutoff_draw_id, dataset_dir=args.out_dir)
        print(f"La Primitiva models trained.")
        print(f"  GBM  main={info['main_accuracy']:.4f}  rein={info['reintegro_accuracy']:.4f}")
        print(f"  RF   main={info['main_rf_accuracy']:.4f}  rein={info['reintegro_rf_accuracy']:.4f}")
        print(f"  LSTM main={info['main_lstm_accuracy']:.4f}  rein={info['reintegro_lstm_accuracy']:.4f}")
    else:
        info = prepare_la_primitiva_dataset(cutoff_draw_id=args.cutoff_draw_id, out_dir=args.out_dir)
        print(f"La Primitiva datasets written to {info['out_dir']}. "
              f"main_rows={info['main_rows']}, reintegro_rows={info['reintegro_rows']}")


if __name__ == "__main__":
    main()
