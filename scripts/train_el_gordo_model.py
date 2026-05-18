"""
Training utilities for El Gordo ML models.

Ensemble of three models:
  1. Gradient Boosting (GBM)
  2. Random Forest (RF)
  3. LSTM

Final probability = 0.4 * p_gbm + 0.3 * p_rf + 0.3 * p_lstm
Neuro-symbolic penalties applied after ensemble.

El Gordo: 5 mains from 1–54, 1 clave from 0–9.
"""

from __future__ import annotations

import argparse
import os
from typing import Dict, List, Tuple

import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.utils.class_weight import compute_sample_weight

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "lottery")
FEATURE_COLLECTION = "el_gordo_feature"

MAIN_MIN, MAIN_MAX = 1, 54
CLAVE_MIN, CLAVE_MAX = 0, 9
SEQ_LEN     = 20
LSTM_HIDDEN = 32
LSTM_EPOCHS = 15
LSTM_LR     = 1e-3
LSTM_BATCH  = 256

MODEL_DIR_DEFAULT = os.path.join(BASE_DIR, "backend", "models", "el_gordo_ml")

W_GBM  = 0.40
W_RF   = 0.30
W_LSTM = 0.30


def _get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI)


def _weekday_to_index(name: str) -> int:
    return {"Monday":0,"Tuesday":1,"Wednesday":2,"Thursday":3,
            "Friday":4,"Saturday":5,"Sunday":6}.get((name or "").strip(), -1)


def _default_output_dir() -> str:
    return os.path.join(BASE_DIR, "data", "el_gordo")


def _load_feature_rows() -> List[dict]:
    client = _get_mongo_client()
    db = client[MONGO_DB]
    docs = list(db[FEATURE_COLLECTION].find(
        {},
        projection={"id_sorteo":1,"fecha_sorteo":1,"dia_semana":1,
                    "main_number":1,"clave":1,"main_dx":1,"clave_dx":1,
                    "frequency":1,"gap":1,"source_index":1},
    ).sort("source_index", 1))
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
            raise RuntimeError(f"cutoff_draw_id {cutoff_draw_id!r} not found in el_gordo_feature")
        docs = docs[:idx + 1]
    if len(docs) < 2:
        raise RuntimeError("Need at least 2 rows in el_gordo_feature.")

    main_rows: List[Dict] = []
    clave_rows: List[Dict] = []

    for idx in range(len(docs) - 1):
        cur, nxt = docs[idx], docs[idx + 1]
        src  = int(cur.get("source_index", idx))
        wday = _weekday_to_index(str(cur.get("dia_semana","")).strip())
        freq = list(cur.get("frequency") or [])
        gap  = list(cur.get("gap") or [])
        mdx  = list(cur.get("main_dx") or [])
        cdx  = list(cur.get("clave_dx") or [])
        cmains = [int(x) for x in (cur.get("main_number") or []) if isinstance(x, int)]
        cclave_raw = cur.get("clave")
        cclave = int(cclave_raw) if isinstance(cclave_raw, int) else -1
        nmains = {int(x) for x in (nxt.get("main_number") or []) if isinstance(x, int)}
        nclave_raw = nxt.get("clave")
        nclave = int(nclave_raw) if isinstance(nclave_raw, int) else None
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
                "draw_sum_mains":sm,"draw_even_mains":em,"draw_clave":cclave,
                "is_current_main":1 if (i<len(mdx) and int(mdx[i])!=0) else 0,
                "label_next_appears":1 if n in nmains else 0,
            })

        clave_offset = MAIN_MAX - MAIN_MIN + 1  # 54
        for c in range(CLAVE_MIN, CLAVE_MAX + 1):
            ic = c - CLAVE_MIN; fi = clave_offset + ic
            fv = freq[fi] if fi < len(freq) else 0
            gr = gap[fi]  if fi < len(gap)  else None
            gv = -1 if gr is None else int(gr)
            clave_rows.append({
                "source_index":src,"weekday_idx":wday,"number":c,
                "freq":int(fv),"gap":gv,
                "freq_norm":int(fv)/total if total else 0.0,
                "gap_cap":min(gv,100) if gv>=0 else -1,
                "draw_sum_mains":sm,"draw_even_mains":em,"draw_clave":cclave,
                "is_current_clave":1 if (ic<len(cdx) and int(cdx[ic])!=0) else 0,
                "label_next_appears":1 if (nclave is not None and c==nclave) else 0,
            })

    return pd.DataFrame(main_rows), pd.DataFrame(clave_rows)


def prepare_el_gordo_dataset(
    cutoff_draw_id: str | None = None,
    out_dir: str | None = None,
) -> Dict:
    df_main, df_clave = build_per_number_datasets(cutoff_draw_id=cutoff_draw_id)
    if out_dir is None:
        out_dir = _default_output_dir()
    os.makedirs(out_dir, exist_ok=True)
    mp = os.path.join(out_dir, "el_gordo_main_dataset.csv")
    cp = os.path.join(out_dir, "el_gordo_clave_dataset.csv")
    df_main.to_csv(mp, index=False)
    df_clave.to_csv(cp, index=False)
    return {"cutoff_draw_id":cutoff_draw_id,"out_dir":out_dir,
            "main_path":mp,"clave_path":cp,
            "main_rows":int(df_main.shape[0]),"clave_rows":int(df_clave.shape[0])}


# ── LSTM (shared helpers, same as euromillones) ───────────────────────────────

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
        if (n <= 5 or n >= 50) and probs[i] > 0.6:
            p[i] *= 0.90
    return p


def _ns_clave_penalties(numbers, probs):
    # Clave 0–9: no strong domain rules, just dampen top-1 slightly
    p = np.ones(len(probs), dtype=np.float32)
    p[np.argmax(probs)] *= 0.90
    return p


# ── Training ──────────────────────────────────────────────────────────────────

def train_el_gordo_models(
    cutoff_draw_id: str | None = None,
    dataset_dir: str | None = None,
    model_dir: str | None = None,
) -> Dict:
    if dataset_dir is None:
        ds = prepare_el_gordo_dataset(cutoff_draw_id=cutoff_draw_id)
        dataset_dir = ds["out_dir"]
    if model_dir is None:
        model_dir = MODEL_DIR_DEFAULT
    os.makedirs(model_dir, exist_ok=True)

    df_main  = pd.read_csv(os.path.join(dataset_dir, "el_gordo_main_dataset.csv"))
    df_clave = pd.read_csv(os.path.join(dataset_dir, "el_gordo_clave_dataset.csv"))

    mf = ["weekday_idx","number","freq","gap","freq_norm","gap_cap",
          "draw_sum_mains","draw_even_mains","draw_clave","is_current_main"]
    cf = ["weekday_idx","number","freq","gap","freq_norm","gap_cap",
          "draw_sum_mains","draw_even_mains","draw_clave","is_current_clave"]

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
    gbm_m = GradientBoostingClassifier(n_estimators=100,max_depth=5,learning_rate=0.08,random_state=42)
    gbm_m.fit(Xtrm,ytrm,sample_weight=sw)
    gbm_m_p = os.path.join(model_dir,"el_gordo_main_gb.joblib")
    joblib.dump({"model":gbm_m,"features":mf},gbm_m_p)

    rf_m = RandomForestClassifier(n_estimators=100,max_depth=10,class_weight="balanced",random_state=42,n_jobs=-1)
    rf_m.fit(Xtrm,ytrm)
    rf_m_p = os.path.join(model_dir,"el_gordo_main_rf.joblib")
    joblib.dump({"model":rf_m,"features":mf},rf_m_p)

    Xs_m,ys_m = _build_lstm_sequences(df_main,mf,SEQ_LEN)
    lstm_m_p = os.path.join(model_dir,"el_gordo_main_lstm.pt")
    lstm_m_acc = _train_lstm(Xs_m,ys_m,len(mf),lstm_m_p) if len(Xs_m)>SEQ_LEN*2 else 0.0

    res.update({"main_accuracy":float(gbm_m.score(Xvlm,yvlm)),
                "main_rf_accuracy":float(rf_m.score(Xvlm,yvlm)),
                "main_lstm_accuracy":lstm_m_acc,
                "main_model_path":gbm_m_p,"main_rf_path":rf_m_p,"main_lstm_path":lstm_m_p})

    # Clave
    Xtrc,Xvlc,ytrc,yvlc = split(df_clave, cf)
    sw_c = compute_sample_weight("balanced", ytrc)
    gbm_c = GradientBoostingClassifier(n_estimators=100,max_depth=5,learning_rate=0.08,random_state=42)
    gbm_c.fit(Xtrc,ytrc,sample_weight=sw_c)
    gbm_c_p = os.path.join(model_dir,"el_gordo_clave_gb.joblib")
    joblib.dump({"model":gbm_c,"features":cf},gbm_c_p)

    rf_c = RandomForestClassifier(n_estimators=100,max_depth=10,class_weight="balanced",random_state=42,n_jobs=-1)
    rf_c.fit(Xtrc,ytrc)
    rf_c_p = os.path.join(model_dir,"el_gordo_clave_rf.joblib")
    joblib.dump({"model":rf_c,"features":cf},rf_c_p)

    Xs_c,ys_c = _build_lstm_sequences(df_clave,cf,SEQ_LEN)
    lstm_c_p = os.path.join(model_dir,"el_gordo_clave_lstm.pt")
    lstm_c_acc = _train_lstm(Xs_c,ys_c,len(cf),lstm_c_p) if len(Xs_c)>SEQ_LEN*2 else 0.0

    res.update({"clave_accuracy":float(gbm_c.score(Xvlc,yvlc)),
                "clave_rf_accuracy":float(rf_c.score(Xvlc,yvlc)),
                "clave_lstm_accuracy":lstm_c_acc,
                "clave_model_path":gbm_c_p,"clave_rf_path":rf_c_p,"clave_lstm_path":lstm_c_p})
    return res


# ── Inference ─────────────────────────────────────────────────────────────────

def compute_el_gordo_probabilities(
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
            raise RuntimeError("No rows in el_gordo_feature")

    draw_id = str(doc.get("id_sorteo") or "").strip()
    fecha   = str(doc.get("fecha_sorteo") or "").strip()
    wday    = _weekday_to_index(str(doc.get("dia_semana") or "").strip())
    freq    = list(doc.get("frequency") or [])
    gap     = list(doc.get("gap") or [])
    mdx     = list(doc.get("main_dx") or [])
    cdx     = list(doc.get("clave_dx") or [])
    cmains  = [int(x) for x in (doc.get("main_number") or []) if isinstance(x, int)]
    cclave_raw = doc.get("clave")
    cclave  = int(cclave_raw) if isinstance(cclave_raw, int) else -1
    src     = int(doc.get("source_index", 0))
    total   = src + 1
    sm = sum(cmains); em = sum(1 for x in cmains if x%2==0)
    client.close()

    mf = ["weekday_idx","number","freq","gap","freq_norm","gap_cap",
          "draw_sum_mains","draw_even_mains","draw_clave","is_current_main"]
    cf = ["weekday_idx","number","freq","gap","freq_norm","gap_cap",
          "draw_sum_mains","draw_even_mains","draw_clave","is_current_clave"]

    main_rows, clave_rows = [], []
    for n in range(MAIN_MIN, MAIN_MAX + 1):
        i = n - MAIN_MIN
        fv = freq[i] if i < len(freq) else 0
        gr = gap[i]  if i < len(gap)  else None
        gv = -1 if gr is None else int(gr)
        main_rows.append({"weekday_idx":wday,"number":n,"freq":int(fv),"gap":gv,
                          "freq_norm":int(fv)/total if total else 0.0,
                          "gap_cap":min(gv,100) if gv>=0 else -1,
                          "draw_sum_mains":sm,"draw_even_mains":em,"draw_clave":cclave,
                          "is_current_main":1 if (i<len(mdx) and int(mdx[i])!=0) else 0})

    clave_offset = MAIN_MAX - MAIN_MIN + 1
    for c in range(CLAVE_MIN, CLAVE_MAX + 1):
        ic = c - CLAVE_MIN; fi = clave_offset + ic
        fv = freq[fi] if fi < len(freq) else 0
        gr = gap[fi]  if fi < len(gap)  else None
        gv = -1 if gr is None else int(gr)
        clave_rows.append({"weekday_idx":wday,"number":c,"freq":int(fv),"gap":gv,
                           "freq_norm":int(fv)/total if total else 0.0,
                           "gap_cap":min(gv,100) if gv>=0 else -1,
                           "draw_sum_mains":sm,"draw_even_mains":em,"draw_clave":cclave,
                           "is_current_clave":1 if (ic<len(cdx) and int(cdx[ic])!=0) else 0})

    df_m = pd.DataFrame(main_rows)
    df_c = pd.DataFrame(clave_rows)
    mdir = MODEL_DIR_DEFAULT

    def _tab(path, fcols, df):
        s = joblib.load(path)
        return s["model"].predict_proba(df[fcols].values)[:,1].astype(np.float32)

    gbm_m_p  = os.path.join(mdir,"el_gordo_main_gb.joblib")
    if not os.path.exists(gbm_m_p):
        raise RuntimeError("El Gordo models not found. Run train first.")

    p_gbm_m = _tab(gbm_m_p, mf, df_m)
    rf_m_p  = os.path.join(mdir,"el_gordo_main_rf.joblib")
    p_rf_m  = _tab(rf_m_p, mf, df_m) if os.path.exists(rf_m_p) else p_gbm_m
    lstm_m_p = os.path.join(mdir,"el_gordo_main_lstm.pt")
    if os.path.exists(lstm_m_p):
        Xi = np.tile(df_m[mf].values.astype(np.float32),(1,SEQ_LEN,1)).reshape(len(df_m),SEQ_LEN,len(mf))
        p_lstm_m = _predict_lstm(Xi, lstm_m_p, len(mf))
    else:
        p_lstm_m = p_gbm_m
    p_main = W_GBM*p_gbm_m + W_RF*p_rf_m + W_LSTM*p_lstm_m
    mn = list(range(MAIN_MIN, MAIN_MAX+1))
    p_main = p_main * _ns_main_penalties(mn, p_main)
    mx = p_main.max(); p_main = p_main/mx if mx>0 else p_main

    gbm_c_p  = os.path.join(mdir,"el_gordo_clave_gb.joblib")
    p_gbm_c  = _tab(gbm_c_p, cf, df_c)
    rf_c_p   = os.path.join(mdir,"el_gordo_clave_rf.joblib")
    p_rf_c   = _tab(rf_c_p, cf, df_c) if os.path.exists(rf_c_p) else p_gbm_c
    lstm_c_p = os.path.join(mdir,"el_gordo_clave_lstm.pt")
    if os.path.exists(lstm_c_p):
        Xi = np.tile(df_c[cf].values.astype(np.float32),(1,SEQ_LEN,1)).reshape(len(df_c),SEQ_LEN,len(cf))
        p_lstm_c = _predict_lstm(Xi, lstm_c_p, len(cf))
    else:
        p_lstm_c = p_gbm_c
    p_clave = W_GBM*p_gbm_c + W_RF*p_rf_c + W_LSTM*p_lstm_c
    cn = list(range(CLAVE_MIN, CLAVE_MAX+1))
    p_clave = p_clave * _ns_clave_penalties(cn, p_clave)
    mx = p_clave.max(); p_clave = p_clave/mx if mx>0 else p_clave

    mains  = sorted([{"number":n,"p":float(p)} for n,p in zip(mn,p_main)],  key=lambda x:x["p"],reverse=True)
    claves = sorted([{"number":c,"p":float(p)} for c,p in zip(cn,p_clave)], key=lambda x:x["p"],reverse=True)

    return {"cutoff_draw_id":cutoff_draw_id or draw_id,"draw_id":draw_id,
            "fecha_sorteo":fecha,"mains":mains,"claves":claves}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff_draw_id", type=str, default="")
    args = parser.parse_args()
    info = prepare_el_gordo_dataset(cutoff_draw_id=args.cutoff_draw_id or None)
    print("El Gordo dataset prepared:", info["main_rows"], "main rows,", info["clave_rows"], "clave rows")
