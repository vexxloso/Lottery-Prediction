"""
Incrementally append La Primitiva rows into `la_primitiva_feature` using the last feature row as state.

Workflow:
1. Run `build_la_primitiva_feature.py` once to backfill all history.
2. After new draws are scraped into `la_primitiva`, run this script to append only new feature rows.
"""

import os
import re
from datetime import datetime
from typing import List, Optional, Tuple

from pymongo import ASCENDING, MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "lottery")

SOURCE_COLLECTION = "la_primitiva"
FEATURE_COLLECTION = "la_primitiva_feature"

MAIN_MIN, MAIN_MAX = 1, 49
COMPLEMENTARIO_MIN, COMPLEMENTARIO_MAX = 1, 49
REINTEGRO_MIN, REINTEGRO_MAX = 0, 9


def _weekday_name(date_str: str) -> str:
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        return d.strftime("%A")
    except Exception:
        return ""


def _parse_main_c_r(doc: dict) -> Tuple[List[int], Optional[int], Optional[int]]:
    """
    Parse La Primitiva: 6 mains (1–49), complementario (1–49), reintegro (0–9).
    Prefer `numbers` + `complementario` + `reintegro`; fallback to combinacion(_acta).
    """
    mains: List[int] = []
    complementario: Optional[int] = doc.get("complementario")
    reintegro: Optional[int] = doc.get("reintegro")

    raw_numbers = doc.get("numbers")
    if isinstance(raw_numbers, list) and len(raw_numbers) >= 6:
        mains = [int(n) for n in raw_numbers[:6] if isinstance(n, (int, float, int))]
    else:
        text = (doc.get("combinacion_acta") or doc.get("combinacion") or "").strip()
        if isinstance(text, str) and text:
            match_c = re.search(r"C\s*\(\s*(\d+)\s*\)", text, re.I)
            match_r = re.search(r"R\s*\(\s*(\d+)\s*\)", text, re.I)
            if match_c:
                try:
                    complementario = int(match_c.group(1))
                except Exception:
                    complementario = complementario
            if match_r:
                try:
                    reintegro = int(match_r.group(1))
                except Exception:
                    reintegro = reintegro
            main_part = re.split(r"\s+C\s*\(|\s+R\s*\(", text)[0].strip()
            parts = re.split(r"[\s\-]+", main_part)
            for p in parts:
                p = p.strip()
                if p.isdigit():
                    mains.append(int(p))
            mains = mains[:6]

    mains = [n for n in mains if MAIN_MIN <= n <= MAIN_MAX][:6]
    if complementario is not None and not (COMPLEMENTARIO_MIN <= complementario <= COMPLEMENTARIO_MAX):
        complementario = None
    if reintegro is not None and not (REINTEGRO_MIN <= reintegro <= REINTEGRO_MAX):
        reintegro = None

    return mains, complementario, reintegro


def _load_last_state(client: MongoClient):
    """
    Load last feature row from la_primitiva_feature and reconstruct
    running frequency and gap state.
    """
    db = client[MONGO_DB]
    feats = db[FEATURE_COLLECTION]

    last_doc = feats.find_one(sort=[("source_index", -1)])
    if not last_doc:
        raise RuntimeError(
            "No documents in la_primitiva_feature. Run build_la_primitiva_feature.py first."
        )

    last_index = int(last_doc.get("source_index", 0))
    last_fecha = (last_doc.get("fecha_sorteo") or "").strip()

    freq = list(last_doc.get("frequency") or [])
    if len(freq) < 49 + 49 + 10:
        raise RuntimeError("Last feature row has invalid frequency length.")
    main_freq = [int(x or 0) for x in freq[:49]]
    comp_freq = [int(x or 0) for x in freq[49:98]]
    rein_freq = [int(x or 0) for x in freq[98:108]]

    gap_arr = list(last_doc.get("gap") or [])
    if len(gap_arr) < 49 + 49 + 10:
        main_gap = [None] * 49
        comp_gap = [None] * 49
        rein_gap = [None] * 10
    else:
        main_gap = gap_arr[:49]
        comp_gap = gap_arr[49:98]
        rein_gap = gap_arr[98:108]

    main_last_seen = [-1] * 49
    comp_last_seen = [-1] * 49
    rein_last_seen = [-1] * 10

    for i in range(49):
        g = main_gap[i]
        if isinstance(g, (int, float)):
            main_last_seen[i] = last_index - int(g)

    for i in range(49):
        g = comp_gap[i]
        if isinstance(g, (int, float)):
            comp_last_seen[i] = last_index - int(g)

    for i in range(10):
        g = rein_gap[i]
        if isinstance(g, (int, float)):
            rein_last_seen[i] = last_index - int(g)

    last_id = str(last_doc.get("id_sorteo") or "").strip()

    return (
        last_index,
        last_fecha,
        last_id,
        main_freq,
        comp_freq,
        rein_freq,
        main_last_seen,
        comp_last_seen,
        rein_last_seen,
    )


def _load_new_draws(client: MongoClient, last_fecha: str):
    """
    Load new La Primitiva draws with fecha_sorteo > last_fecha.
    """
    db = client[MONGO_DB]
    src = db[SOURCE_COLLECTION]

    projection = {
        "id_sorteo": 1,
        "fecha_sorteo": 1,
        "numbers": 1,
        "complementario": 1,
        "reintegro": 1,
        "combinacion": 1,
        "combinacion_acta": 1,
    }

    if not last_fecha:
        cursor = src.find({}, projection=projection).sort("fecha_sorteo", ASCENDING)
    else:
        cursor = src.find(
            {"fecha_sorteo": {"$gt": last_fecha}},
            projection=projection,
        ).sort("fecha_sorteo", ASCENDING)

    return list(cursor)


def main() -> None:
    client = MongoClient(MONGO_URI)
    try:
        (
            last_index,
            last_fecha,
            last_id,
            main_freq,
            comp_freq,
            rein_freq,
            main_last_seen,
            comp_last_seen,
            rein_last_seen,
        ) = _load_last_state(client)

        draws = _load_new_draws(client, last_fecha)
        if not draws:
            print("No new La Primitiva draws to process.")
            return

        db = client[MONGO_DB]
        feats = db[FEATURE_COLLECTION]

        print(
            f"Appending La Primitiva features from fecha_sorteo > {last_fecha} "
            f"(starting at source_index={last_index + 1})…"
        )

        prev_id = last_id
        processed = 0

        for doc in draws:
            draw_id = str(doc.get("id_sorteo") or "").strip()
            fecha_full = (doc.get("fecha_sorteo") or "").strip()
            if not draw_id or not fecha_full:
                continue
            fecha = fecha_full.split(" ")[0]

            mains, complementario, reintegro = _parse_main_c_r(doc)
            if len(mains) != 6:
                continue

            last_index += 1

            main_dx = [0] * 49
            for n in mains:
                main_dx[n - MAIN_MIN] = 1

            comp_dx = [0] * 49
            if complementario is not None:
                comp_dx[complementario - COMPLEMENTARIO_MIN] = 1

            rein_dx = [0] * 10
            if reintegro is not None:
                rein_dx[reintegro - REINTEGRO_MIN] = 1

            # frequency including current draw
            freq_main_current = main_freq[:]
            for n in mains:
                freq_main_current[n - MAIN_MIN] += 1

            freq_comp_current = comp_freq[:]
            if complementario is not None:
                freq_comp_current[complementario - COMPLEMENTARIO_MIN] += 1

            freq_rein_current = rein_freq[:]
            if reintegro is not None:
                freq_rein_current[reintegro - REINTEGRO_MIN] += 1

            frequency = freq_main_current + freq_comp_current + freq_rein_current

            # gaps
            main_gap: List[Optional[int]] = []
            for i in range(49):
                last = main_last_seen[i]
                main_gap.append(None if last == -1 else last_index - last)

            comp_gap: List[Optional[int]] = []
            for i in range(49):
                last = comp_last_seen[i]
                comp_gap.append(None if last == -1 else last_index - last)

            rein_gap: List[Optional[int]] = []
            for i in range(10):
                last = rein_last_seen[i]
                rein_gap.append(None if last == -1 else last_index - last)

            gap = main_gap + comp_gap + rein_gap

            out = {
                "id_sorteo": draw_id,
                "pre_id_sorteo": prev_id,
                "fecha_sorteo": fecha,
                "dia_semana": _weekday_name(fecha),
                "main_number": mains,
                "complementario": complementario,
                "reintegro": reintegro,
                "main_dx": main_dx,
                "complementario_dx": comp_dx,
                "reintegro_dx": rein_dx,
                "frequency": frequency,
                "gap": gap,
                "source_index": last_index,
            }

            feats.replace_one({"id_sorteo": draw_id}, out, upsert=True)
            processed += 1
            prev_id = draw_id

            # update running state
            for n in mains:
                i = n - MAIN_MIN
                main_freq[i] += 1
                main_last_seen[i] = last_index

            if complementario is not None:
                j = complementario - COMPLEMENTARIO_MIN
                comp_freq[j] += 1
                comp_last_seen[j] = last_index

            if reintegro is not None:
                k = reintegro - REINTEGRO_MIN
                rein_freq[k] += 1
                rein_last_seen[k] = last_index

        print(f"Done. Appended {processed} new La Primitiva feature rows.")
    finally:
        client.close()


if __name__ == "__main__":
    main()

