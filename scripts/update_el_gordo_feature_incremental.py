"""
Incrementally append El Gordo rows into `el_gordo_feature` using the last feature row as state.

Workflow:
1. Run `build_el_gordo_feature.py` once to backfill all history.
2. After new draws are scraped into `el_gordo`, run this script to append only new feature rows.
"""

import os
import re
from datetime import datetime
from typing import List, Optional, Tuple

from pymongo import ASCENDING, MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "lottery")

SOURCE_COLLECTION = "el_gordo"
FEATURE_COLLECTION = "el_gordo_feature"

MAIN_MIN, MAIN_MAX = 1, 54
CLAVE_MIN, CLAVE_MAX = 0, 9


def _weekday_name(date_str: str) -> str:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%A")
    except Exception:
        return ""


def _parse_main_and_clave(doc: dict) -> Tuple[List[int], Optional[int]]:
    """
    Parse El Gordo: 5 main numbers (1–54) and 1 clave (0–9).
    Prefer `numbers` + `reintegro`; fallback to combinacion(_acta) like "1-2-3-4-5 R(7)".
    """
    mains: List[int] = []
    clave: Optional[int] = doc.get("reintegro")

    raw_numbers = doc.get("numbers")
    if isinstance(raw_numbers, list) and len(raw_numbers) >= 5:
        mains = [int(n) for n in raw_numbers[:5] if isinstance(n, (int, float, int))]
    else:
        text = (doc.get("combinacion_acta") or doc.get("combinacion") or "").strip()
        if isinstance(text, str) and text:
            match_r = re.search(r"R\s*\(\s*(\d+)\s*\)", text, re.I)
            if match_r:
                try:
                    clave = int(match_r.group(1))
                except Exception:
                    clave = clave
            main_part = re.split(r"\s+R\s*\(", text)[0].strip()
            parts = re.split(r"[\s\-]+", main_part)
            for p in parts:
                p = p.strip()
                if p.isdigit():
                    mains.append(int(p))
            mains = mains[:5]

    mains = [n for n in mains if MAIN_MIN <= n <= MAIN_MAX][:5]
    if clave is not None and not (CLAVE_MIN <= clave <= CLAVE_MAX):
        clave = None

    return mains, clave


def _load_last_state(client: MongoClient):
    """
    Load last feature row from el_gordo_feature and reconstruct
    running frequency and gap state.
    """
    db = client[MONGO_DB]
    feats = db[FEATURE_COLLECTION]

    last_doc = feats.find_one(sort=[("source_index", -1)])
    if not last_doc:
        raise RuntimeError(
            "No documents in el_gordo_feature. Run build_el_gordo_feature.py first."
        )

    last_index = int(last_doc.get("source_index", 0))
    last_fecha = (last_doc.get("fecha_sorteo") or "").strip()

    freq = list(last_doc.get("frequency") or [])
    if len(freq) < 54 + 10:
        raise RuntimeError("Last feature row has invalid frequency length.")
    main_freq = [int(x or 0) for x in freq[:54]]
    clave_freq = [int(x or 0) for x in freq[54:64]]

    gap_arr = list(last_doc.get("gap") or [])
    if len(gap_arr) < 54 + 10:
        main_gap = [None] * 54
        clave_gap = [None] * 10
    else:
        main_gap = gap_arr[:54]
        clave_gap = gap_arr[54:64]

    main_last_seen = [-1] * 54
    clave_last_seen = [-1] * 10

    for i in range(54):
        g = main_gap[i]
        if isinstance(g, (int, float)):
            main_last_seen[i] = last_index - int(g)

    for i in range(10):
        g = clave_gap[i]
        if isinstance(g, (int, float)):
            clave_last_seen[i] = last_index - int(g)

    last_id = str(last_doc.get("id_sorteo") or "").strip()

    return (
        last_index,
        last_fecha,
        last_id,
        main_freq,
        clave_freq,
        main_last_seen,
        clave_last_seen,
    )


def _load_new_draws(client: MongoClient, last_fecha: str):
    """
    Load new El Gordo draws with fecha_sorteo > last_fecha.
    """
    db = client[MONGO_DB]
    src = db[SOURCE_COLLECTION]

    if not last_fecha:
        cursor = src.find(
            {},
            projection={
                "id_sorteo": 1,
                "fecha_sorteo": 1,
                "numbers": 1,
                "reintegro": 1,
                "combinacion": 1,
                "combinacion_acta": 1,
            },
        ).sort("fecha_sorteo", ASCENDING)
    else:
        cursor = src.find(
            {"fecha_sorteo": {"$gt": last_fecha}},
            projection={
                "id_sorteo": 1,
                "fecha_sorteo": 1,
                "numbers": 1,
                "reintegro": 1,
                "combinacion": 1,
                "combinacion_acta": 1,
            },
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
            clave_freq,
            main_last_seen,
            clave_last_seen,
        ) = _load_last_state(client)

        draws = _load_new_draws(client, last_fecha)
        if not draws:
            print("No new El Gordo draws to process.")
            return

        db = client[MONGO_DB]
        feats = db[FEATURE_COLLECTION]

        print(
            f"Appending El Gordo features from fecha_sorteo > {last_fecha} "
            f"(starting at source_index={last_index + 1})…"
        )

        prev_id = last_id
        processed = 0

        for doc in draws:
            draw_id = str(doc.get("id_sorteo") or "").strip()
            fecha_full = str(doc.get("fecha_sorteo") or "").strip()
            if not draw_id or not fecha_full:
                continue
            fecha = fecha_full.split(" ")[0]

            mains, clave = _parse_main_and_clave(doc)
            if len(mains) != 5:
                continue

            last_index += 1

            main_dx = [0] * 54
            for n in mains:
                main_dx[n - MAIN_MIN] = 1

            clave_dx = [0] * 10
            if clave is not None:
                clave_dx[clave - CLAVE_MIN] = 1

            # frequency including current draw
            freq_main_current = main_freq[:]
            for n in mains:
                freq_main_current[n - MAIN_MIN] += 1

            freq_clave_current = clave_freq[:]
            if clave is not None:
                freq_clave_current[clave - CLAVE_MIN] += 1

            frequency = freq_main_current + freq_clave_current

            # gaps
            main_gap: List[Optional[int]] = []
            for i in range(54):
                last = main_last_seen[i]
                main_gap.append(None if last == -1 else last_index - last)

            clave_gap: List[Optional[int]] = []
            for i in range(10):
                last = clave_last_seen[i]
                clave_gap.append(None if last == -1 else last_index - last)

            gap = main_gap + clave_gap

            out = {
                "id_sorteo": draw_id,
                "pre_id_sorteo": prev_id,
                "fecha_sorteo": fecha,
                "dia_semana": _weekday_name(fecha),
                "main_number": mains,
                "clave": clave,
                "main_dx": main_dx,
                "clave_dx": clave_dx,
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

            if clave is not None:
                j = clave - CLAVE_MIN
                clave_freq[j] += 1
                clave_last_seen[j] = last_index

        print(f"Done. Appended {processed} new El Gordo feature rows.")
    finally:
        client.close()


if __name__ == "__main__":
    main()

