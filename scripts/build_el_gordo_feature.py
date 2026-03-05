"""
Build El Gordo per-draw feature rows into `el_gordo_feature`.

This is the El Gordo analogue of `build_euromillones_feature.py`:

- Source:  `el_gordo` collection (normalized draws from scraper/backfill).
- Target:  `el_gordo_feature` (one document per draw).

For each draw (sorted by `fecha_sorteo` ascending) we compute:

- main_number: list of 5 main numbers (1–54)
- clave: single clave number (0–9) – taken from `reintegro` or parsed from text
- pre_id_sorteo: previous draw id (or None for the first draw)
- main_dx: one-hot vector length 54 (index n-1 is 1 if main n appeared)
- clave_dx: one-hot vector length 10 (index clave is 1 if that clave appeared)
- frequency: cumulative appearance counts up to and including this draw
  * first 54 entries: mains 1–54
  * next 10 entries: clave 0–9
- gap: draws since last appearance for each main/clave (None if never seen)
  * first 54 entries: mains 1–54
  * next 10 entries: clave 0–9
"""

import argparse
import os
import re
from datetime import datetime
from typing import List, Tuple, Optional

from pymongo import ASCENDING, MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "lottery")

SOURCE_COLLECTION = "el_gordo"
TARGET_COLLECTION = "el_gordo_feature"

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

  Preference:
    1. Use `numbers` (5 mains) + `reintegro` (clave) if present.
    2. Parse `combinacion_acta` or `combinacion` text, e.g. "01-02-03-04-05 R(7)".
  """
  mains: List[int] = []
  clave: Optional[int] = doc.get("reintegro")

  # Preferred: numeric fields already split
  raw_numbers = doc.get("numbers")
  if isinstance(raw_numbers, list) and len(raw_numbers) >= 5:
    mains = [int(n) for n in raw_numbers[:5] if isinstance(n, (int, float, int))]
  else:
    text = (doc.get("combinacion_acta") or doc.get("combinacion") or "").strip()
    if isinstance(text, str) and text:
      # Try to extract clave from trailing R(x)
      match_r = re.search(r"R\s*\(\s*(\d+)\s*\)", text, re.I)
      if match_r:
        try:
          clave = int(match_r.group(1))
        except Exception:
          clave = clave
      # Main part before R(...)
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


def build(limit: int | None = None) -> None:
  client = MongoClient(MONGO_URI)
  db = client[MONGO_DB]
  src = db[SOURCE_COLLECTION]
  dst = db[TARGET_COLLECTION]

  dst.create_index([("id_sorteo", ASCENDING)], unique=True)

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

  if limit is not None and limit > 0:
    cursor = cursor.limit(limit)

  draws = list(cursor)
  if not draws:
    print("No draws found in source collection.")
    client.close()
    return

  # Running frequency and last-seen indices
  main_freq = [0] * (MAIN_MAX - MAIN_MIN + 1)  # 54
  clave_freq = [0] * (CLAVE_MAX - CLAVE_MIN + 1)  # 10 (0–9)
  main_last_seen = [-1] * (MAIN_MAX - MAIN_MIN + 1)
  clave_last_seen = [-1] * (CLAVE_MAX - CLAVE_MIN + 1)

  processed = 0
  added = 0

  for idx, doc in enumerate(draws):
    draw_id = str(doc.get("id_sorteo") or "").strip()
    fecha_full = str(doc.get("fecha_sorteo") or "").strip()
    if not draw_id or not fecha_full:
      continue
    fecha = fecha_full.split(" ")[0]

    mains, clave = _parse_main_and_clave(doc)
    if len(mains) != 5:
      continue

    pre_id = None if idx == 0 else str(draws[idx - 1].get("id_sorteo") or "").strip() or None

    # One-hot for mains 1–54
    main_dx = [0] * (MAIN_MAX - MAIN_MIN + 1)
    for n in mains:
      main_dx[n - MAIN_MIN] = 1

    # One-hot for clave 0–9
    clave_dx = [0] * (CLAVE_MAX - CLAVE_MIN + 1)
    if clave is not None:
      clave_dx[clave - CLAVE_MIN] = 1

    # Frequency including current draw
    freq_main_current = main_freq[:]
    for n in mains:
      freq_main_current[n - MAIN_MIN] += 1
    freq_clave_current = clave_freq[:]
    if clave is not None:
      freq_clave_current[clave - CLAVE_MIN] += 1
    frequency = freq_main_current + freq_clave_current

    # Gaps (None if never seen; otherwise draws since last appearance)
    main_gap: List[Optional[int]] = []
    for i in range(MAIN_MAX - MAIN_MIN + 1):
      last = main_last_seen[i]
      main_gap.append(None if last == -1 else idx - last)

    clave_gap: List[Optional[int]] = []
    for i in range(CLAVE_MAX - CLAVE_MIN + 1):
      last = clave_last_seen[i]
      clave_gap.append(None if last == -1 else idx - last)

    gap = main_gap + clave_gap

    out = {
      "id_sorteo": draw_id,
      "pre_id_sorteo": pre_id,
      "fecha_sorteo": fecha,
      "dia_semana": _weekday_name(fecha),
      "main_number": mains,
      "clave": clave,
      "main_dx": main_dx,
      "clave_dx": clave_dx,
      "frequency": frequency,
      "gap": gap,
      "source_index": idx,
    }

    result = dst.replace_one({"id_sorteo": draw_id}, out, upsert=True)
    processed += 1
    if result.upserted_id is not None:
      added += 1

    # Update running stats for next draws
    for n in mains:
      i = n - MAIN_MIN
      main_freq[i] += 1
      main_last_seen[i] = idx

    if clave is not None:
      j = clave - CLAVE_MIN
      clave_freq[j] += 1
      clave_last_seen[j] = idx

  print(f"Done. Found={len(draws)}, processed={processed}, added_new={added}")
  client.close()


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument(
    "--limit",
    type=int,
    default=0,
    help="How many sorted draws to process (0 = no limit)",
  )
  args = parser.parse_args()
  limit_arg = args.limit if args.limit and args.limit > 0 else None
  build(limit=limit_arg)

