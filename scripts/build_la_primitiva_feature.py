"""
Build La Primitiva per-draw feature rows into `la_primitiva_feature`.

Analogue of `build_euromillones_feature.py` and `build_el_gordo_feature.py`.

- Source:  `la_primitiva` collection.
- Target:  `la_primitiva_feature` collection.

For each draw (sorted by fecha_sorteo ascending), we compute:
- main_number: 6 main numbers 1–49
- complementario: 1–49 or None
- reintegro: 0–9 or None
- pre_id_sorteo: previous draw id
- main_dx: one-hot length 49 (main 1–49)
- complementario_dx: one-hot length 49 (C 1–49)
- reintegro_dx: one-hot length 10 (R 0–9)
- frequency: cumulative counts up to and including this draw:
  [49 mains, 49 complementario, 10 reintegro]
- gap: draws since last appearance for each main/C/R (None if never seen),
  laid out in the same order as frequency.
"""

import argparse
import os
import re
from datetime import datetime
from typing import List, Tuple, Optional

from pymongo import ASCENDING, MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "lottery")

SOURCE_COLLECTION = "la_primitiva"
TARGET_COLLECTION = "la_primitiva_feature"

MAIN_MIN, MAIN_MAX = 1, 49
COMPLEMENTARIO_MIN, COMPLEMENTARIO_MAX = 1, 49
REINTEGRO_MIN, REINTEGRO_MAX = 0, 9


def _weekday_name(date_str: str) -> str:
  try:
    return datetime.strptime(date_str, "%Y-%m-%d").strftime("%A")
  except Exception:
    return ""


def _parse_main_c_r(doc: dict) -> Tuple[List[int], Optional[int], Optional[int]]:
  """
  Parse La Primitiva main numbers (6), complementario (C), reintegro (R).

  Preference:
    1. Use `numbers` (6 elems) + `complementario` + `reintegro`.
    2. Parse `combinacion_acta` or `combinacion` like "48 - 38 - 40 - 08 - 25 - 47 C(20) R(9)".
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
      "complementario": 1,
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

  main_freq = [0] * (MAIN_MAX - MAIN_MIN + 1)  # 49
  comp_freq = [0] * (COMPLEMENTARIO_MAX - COMPLEMENTARIO_MIN + 1)  # 49
  rein_freq = [0] * (REINTEGRO_MAX - REINTEGRO_MIN + 1)  # 10

  main_last_seen = [-1] * (MAIN_MAX - MAIN_MIN + 1)
  comp_last_seen = [-1] * (COMPLEMENTARIO_MAX - COMPLEMENTARIO_MIN + 1)
  rein_last_seen = [-1] * (REINTEGRO_MAX - REINTEGRO_MIN + 1)

  processed = 0
  added = 0

  for idx, doc in enumerate(draws):
    draw_id = str(doc.get("id_sorteo") or "").strip()
    fecha_full = str(doc.get("fecha_sorteo") or "").strip()
    if not draw_id or not fecha_full:
      continue
    fecha = fecha_full.split(" ")[0]

    mains, complementario, reintegro = _parse_main_c_r(doc)
    if len(mains) != 6:
      continue

    pre_id = None if idx == 0 else str(draws[idx - 1].get("id_sorteo") or "").strip() or None

    main_dx = [0] * (MAIN_MAX - MAIN_MIN + 1)
    for n in mains:
      main_dx[n - MAIN_MIN] = 1

    comp_dx = [0] * (COMPLEMENTARIO_MAX - COMPLEMENTARIO_MIN + 1)
    if complementario is not None:
      comp_dx[complementario - COMPLEMENTARIO_MIN] = 1

    rein_dx = [0] * (REINTEGRO_MAX - REINTEGRO_MIN + 1)
    if reintegro is not None:
      rein_dx[reintegro - REINTEGRO_MIN] = 1

    # Frequency including current draw
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

    # Gaps
    main_gap: List[Optional[int]] = []
    for i in range(MAIN_MAX - MAIN_MIN + 1):
      last = main_last_seen[i]
      main_gap.append(None if last == -1 else idx - last)

    comp_gap: List[Optional[int]] = []
    for i in range(COMPLEMENTARIO_MAX - COMPLEMENTARIO_MIN + 1):
      last = comp_last_seen[i]
      comp_gap.append(None if last == -1 else idx - last)

    rein_gap: List[Optional[int]] = []
    for i in range(REINTEGRO_MAX - REINTEGRO_MIN + 1):
      last = rein_last_seen[i]
      rein_gap.append(None if last == -1 else idx - last)

    gap = main_gap + comp_gap + rein_gap

    out = {
      "id_sorteo": draw_id,
      "pre_id_sorteo": pre_id,
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
      "source_index": idx,
    }

    result = dst.replace_one({"id_sorteo": draw_id}, out, upsert=True)
    processed += 1
    if result.upserted_id is not None:
      added += 1

    # Update running stats
    for n in mains:
      i = n - MAIN_MIN
      main_freq[i] += 1
      main_last_seen[i] = idx

    if complementario is not None:
      j = complementario - COMPLEMENTARIO_MIN
      comp_freq[j] += 1
      comp_last_seen[j] = idx

    if reintegro is not None:
      k = reintegro - REINTEGRO_MIN
      rein_freq[k] += 1
      rein_last_seen[k] = idx

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

