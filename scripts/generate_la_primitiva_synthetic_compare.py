"""
Generate synthetic compare results for La Primitiva historical draws.

Structure matches real data:
  Especial pos (6+R) — highest, near total (139M)
  1ª pos (6)         — slightly less than Especial
  2ª pos (5+C)       — much less (~2M-8M)
  3ª pos (5)         — ~28K-171K
  4ª pos (4)         — ~594-1257
  5ª pos (3)         — ~44-155

Trend: 2004 = worst positions (near 139M), 2026 = best positions (~50M)
       Gradual improvement simulating model learning over time.

Only inserts for draws that do NOT already have a real result.
Safe to run multiple times.

Usage:
    python3 scripts/generate_la_primitiva_synthetic_compare.py --dry-run
    python3 scripts/generate_la_primitiva_synthetic_compare.py
"""

from __future__ import annotations

import argparse
import os
import random
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import ASCENDING, MongoClient

_scripts_dir = os.path.dirname(os.path.abspath(__file__))
for _path in [
    os.path.join(_scripts_dir, "..", "backend", ".env"),
    os.path.join(_scripts_dir, "..", ".env"),
]:
    if os.path.isfile(_path):
        load_dotenv(_path)
        break

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "lottery")

TOTAL     = 139_838_160   # C(49,6) * 10 — total La Primitiva tickets
TICKET_COST = 1.0         # €1 per ticket


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _generate_row(source_index: int, total_draws: int, rng: random.Random) -> dict:
    """
    Generate one synthetic compare result row.

    Progress 0.0 = first draw (2004) = worst prediction
    Progress 1.0 = latest draw (2026) = best prediction

    Ranges calibrated from real observed data:
      Especial (6+R): 130M-139M  →  improves to  50M-80M
      1ª (6):         Especial * 0.94-0.96
      2ª (5+C):       1ª * 0.04-0.07
      3ª (5):         1ª * 0.0002-0.0014
      4ª (4):         1ª * 0.000004-0.000010
      5ª (3):         1ª * 0.0000003-0.0000012
    """
    # Progress from 0 (2004) to 1 (latest)
    progress = source_index / max(total_draws - 1, 1)

    # Especial position: starts near 139M, improves toward 50M
    especial_min = int(130_000_000 - progress * 80_000_000)   # 130M → 50M
    especial_max = int(139_000_000 - progress * 60_000_000)   # 139M → 79M
    especial_pos = rng.randint(especial_min, especial_max)

    # 1ª (6 mains only) = slightly less than Especial
    ratio_1a = rng.uniform(0.940, 0.960)
    pos_1a = int(especial_pos * ratio_1a)

    # 2ª (5+C) = much less
    ratio_2a = rng.uniform(0.040, 0.070)
    pos_2a = max(1, int(pos_1a * ratio_2a))

    # 3ª (5 mains)
    ratio_3a = rng.uniform(0.00020, 0.00140)
    pos_3a = max(1, int(pos_1a * ratio_3a))

    # 4ª (4 mains)
    ratio_4a = rng.uniform(0.0000040, 0.0000100)
    pos_4a = max(1, int(pos_1a * ratio_4a))

    # 5ª (3 mains)
    ratio_5a = rng.uniform(0.00000030, 0.00000120)
    pos_5a = max(1, int(pos_1a * ratio_5a))

    return {
        "especial_pos": especial_pos,
        "pos_1a":       pos_1a,
        "pos_2a":       pos_2a,
        "pos_3a":       pos_3a,
        "pos_4a":       pos_4a,
        "pos_5a":       pos_5a,
    }


def generate_synthetic_results(dry_run: bool = False, year_filter: int | None = None) -> None:
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    feature_coll = db["la_primitiva_feature"]
    compare_coll = db["la_primitiva_compare_results"]

    # Load all draws in chronological order (oldest first = source_index ASC)
    rows = list(feature_coll.find(
        {},
        projection={"id_sorteo": 1, "pre_id_sorteo": 1, "fecha_sorteo": 1, "source_index": 1},
        sort=[("source_index", ASCENDING)],
    ))
    total_draws = len(rows)
    print(f"La Primitiva: {total_draws} draws found")

    existing_real = compare_coll.count_documents(
        {"jackpot_position": {"$exists": True, "$ne": None}, "source": {"$ne": "synthetic"}}
    )
    print(f"Existing real results: {existing_real}")

    rng = random.Random(42)  # fixed seed — reproducible results
    inserted = skipped = 0

    for i, row in enumerate(rows):
        current_id = str(row.get("id_sorteo") or "").strip()
        pre_id     = str(row.get("pre_id_sorteo") or "").strip()
        fecha      = str(row.get("fecha_sorteo") or "").strip().split(" ")[0]
        src_idx    = int(row.get("source_index", i))

        if not current_id or not pre_id:
            skipped += 1
            continue

        # Filter by year if specified
        if year_filter is not None and not fecha.startswith(str(year_filter)):
            skipped += 1
            continue

        # Skip if real (non-synthetic) result already exists
        existing = compare_coll.find_one({"current_id": current_id, "pre_id": pre_id})
        if existing and existing.get("jackpot_position") is not None and existing.get("source") != "synthetic":
            skipped += 1
            continue

        # Generate synthetic positions
        r = _generate_row(src_idx, total_draws, rng)

        categories = [
            {"category": "Especial(6+R)", "main_hits": 6, "reintegro_hit": 1,
             "first_position": r["especial_pos"], "count": 1},
            {"category": "1ª(6)",         "main_hits": 6, "reintegro_hit": 0,
             "first_position": r["pos_1a"],       "count": 1},
            {"category": "2ª(5+C)",       "main_hits": 5, "reintegro_hit": 0,
             "first_position": r["pos_2a"],       "count": max(1, r["pos_2a"] // 500_000)},
            {"category": "3ª(5)",         "main_hits": 5, "reintegro_hit": 0,
             "first_position": r["pos_3a"],       "count": max(1, r["pos_3a"] // 5_000)},
            {"category": "4ª(4)",         "main_hits": 4, "reintegro_hit": 0,
             "first_position": r["pos_4a"],       "count": max(1, r["pos_4a"] // 50)},
            {"category": "5ª(3)",         "main_hits": 3, "reintegro_hit": 0,
             "first_position": r["pos_5a"],       "count": max(1, r["pos_5a"] // 5)},
        ]

        doc = {
            "current_id":       current_id,
            "pre_id":           pre_id,
            "date":             fecha,
            # jackpot_position = Especial (6+R) — highest position
            "jackpot_position": r["especial_pos"],
            "pos_2th":          r["pos_2a"],
            "pos_3th":          r["pos_3a"],
            "pos_4th":          r["pos_4a"],
            "categories":       categories,
            "total_categories": len(categories),
            "total_tickets":    r["especial_pos"],
            "ticket_cost":      round(r["especial_pos"] * TICKET_COST, 2),
            "earning":          0.0,
            "source":           "synthetic",
            "updated_at":       _now_iso(),
        }

        if dry_run:
            if inserted < 5 or i >= total_draws - 3:
                print(
                    f"  [{i+1}/{total_draws}] {fecha}  "
                    f"Especial={r['especial_pos']:>13,}  "
                    f"1ª={r['pos_1a']:>13,}  "
                    f"2ª={r['pos_2a']:>10,}  "
                    f"3ª={r['pos_3a']:>7,}  "
                    f"4ª={r['pos_4a']:>5,}  "
                    f"5ª={r['pos_5a']:>4,}"
                )
        else:
            compare_coll.replace_one(
                {"current_id": current_id, "pre_id": pre_id},
                doc,
                upsert=True,
            )

        inserted += 1
        if not dry_run and inserted % 300 == 0:
            print(f"  [{i+1}/{total_draws}] inserted={inserted} skipped={skipped}")

    print(f"\nDone. inserted={inserted} skipped={skipped} dry_run={dry_run}")
    client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic La Primitiva compare results for historical draws."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Print sample rows without writing to DB")
    parser.add_argument("--year", type=int, default=None,
                        help="Only process draws from this year (e.g. --year 2004)")
    args = parser.parse_args()
    generate_synthetic_results(dry_run=args.dry_run, year_filter=args.year)


if __name__ == "__main__":
    main()
