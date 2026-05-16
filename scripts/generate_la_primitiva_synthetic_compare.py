"""
Generate synthetic compare results for La Primitiva historical draws.

For draws that don't have a real compare result, this script inserts
estimated/synthetic data into la_primitiva_compare_results.

The synthetic values are based on:
- Real observed range from actual results (jackpot ~54M-136M out of 139M)
- A realistic improvement trend: older draws have worse positions,
  newer draws have better positions (simulating model learning)
- Realistic ratios between prize categories

Only inserts for draws that DON'T already have a real result.
Safe to run multiple times — skips existing results.

Usage:
    python3 scripts/generate_la_primitiva_synthetic_compare.py
    python3 scripts/generate_la_primitiva_synthetic_compare.py --dry-run
"""

from __future__ import annotations

import argparse
import os
import random
from datetime import datetime, timezone
from typing import Optional

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

TOTAL_TICKETS = 139_838_160  # C(49,6) * 10
TICKET_COST   = 1.0          # €1 per La Primitiva ticket

# Prize amounts (approximate averages from real escrutinio data)
PRIZE_6_REIN  = 6_000_000.0   # Especial: 6 + reintegro
PRIZE_6       = 3_000_000.0   # 1ª: 6 mains
PRIZE_5_C     = 50_000.0      # 2ª: 5 + complementario
PRIZE_5       = 2_000.0       # 3ª: 5 mains
PRIZE_4       = 80.0          # 4ª: 4 mains
PRIZE_3       = 10.0          # 5ª: 3 mains


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _synthetic_jackpot_position(source_index: int, total_draws: int, rng: random.Random) -> int:
    """
    Generate a realistic jackpot position.

    Early draws (freq/gap only): position near the worst end (~120M-138M)
    Recent draws (ML model):     position near the best end  (~50M-80M)
    Smooth improvement trend with noise.
    """
    # Progress 0.0 (first draw) → 1.0 (latest draw)
    progress = source_index / max(total_draws - 1, 1)

    # Worst position range: 120M-138M (early draws)
    # Best position range:  50M-80M  (recent draws)
    worst_min, worst_max = 120_000_000, 138_000_000
    best_min,  best_max  = 50_000_000,  80_000_000

    # Interpolate
    pos_min = int(worst_min + progress * (best_min  - worst_min))
    pos_max = int(worst_max + progress * (best_max  - worst_max))

    # Add noise (±10% of range)
    noise = int((pos_max - pos_min) * 0.1)
    pos = rng.randint(max(pos_min - noise, 1), min(pos_max + noise, TOTAL_TICKETS - 1))
    return pos


def _synthetic_positions(jackpot_pos: int, rng: random.Random) -> dict:
    """
    Generate realistic 2nd, 3rd, 4th prize positions based on jackpot position.

    Observed ratios from real data:
      pos_2th ≈ jackpot_pos * 0.04 - 0.06
      pos_3th ≈ jackpot_pos * 0.0002 - 0.0003
      pos_4th ≈ jackpot_pos * 0.000004 - 0.000006
    """
    ratio_2 = rng.uniform(0.04, 0.06)
    ratio_3 = rng.uniform(0.00020, 0.00030)
    ratio_4 = rng.uniform(0.000004, 0.000006)

    pos_2th = max(1, int(jackpot_pos * ratio_2))
    pos_3th = max(1, int(jackpot_pos * ratio_3))
    pos_4th = max(1, int(jackpot_pos * ratio_4))

    return {"pos_2th": pos_2th, "pos_3th": pos_3th, "pos_4th": pos_4th}


def _synthetic_categories(jackpot_pos: int, pos_2th: int, pos_3th: int, pos_4th: int) -> list:
    """Build categories array matching the real compare result structure."""
    # Counts based on position ratios (approximate)
    count_6_rein = 1
    count_6      = max(1, jackpot_pos // 139_838_160 + 1)
    count_5_c    = max(1, pos_2th // 1_000_000 + 1)
    count_5      = max(1, pos_3th // 10_000 + 1)
    count_4      = max(1, pos_4th // 100 + 1)
    count_3      = max(1, pos_4th * 10)

    return [
        {"category": "Especial(6+R)", "main_hits": 6, "reintegro_hit": 1,
         "first_position": 1,         "count": count_6_rein},
        {"category": "1ª(6)",         "main_hits": 6, "reintegro_hit": 0,
         "first_position": jackpot_pos, "count": count_6},
        {"category": "2ª(5+C)",       "main_hits": 5, "reintegro_hit": 0,
         "first_position": pos_2th,   "count": count_5_c},
        {"category": "3ª(5)",         "main_hits": 5, "reintegro_hit": 0,
         "first_position": pos_3th,   "count": count_5},
        {"category": "4ª(4)",         "main_hits": 4, "reintegro_hit": 0,
         "first_position": pos_4th,   "count": count_4},
        {"category": "5ª(3)",         "main_hits": 3, "reintegro_hit": 0,
         "first_position": pos_4th * 5, "count": count_3},
    ]


def generate_synthetic_results(dry_run: bool = False) -> None:
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    feature_coll = db["la_primitiva_feature"]
    compare_coll = db["la_primitiva_compare_results"]

    # Load all feature rows in chronological order
    rows = list(feature_coll.find(
        {},
        projection={"id_sorteo": 1, "pre_id_sorteo": 1, "fecha_sorteo": 1, "source_index": 1},
        sort=[("source_index", ASCENDING)],
    ))
    total_draws = len(rows)
    print(f"La Primitiva: {total_draws} draws found")

    # Count existing real results
    existing_real = compare_coll.count_documents(
        {"jackpot_position": {"$exists": True, "$ne": None}}
    )
    print(f"Existing real results: {existing_real}")

    rng = random.Random(42)  # fixed seed for reproducibility
    inserted = 0
    skipped  = 0

    for i, row in enumerate(rows):
        current_id = str(row.get("id_sorteo") or "").strip()
        pre_id     = str(row.get("pre_id_sorteo") or "").strip()
        fecha      = str(row.get("fecha_sorteo") or "").strip().split(" ")[0]
        src_idx    = int(row.get("source_index", i))

        if not current_id or not pre_id:
            skipped += 1
            continue

        # Skip if real result already exists
        existing = compare_coll.find_one({"current_id": current_id, "pre_id": pre_id})
        if existing and existing.get("jackpot_position") is not None:
            skipped += 1
            continue

        # Generate synthetic result
        jackpot_pos = _synthetic_jackpot_position(src_idx, total_draws, rng)
        positions   = _synthetic_positions(jackpot_pos, rng)
        categories  = _synthetic_categories(
            jackpot_pos,
            positions["pos_2th"],
            positions["pos_3th"],
            positions["pos_4th"],
        )

        total_tickets = jackpot_pos
        ticket_cost   = round(total_tickets * TICKET_COST, 2)

        # Estimate earnings from smaller prizes up to jackpot position
        earning = round(
            positions["pos_2th"] * PRIZE_5_C / 1_000_000 +
            positions["pos_3th"] * PRIZE_5   / 1_000 +
            positions["pos_4th"] * PRIZE_4,
            2
        )

        doc = {
            "current_id":       current_id,
            "pre_id":           pre_id,
            "date":             fecha,
            "jackpot_position": jackpot_pos,
            "pos_2th":          positions["pos_2th"],
            "pos_3th":          positions["pos_3th"],
            "pos_4th":          positions["pos_4th"],
            "categories":       categories,
            "total_categories": len(categories),
            "total_tickets":    total_tickets,
            "ticket_cost":      ticket_cost,
            "earning":          earning,
            "source":           "synthetic",
            "updated_at":       _now_iso(),
        }

        if dry_run:
            if inserted < 5:
                print(f"  [{i+1}/{total_draws}] DRY RUN: {current_id} ({fecha}) jackpot=#{jackpot_pos:,}")
        else:
            compare_coll.replace_one(
                {"current_id": current_id, "pre_id": pre_id},
                doc,
                upsert=True,
            )

        inserted += 1
        if inserted % 200 == 0:
            print(f"  [{i+1}/{total_draws}] inserted={inserted} skipped={skipped}")

    print(f"\nDone. inserted={inserted} skipped={skipped} dry_run={dry_run}")
    client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic La Primitiva compare results for historical draws."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be inserted without writing to DB")
    args = parser.parse_args()
    generate_synthetic_results(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
