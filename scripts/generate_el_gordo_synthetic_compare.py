"""
Generate synthetic compare results for El Gordo historical draws.

Structure matches real data from el_gordo_compare_results:
  jackpot_position  (5+clave) — 1st prize, highest position
  pos_2th           (5+0)     — scalar
  pos_3th           (4+1)     — scalar
  pos_4th           (4+0)     — scalar
  categories        list      — all prize tiers with main_hits/clave_hit/first_position/count

Total ticket space: C(54,5) × 10 = 31,625,100

Jackpot range: 2,000,000 – 30,000,000  (6.3% – 94.9% of total)
  Trend: oldest draws ~28M avg, latest draws ~5M avg
  Wide variance ±10M per draw for realistic noise.

Sub-prize ratios derived from real La Primitiva data (same proportional structure):
  2th (5+0)  : 94.05–95.37% of jackpot
  3th (4+1)  : 4.53–7.20%   of jackpot
  4th (4+0)  : 0.035–0.147% of jackpot
  5th (3+1)  : 0.00041–0.00105% of jackpot
  6th (3+0)  : 0.000040–0.000129% of jackpot
  7th (2+1)  : cascading fraction of 6th
  8th (2+0)  : cascading fraction of 7th

Only inserts for draws that do NOT already have a real result.
Safe to run multiple times (idempotent via replace_one + upsert).

Usage:
    python3 scripts/generate_el_gordo_synthetic_compare.py --dry-run
    python3 scripts/generate_el_gordo_synthetic_compare.py
    python3 scripts/generate_el_gordo_synthetic_compare.py --year 2004
"""

from __future__ import annotations

import argparse
import os
import random
from datetime import datetime, timezone

from pymongo import ASCENDING, MongoClient

# ── env loading ───────────────────────────────────────────────────────────────
_scripts_dir = os.path.dirname(os.path.abspath(__file__))
try:
    from dotenv import load_dotenv
    for _path in [
        os.path.join(_scripts_dir, "..", "backend", ".env"),
        os.path.join(_scripts_dir, "..", ".env"),
    ]:
        if os.path.isfile(_path):
            load_dotenv(_path)
            break
except ImportError:
    pass

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "lottery")

# ── constants ─────────────────────────────────────────────────────────────────
# C(54,5) * 10 = 3,162,510 * 10 = 31,625,100
TOTAL       = 31_625_100   # C(54,5) × 10 — total El Gordo tickets
TICKET_COST = 1.50         # €1.50 per ticket


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _generate_row(source_index: int, total_draws: int, rng: random.Random) -> dict:
    """
    Generate one synthetic compare result row for El Gordo.

    Total ticket space: C(54,5) × 10 = 31,625,100

    Jackpot position range: 2,000,000 – 30,000,000  (6.3% – 94.9% of total)
      Trend:
        oldest draws : center ~28,000,000  (model near top 88% of total)
        latest draws : center ~5,000,000   (model improved, jackpot in top 16%)
      Variance: ±10,000,000 around center
      Floor: 2,000,000  /  Ceiling: 30,000,000

    Sub-prize ratios from real La Primitiva data (same proportional structure):
      2th (5+0)  : 94.05–95.37% of jackpot
      3th (4+1)  : 4.53–7.20%   of jackpot
      4th (4+0)  : 0.035–0.147% of jackpot
      5th (3+1)  : 0.00041–0.00105% of jackpot
      6th (3+0)  : 0.000040–0.000129% of jackpot
      7th (2+1)  : cascading fraction of 6th
      8th (2+0)  : cascading fraction of 7th
    """
    progress = source_index / max(total_draws - 1, 1)

    # Center shifts from 28M → 5M as model improves over time
    center = int(28_000_000 - progress * 23_000_000)

    lo = max(2_000_000,  center - 10_000_000)
    hi = min(30_000_000, center + 10_000_000)
    jackpot_pos = rng.randint(lo, hi)

    # 2th (5+0): 94.05–95.37% of jackpot
    pos_2th = max(1, int(jackpot_pos * rng.uniform(0.9405, 0.9537)))

    # 3th (4+1): 4.53–7.20% of jackpot
    pos_3th = max(1, int(jackpot_pos * rng.uniform(0.0453, 0.0720)))

    # 4th (4+0): 0.035–0.147% of jackpot
    pos_4th = max(1, int(jackpot_pos * rng.uniform(0.000348, 0.001469)))

    # 5th (3+1): 0.00041–0.00105% of jackpot
    pos_5th = max(1, int(jackpot_pos * rng.uniform(0.00000415, 0.00001046)))

    # 6th (3+0): 0.000040–0.000129% of jackpot
    pos_6th = max(1, int(jackpot_pos * rng.uniform(0.000000402, 0.000001286)))

    # 7th (2+1): cascading fraction of 6th
    pos_7th = max(1, int(pos_6th * rng.uniform(0.30, 0.80)))

    # 8th (2+0): cascading fraction of 7th
    pos_8th = max(1, int(pos_7th * rng.uniform(0.20, 0.70)))

    return {
        "jackpot_pos": jackpot_pos,
        "pos_2th":     pos_2th,
        "pos_3th":     pos_3th,
        "pos_4th":     pos_4th,
        "pos_5th":     pos_5th,
        "pos_6th":     pos_6th,
        "pos_7th":     pos_7th,
        "pos_8th":     pos_8th,
    }


def _build_categories(r: dict) -> list:
    """
    Build the categories list matching the real El Gordo compare result format.
    Each entry has: category, main_hits, clave_hit, first_position, count.

    El Gordo prize tiers (main_hits + clave_hit):
      5+1  jackpot
      5+0  2nd
      4+1  3rd
      4+0  4th
      3+1  5th
      3+0  6th
      2+1  7th
      2+0  8th
    """
    tiers = [
        # (main_hits, clave_hit, label,   position_key, count_divisor)
        (5, 1, "5+1", "jackpot_pos", 1),
        (5, 0, "5+0", "pos_2th",     1),
        (4, 1, "4+1", "pos_3th",     1),
        (4, 0, "4+0", "pos_4th",     50),
        (3, 1, "3+1", "pos_5th",     200),
        (3, 0, "3+0", "pos_6th",     1_000),
        (2, 1, "2+1", "pos_7th",     5_000),
        (2, 0, "2+0", "pos_8th",     20_000),
    ]
    categories = []
    for main_hits, clave_hit, label, pos_key, div in tiers:
        pos = r[pos_key]
        count = max(1, pos // div)
        categories.append({
            "category":      label,
            "main_hits":     main_hits,
            "clave_hit":     clave_hit,
            "first_position": pos,
            "count":         count,
        })
    return categories


def generate_synthetic_results(dry_run: bool = False, year_filter: int | None = None) -> None:
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    feature_coll = db["el_gordo_feature"]
    compare_coll = db["el_gordo_compare_results"]

    # Load all draws in chronological order (oldest first = source_index ASC)
    rows = list(feature_coll.find(
        {},
        projection={
            "id_sorteo":     1,
            "pre_id_sorteo": 1,
            "fecha_sorteo":  1,
            "source_index":  1,
        },
        sort=[("source_index", ASCENDING)],
    ))
    total_draws = len(rows)
    print(f"El Gordo: {total_draws} draws found in el_gordo_feature")
    print(f"Total ticket space: {TOTAL:,}  (C(54,5) × 10)")

    existing_real = compare_coll.count_documents(
        {"jackpot_position": {"$exists": True, "$ne": None}, "source": {"$ne": "synthetic"}}
    )
    print(f"Existing real (non-synthetic) results: {existing_real}")

    rng = random.Random(42)   # fixed seed — reproducible results
    inserted = skipped = 0

    for i, row in enumerate(rows):
        current_id = str(row.get("id_sorteo")     or "").strip()
        pre_id     = str(row.get("pre_id_sorteo") or "").strip()
        fecha      = str(row.get("fecha_sorteo")  or "").strip().split(" ")[0]
        src_idx    = int(row.get("source_index", i))

        if not current_id or not pre_id:
            skipped += 1
            continue

        # Filter by year if specified
        if year_filter is not None and not fecha.startswith(str(year_filter)):
            skipped += 1
            continue

        # Skip if a real (non-synthetic) result already exists
        existing = compare_coll.find_one({"current_id": current_id, "pre_id": pre_id})
        if (
            existing
            and existing.get("jackpot_position") is not None
            and existing.get("source") != "synthetic"
        ):
            skipped += 1
            continue

        # Generate synthetic positions
        r = _generate_row(src_idx, total_draws, rng)
        categories = _build_categories(r)

        doc = {
            "current_id":       current_id,
            "pre_id":           pre_id,
            "date":             fecha,
            # jackpot_position = 1st prize (5+clave)
            "jackpot_position": r["jackpot_pos"],
            # pos_2th/3th/4th are SCALARS (matches real El Gordo compare format)
            "pos_2th":          r["pos_2th"],
            "pos_3th":          r["pos_3th"],
            "pos_4th":          r["pos_4th"],
            "categories":       categories,
            "total_categories": len(categories),
            "ticket_cost":      round(r["jackpot_pos"] * TICKET_COST, 2),
            "source":           "synthetic",
            "updated_at":       _now_iso(),
        }

        if dry_run:
            if inserted < 5 or i >= total_draws - 3:
                print(
                    f"  [{i+1}/{total_draws}] {fecha}  "
                    f"1th={r['jackpot_pos']:>10,}  "
                    f"2th={r['pos_2th']:>10,}  "
                    f"3th={r['pos_3th']:>8,}  "
                    f"4th={r['pos_4th']:>7,}  "
                    f"5th={r['pos_5th']:>5,}  "
                    f"6th={r['pos_6th']:>4,}"
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
        description="Generate synthetic El Gordo compare results for historical draws."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print sample rows without writing to DB",
    )
    parser.add_argument(
        "--year", type=int, default=None,
        help="Only process draws from this year (e.g. --year 2004)",
    )
    args = parser.parse_args()
    generate_synthetic_results(dry_run=args.dry_run, year_filter=args.year)


if __name__ == "__main__":
    main()
