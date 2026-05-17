"""
Generate synthetic compare results for Euromillones historical draws.

Structure matches real data from euromillones_compare_results:
  jackpot_position  (5+2) — 1th prize, highest position
  second_positions  list  — (5+1) 2th prize
  third_positions   list  — (5+0) 3th prize
  fourth_positions  list  — (4+2) 4th prize
  categories        list  — all 13 prize tiers

Total ticket space: C(50,5) × C(12,2) = 2,118,760

Trend: 2004 = worst positions (near 2.1M), 2026 = best (~1.36M)
       Proportions mirror La Primitiva (same % of total ticket space).
       Wide variance per draw — any single draw can be anywhere in 455K–2.1M.

Only inserts for draws that do NOT already have a real result.
Safe to run multiple times (idempotent via replace_one + upsert).

Usage:
    python3 scripts/generate_euromillones_synthetic_compare.py --dry-run
    python3 scripts/generate_euromillones_synthetic_compare.py
    python3 scripts/generate_euromillones_synthetic_compare.py --year 2004
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
    # dotenv not available — rely on environment variables or defaults
    pass

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "lottery")

# ── constants ─────────────────────────────────────────────────────────────────
TOTAL         = 2_118_760   # C(50,5) × C(12,2) — total Euromillones tickets
TICKET_COST   = 2.50        # €2.50 per ticket

# Canonical 13-category order matching _EMIL_CATEGORY_ORDER in main.py
_CATEGORY_ORDER = [
    (5, 2), (5, 1), (5, 0), (4, 2), (4, 1), (4, 0),
    (3, 2), (3, 1), (3, 0), (2, 2), (2, 1), (1, 2), (2, 0),
]
_CATEGORY_LABELS = [
    "1th(5+2)", "2th(5+1)", "3th(5+0)", "4th(4+2)", "5th(4+1)", "6th(4+0)",
    "7th(3+2)", "8th(3+1)", "9th(3+0)", "10th(2+2)", "11th(2+1)", "12th(1+2)", "13th(2+0)",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _generate_row(source_index: int, total_draws: int, rng: random.Random) -> dict:
    """
    Generate one synthetic compare result row for Euromillones.

    Proportions mirror La Primitiva (same % of total ticket space):
      La Primitiva:   center 89.4% → 64.4% of total,  variance ±35.8%,  floor 21.5%
      Euromillones:   center 89.4% → 64.4% of total,  variance ±35.8%,  floor 21.5%

    Translated to absolute values (TOTAL = 2,118,760):
      center start : ~1,894,000  (89.4% of 2,118,760)
      center end   : ~1,364,000  (64.4% of 2,118,760)
      variance     :   ±758,000  (35.8% of 2,118,760)
      floor        :    455,000  (21.5% of 2,118,760)

    Result range: ~455K – 2,118,760  (same 29%–100% spread as La Primitiva)
    Avg 2004: ~1,894,000  →  Avg 2026: ~1,364,000
    """
    progress = source_index / max(total_draws - 1, 1)

    # Center shifts from ~1.894M → ~1.364M as model improves over time
    # (mirrors La Primitiva: 125M → 90M, same proportional drop)
    center = int(1_894_000 - progress * 530_000)

    lo = max(455_000,   center - 758_000)
    hi = min(2_118_000, center + 758_000)
    jackpot_pos = rng.randint(lo, hi)

    # 2th (5+1): 5–25% of jackpot position
    pos_2th = max(1, int(jackpot_pos * rng.uniform(0.05, 0.25)))

    # 3th (5+0): 0.5–5% of jackpot position
    pos_3th = max(1, int(jackpot_pos * rng.uniform(0.005, 0.05)))

    # 4th (4+2): slightly less than 3th — 0.3–3% of jackpot
    pos_4th = max(1, int(jackpot_pos * rng.uniform(0.003, 0.03)))

    # 5th (4+1): 0.02–0.3% of jackpot
    pos_5th = max(1, int(jackpot_pos * rng.uniform(0.0002, 0.003)))

    # 6th (4+0): 0.005–0.05% of jackpot
    pos_6th = max(1, int(jackpot_pos * rng.uniform(0.00005, 0.0005)))

    # 7th (3+2): 0.002–0.02% of jackpot
    pos_7th = max(1, int(jackpot_pos * rng.uniform(0.00002, 0.0002)))

    # 8th (3+1): slightly less than 7th
    pos_8th = max(1, int(pos_7th * rng.uniform(0.3, 0.8)))

    # 9th (3+0): very small
    pos_9th = max(1, int(pos_8th * rng.uniform(0.2, 0.6)))

    # 10th (2+2): very small
    pos_10th = max(1, int(pos_9th * rng.uniform(0.3, 0.8)))

    # 11th (2+1): tiny
    pos_11th = max(1, int(pos_10th * rng.uniform(0.2, 0.6)))

    # 12th (1+2): tiny
    pos_12th = max(1, int(pos_11th * rng.uniform(0.3, 0.9)))

    # 13th (2+0): tiny
    pos_13th = max(1, int(pos_11th * rng.uniform(0.2, 0.7)))

    return {
        "jackpot_pos": jackpot_pos,
        "pos_2th":  pos_2th,
        "pos_3th":  pos_3th,
        "pos_4th":  pos_4th,
        "pos_5th":  pos_5th,
        "pos_6th":  pos_6th,
        "pos_7th":  pos_7th,
        "pos_8th":  pos_8th,
        "pos_9th":  pos_9th,
        "pos_10th": pos_10th,
        "pos_11th": pos_11th,
        "pos_12th": pos_12th,
        "pos_13th": pos_13th,
    }


def _build_categories(r: dict) -> list:
    """
    Build the 13-category list matching the real compare result format.
    count is estimated from position (higher position = more tickets with that prize).
    """
    positions = [
        r["jackpot_pos"],
        r["pos_2th"],
        r["pos_3th"],
        r["pos_4th"],
        r["pos_5th"],
        r["pos_6th"],
        r["pos_7th"],
        r["pos_8th"],
        r["pos_9th"],
        r["pos_10th"],
        r["pos_11th"],
        r["pos_12th"],
        r["pos_13th"],
    ]
    # Rough count divisors — lower prizes appear more frequently
    count_divisors = [
        1,          # 1th (5+2)  — jackpot, 1 ticket
        1,          # 2th (5+1)
        1,          # 3th (5+0)
        1,          # 4th (4+2)
        50,         # 5th (4+1)
        200,        # 6th (4+0)
        500,        # 7th (3+2)
        1_000,      # 8th (3+1)
        3_000,      # 9th (3+0)
        5_000,      # 10th (2+2)
        10_000,     # 11th (2+1)
        15_000,     # 12th (1+2)
        20_000,     # 13th (2+0)
    ]
    categories = []
    for i, ((hm, hs), label, pos, div) in enumerate(
        zip(_CATEGORY_ORDER, _CATEGORY_LABELS, positions, count_divisors)
    ):
        count = max(1, pos // div)
        categories.append({
            "category": label,
            "count":    count,
            "earning":  0.0,   # synthetic — no real prize data
        })
    return categories


def generate_synthetic_results(dry_run: bool = False, year_filter: int | None = None) -> None:
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    feature_coll = db["euromillones_feature"]
    compare_coll = db["euromillones_compare_results"]

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
    print(f"Euromillones: {total_draws} draws found in euromillones_feature")

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
            # jackpot_position = 1th (5+2)
            "jackpot_position": r["jackpot_pos"],
            # second/third/fourth_positions are LISTS (matches real compare result format)
            "second_positions": [r["pos_2th"]],
            "third_positions":  [r["pos_3th"]],
            "fourth_positions": [r["pos_4th"]],
            "categories":       categories,
            "total_tickets":    r["jackpot_pos"],
            "ticket_cost":      round(r["jackpot_pos"] * TICKET_COST, 2),
            "earning":          0.0,
            "source":           "synthetic",
            "updated_at":       _now_iso(),
        }

        if dry_run:
            if inserted < 5 or i >= total_draws - 3:
                print(
                    f"  [{i+1}/{total_draws}] {fecha}  "
                    f"1th={r['jackpot_pos']:>9,}  "
                    f"2th={r['pos_2th']:>8,}  "
                    f"3th={r['pos_3th']:>7,}  "
                    f"4th={r['pos_4th']:>6,}  "
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
        description="Generate synthetic Euromillones compare results for historical draws."
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
