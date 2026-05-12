"""
Backfill probability snapshots for ALL historical draws — direct MongoDB approach.

Instead of calling the API (which trains ML models, slow), this script reads
frequency+gap data directly from feature collections and computes initial
probability scores. Fast: ~1 second per draw, ~2 hours for all 6400 draws.

For draws with enough history (>= MIN_DRAWS_FOR_ML), it also reads any
already-computed ML probabilities from train_progress if available.

Usage:
    python3 scripts/backfill_rankings.py
    python3 scripts/backfill_rankings.py --lottery euromillones
    python3 scripts/backfill_rankings.py --skip-existing   (default: True)
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

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

LOTTERY_CONFIGS = {
    "euromillones": {
        "feature_collection":    "euromillones_feature",
        "progress_collection":   "euromillones_train_progress",
        "draw_probs_collection": "euromillones_draw_probs",
        "secondary_field":       "stars_probs",
        "ml_secondary_field":    "stars_probs",
        "mains_count":           50,
        "secondary_count":       12,
        "secondary_offset":      50,
        "secondary_base":        1,
    },
    "el_gordo": {
        "feature_collection":    "el_gordo_feature",
        "progress_collection":   "el_gordo_train_progress",
        "draw_probs_collection": "el_gordo_draw_probs",
        "secondary_field":       "clave_probs",
        "ml_secondary_field":    "clave_probs",
        "mains_count":           54,
        "secondary_count":       10,
        "secondary_offset":      54,
        "secondary_base":        0,
    },
    "la_primitiva": {
        "feature_collection":    "la_primitiva_feature",
        "progress_collection":   "la_primitiva_train_progress",
        "draw_probs_collection": "la_primitiva_draw_probs",
        "secondary_field":       "rein_probs",
        "ml_secondary_field":    "reintegro_probs",
        "mains_count":           49,
        "secondary_count":       10,
        "secondary_offset":      98,
        "secondary_base":        0,
    },
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _freq_gap_probs(
    frequency: List, gap: List,
    offset: int, count: int, base: int,
) -> Dict[str, float]:
    """Compute probability scores from frequency+gap data."""
    freqs = [int(frequency[offset+i]) if offset+i < len(frequency) else 0 for i in range(count)]
    gaps  = [gap[offset+i] if offset+i < len(gap) else None for i in range(count)]
    mf = max(freqs) if any(f > 0 for f in freqs) else 1
    vg = [g for g in gaps if g is not None]
    mg = max(vg) + 1 if vg else 1
    return {
        str(base + i): max(0.4 * freqs[i]/mf + 0.6*(0.0 if gaps[i] is None else 1.0 - gaps[i]/mg), 1e-6)
        for i in range(count)
    }


def _probs_from_list(probs_list: List[Dict]) -> Dict[str, float]:
    """Convert [{number: n, p: p}] → {str(n): p}."""
    return {
        str(int(x["number"])): float(x.get("p", 0.0))
        for x in (probs_list or [])
        if x.get("number") is not None
    }


def backfill_lottery(
    db,
    lottery_key: str,
    skip_existing: bool = True,
) -> Dict[str, Any]:
    cfg = LOTTERY_CONFIGS[lottery_key]
    feat_coll     = db[cfg["feature_collection"]]
    prog_coll     = db[cfg["progress_collection"]]
    probs_coll    = db[cfg["draw_probs_collection"]]

    rows = list(feat_coll.find(
        {},
        projection={"id_sorteo": 1, "fecha_sorteo": 1, "source_index": 1,
                    "frequency": 1, "gap": 1},
        sort=[("source_index", ASCENDING)],
    ))
    total = len(rows)
    print(f"\n[{lottery_key}] {total} draws in {cfg['feature_collection']}")

    processed = skipped = 0

    for i, row in enumerate(rows):
        draw_id = str(row.get("id_sorteo") or "").strip()
        fecha   = str(row.get("fecha_sorteo") or "").strip().split(" ")[0]
        src_idx = int(row.get("source_index", i))

        if not draw_id:
            skipped += 1
            continue

        if skip_existing and probs_coll.count_documents({"draw_id": draw_id}) > 0:
            skipped += 1
            if skipped % 100 == 0:
                print(f"  [{i+1}/{total}] skipping already-processed draws...")
            continue

        frequency = list(row.get("frequency") or [])
        gap       = list(row.get("gap") or [])

        # Always compute freq+gap probs as baseline
        mains_probs = _freq_gap_probs(frequency, gap, 0, cfg["mains_count"], 1)
        sec_probs   = _freq_gap_probs(
            frequency, gap,
            cfg["secondary_offset"], cfg["secondary_count"], cfg["secondary_base"],
        )
        source = "freq_gap"

        # If ML probs already computed for this draw, use them instead
        prog_doc = prog_coll.find_one({"cutoff_draw_id": draw_id})
        if prog_doc and prog_doc.get("mains_probs"):
            ml_mains = _probs_from_list(prog_doc.get("mains_probs") or [])
            ml_sec   = _probs_from_list(
                prog_doc.get(cfg["ml_secondary_field"]) or
                prog_doc.get("reintegro_probs") or []
            )
            if ml_mains:
                mains_probs = ml_mains
                sec_probs   = ml_sec
                source = "ml_model"

        probs_coll.replace_one(
            {"draw_id": draw_id},
            {
                "draw_id":              draw_id,
                "draw_date":            fecha,
                "saved_at":             _now_iso(),
                "mains_probs":          mains_probs,
                cfg["secondary_field"]: sec_probs,
                "source":               source,
            },
            upsert=True,
        )
        processed += 1

        if processed % 50 == 0 or i < 20:
            print(f"  [{i+1}/{total}] draw={draw_id} ({fecha}) source={source} ✓")

    print(f"\n[{lottery_key}] Done — total={total} processed={processed} skipped={skipped}")
    return {"lottery": lottery_key, "total": total, "processed": processed, "skipped": skipped}


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill probability snapshots for all historical draws.")
    parser.add_argument("--lottery", choices=["euromillones", "el_gordo", "la_primitiva", "all"], default="all")
    parser.add_argument("--skip-existing", action="store_true", default=True)
    parser.add_argument("--no-skip-existing", dest="skip_existing", action="store_false")
    args = parser.parse_args()

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    lotteries = list(LOTTERY_CONFIGS.keys()) if args.lottery == "all" else [args.lottery]

    print(f"Backfill draw_probs — lotteries={lotteries} skip_existing={args.skip_existing}")
    print("=" * 60)

    results = []
    for lottery_key in lotteries:
        result = backfill_lottery(db, lottery_key, skip_existing=args.skip_existing)
        results.append(result)

    print("\n" + "=" * 60)
    print("SUMMARY:")
    for r in results:
        print(f"  {r['lottery']:15s}  total={r['total']:5d}  processed={r['processed']:5d}  skipped={r['skipped']:5d}")

    client.close()


if __name__ == "__main__":
    main()
