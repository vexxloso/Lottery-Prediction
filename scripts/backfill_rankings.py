"""
Backfill ranking snapshots for ALL historical draws.

For each lottery, reads every draw from the feature collection in chronological
order (source_index ASC) and:
  1. Runs the train pipeline for that draw's cutoff_draw_id.
  2. Calls /compare/full-wheel/reorder to save the ranking snapshot to DB.

This gives you a complete ranking history from the very first draw (e.g. 2004)
up to today.

Usage:
    # Backfill all 3 lotteries
    python3 scripts/backfill_rankings.py --api-url http://localhost:8000

    # Backfill only one lottery
    python3 scripts/backfill_rankings.py --lottery euromillones

    # Resume from a specific draw (skip draws already processed)
    python3 scripts/backfill_rankings.py --skip-existing

    # Dry run — just print what would be processed
    python3 scripts/backfill_rankings.py --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from pymongo import ASCENDING, MongoClient
from dotenv import load_dotenv

# Load env
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

POLL_SECONDS             = 5
PIPELINE_TIMEOUT_SECONDS = 45 * 60   # 45 min per draw
COMPARE_TIMEOUT_SECONDS  = 10 * 60   # 10 min per compare

LOTTERY_CONFIGS = {
    "euromillones": {
        "feature_collection":   "euromillones_feature",
        "draw_probs_collection": "euromillones_draw_probs",
        "api_slug":             "euromillones",
        "secondary_field":      "stars_probs",
        "mains_count":          50,
        "secondary_count":      12,
        "secondary_offset":     50,
    },
    "el_gordo": {
        "feature_collection":   "el_gordo_feature",
        "draw_probs_collection": "el_gordo_draw_probs",
        "api_slug":             "el-gordo",
        "secondary_field":      "clave_probs",
        "mains_count":          54,
        "secondary_count":      10,
        "secondary_offset":     54,
    },
    "la_primitiva": {
        "feature_collection":   "la_primitiva_feature",
        "draw_probs_collection": "la_primitiva_draw_probs",
        "api_slug":             "la-primitiva",
        "secondary_field":      "rein_probs",
        "mains_count":          49,
        "secondary_count":      10,
        "secondary_offset":     98,
    },
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _request_json(
    session: requests.Session,
    method: str,
    url: str,
    timeout: int = 60,
) -> Dict[str, Any]:
    res = session.request(method, url, timeout=timeout)
    data: Dict[str, Any] = {}
    try:
        data = res.json() if res.content else {}
    except Exception:
        pass
    if not res.ok:
        detail = data.get("detail") if isinstance(data, dict) else None
        raise RuntimeError(
            f"{method} {url} → {res.status_code}: {detail or res.text[:200]}"
        )
    return data


def _wait_pipeline(
    session: requests.Session,
    base_url: str,
    api_slug: str,
    cutoff_draw_id: str,
) -> None:
    url   = f"{base_url}/api/{api_slug}/train/progress?cutoff_draw_id={cutoff_draw_id}"
    start = time.time()
    while True:
        data     = _request_json(session, "GET", url)
        progress = data.get("progress") or {}
        status   = str(progress.get("pipeline_status") or "").lower()
        if status == "done" or progress.get("rules_applied") is True:
            return
        if status == "error":
            raise RuntimeError(
                f"Pipeline error for {cutoff_draw_id}: {progress.get('pipeline_error')}"
            )
        if time.time() - start > PIPELINE_TIMEOUT_SECONDS:
            raise RuntimeError(
                f"Pipeline timeout ({PIPELINE_TIMEOUT_SECONDS}s) for {cutoff_draw_id}"
            )
        time.sleep(POLL_SECONDS)


def _ensure_pipeline(
    session: requests.Session,
    base_url: str,
    api_slug: str,
    cutoff_draw_id: str,
) -> None:
    """Run pipeline if not already done for this cutoff_draw_id."""
    # Check if already done
    url  = f"{base_url}/api/{api_slug}/train/progress?cutoff_draw_id={cutoff_draw_id}"
    data = _request_json(session, "GET", url)
    progress = data.get("progress") or {}
    if progress.get("rules_applied") is True:
        return  # already done

    # Trigger pipeline
    trigger_url = f"{base_url}/api/{api_slug}/train/run-pipeline?cutoff_draw_id={cutoff_draw_id}"
    _request_json(session, "POST", trigger_url)
    _wait_pipeline(session, base_url, api_slug, cutoff_draw_id)


def _trigger_reorder(
    session: requests.Session,
    base_url: str,
    api_slug: str,
    current_id: str,
    pre_id: str,
) -> Optional[int]:
    """Call /compare/full-wheel/reorder and return jackpot_position."""
    url  = (
        f"{base_url}/api/{api_slug}/compare/full-wheel/reorder"
        f"?current_id={current_id}&pre_id={pre_id}"
    )
    data = _request_json(session, "POST", url, timeout=COMPARE_TIMEOUT_SECONDS)
    return data.get("jackpot_position")


def _load_all_feature_rows(
    db,
    feature_collection: str,
) -> List[Dict[str, Any]]:
    """Load all feature rows sorted by source_index ASC (chronological order)."""
    coll = db[feature_collection]
    return list(
        coll.find(
            {},
            projection={
                "id_sorteo":     1,
                "pre_id_sorteo": 1,
                "fecha_sorteo":  1,
                "source_index":  1,
            },
        ).sort("source_index", ASCENDING)
    )


def _ranking_exists(db, rankings_collection: str, draw_id: str) -> bool:
    """Return True if a ranking snapshot already exists for this draw_id."""
    return db[rankings_collection].count_documents({"draw_id": draw_id}) > 0


def _save_freq_gap_probs(
    db, lottery_key: str, feature_collection: str,
    draw_probs_collection: str, draw_id: str, fecha: str, source_idx: int,
) -> None:
    """
    For early draws (not enough data for ML), compute probs from frequency+gap
    of the feature row itself and save to draw_probs collection.
    """
    cfg = LOTTERY_CONFIGS[lottery_key]
    doc = db[feature_collection].find_one({"id_sorteo": draw_id})
    if not doc:
        raise RuntimeError(f"Feature row not found for draw_id={draw_id}")

    frequency = list(doc.get("frequency") or [])
    gap       = list(doc.get("gap") or [])
    total     = max(source_idx + 1, 1)

    def _build(offset: int, count: int, base: int) -> dict:
        freqs = [int(frequency[offset+i]) if offset+i < len(frequency) else 0 for i in range(count)]
        gaps  = [gap[offset+i] if offset+i < len(gap) else None for i in range(count)]
        mf = max(freqs) if any(f > 0 for f in freqs) else 1
        vg = [g for g in gaps if g is not None]
        mg = max(vg) + 1 if vg else 1
        return {
            str(base + i): max(0.4 * freqs[i]/mf + 0.6 * (0.0 if gaps[i] is None else 1.0 - gaps[i]/mg), 1e-6)
            for i in range(count)
        }

    mains_probs = _build(0, cfg["mains_count"], 1)
    sec_probs   = _build(cfg["secondary_offset"], cfg["secondary_count"],
                         0 if cfg["secondary_offset"] >= cfg["mains_count"] else 1)

    db[draw_probs_collection].replace_one(
        {"draw_id": draw_id},
        {
            "draw_id":              draw_id,
            "draw_date":            fecha,
            "saved_at":             datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "mains_probs":          mains_probs,
            cfg["secondary_field"]: sec_probs,
            "source":               "freq_gap",
        },
        upsert=True,
    )


def _save_probs_from_progress(
    db, lottery_key: str, draw_probs_collection: str, draw_id: str, fecha: str,
) -> None:
    """
    After pipeline runs, copy mains_probs + secondary_probs from train_progress
    into the draw_probs collection.
    """
    cfg = LOTTERY_CONFIGS[lottery_key]
    progress_coll_map = {
        "euromillones": "euromillones_train_progress",
        "el_gordo":     "el_gordo_train_progress",
        "la_primitiva": "la_primitiva_train_progress",
    }
    progress_coll = db[progress_coll_map[lottery_key]]
    doc = progress_coll.find_one({"cutoff_draw_id": draw_id})
    if not doc:
        raise RuntimeError(f"train_progress not found for draw_id={draw_id}")

    # Get probs — field names differ per lottery
    if lottery_key == "euromillones":
        mains_raw = doc.get("mains_probs") or []
        sec_raw   = doc.get("stars_probs") or []
    elif lottery_key == "el_gordo":
        mains_raw = doc.get("mains_probs") or []
        sec_raw   = doc.get("clave_probs") or []
    else:  # la_primitiva
        mains_raw = doc.get("mains_probs") or []
        sec_raw   = doc.get("reintegro_probs") or []

    if not mains_raw:
        raise RuntimeError(f"No mains_probs in train_progress for draw_id={draw_id}")

    mains_probs = {str(int(x["number"])): float(x.get("p", 0.0)) for x in mains_raw if x.get("number") is not None}
    sec_probs   = {str(int(x["number"])): float(x.get("p", 0.0)) for x in sec_raw   if x.get("number") is not None}

    db[draw_probs_collection].replace_one(
        {"draw_id": draw_id},
        {
            "draw_id":              draw_id,
            "draw_date":            fecha,
            "saved_at":             datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "mains_probs":          mains_probs,
            cfg["secondary_field"]: sec_probs,
            "source":               "ml_model",
        },
        upsert=True,
    )


# ── Main backfill logic ───────────────────────────────────────────────────────

def backfill_lottery(
    session: requests.Session,
    db,
    base_url: str,
    lottery_key: str,
    skip_existing: bool = True,
    dry_run: bool = False,
) -> Dict[str, Any]:
    cfg                = LOTTERY_CONFIGS[lottery_key]
    feature_collection = cfg["feature_collection"]
    draw_probs_collection = cfg["draw_probs_collection"]
    api_slug           = cfg["api_slug"]

    rows = _load_all_feature_rows(db, feature_collection)
    total = len(rows)
    print(f"\n[{lottery_key}] {total} draws found in {feature_collection}")

    if total == 0:
        print(f"[{lottery_key}] No draws — skipping. Run build_{lottery_key}_feature.py first.")
        return {"lottery": lottery_key, "total": 0, "processed": 0, "skipped": 0, "errors": 0}

    # Minimum draws needed before ML training is reliable
    MIN_DRAWS_FOR_ML = 10

    processed = 0
    skipped   = 0
    errors    = 0

    for i, row in enumerate(rows):
        current_id = str(row.get("id_sorteo")     or "").strip()
        pre_id     = str(row.get("pre_id_sorteo") or "").strip()
        fecha      = str(row.get("fecha_sorteo")  or "").strip()
        source_idx = int(row.get("source_index",  i))

        if not current_id:
            skipped += 1
            continue

        # Skip if prob snapshot already saved for this draw
        if skip_existing and db[draw_probs_collection].count_documents({"draw_id": current_id}) > 0:
            print(f"  [{i+1}/{total}] draw={current_id} ({fecha}) — probs already saved, skip")
            skipped += 1
            continue

        # Not enough history for ML — save freq/gap probs directly from feature row
        if source_idx < MIN_DRAWS_FOR_ML:
            print(f"  [{i+1}/{total}] draw={current_id} ({fecha}) — early draw (idx={source_idx}), using freq/gap probs")
            if not dry_run:
                try:
                    _save_freq_gap_probs(db, lottery_key, feature_collection, draw_probs_collection, current_id, fecha, source_idx)
                    processed += 1
                except Exception as e:
                    print(f"    ERROR freq/gap probs: {e}")
                    errors += 1
            continue

        if not pre_id:
            print(f"  [{i+1}/{total}] draw={current_id} ({fecha}) — no pre_id, skip compare")
            skipped += 1
            continue

        print(f"  [{i+1}/{total}] draw={current_id} ({fecha}) pre_id={pre_id} idx={source_idx}")

        if dry_run:
            processed += 1
            continue

        try:
            # Run pipeline — saves probs to train_progress
            _ensure_pipeline(session, base_url, api_slug, current_id)

            # Save prob snapshot to draw_probs collection
            _save_probs_from_progress(db, lottery_key, draw_probs_collection, current_id, fecha)

            # Compare if we have a pre_id with probs
            if db[draw_probs_collection].count_documents({"draw_id": pre_id}) > 0:
                jackpot = _trigger_reorder(session, base_url, api_slug, current_id, pre_id)
                print(f"    ✓ probs saved + compare done, jackpot_position={jackpot}")
            else:
                print(f"    ✓ probs saved (no compare — pre_id probs missing)")
            processed += 1

        except Exception as e:
            print(f"    ERROR: {e}")
            errors += 1
            continue

    print(f"\n[{lottery_key}] Done — total={total} processed={processed} skipped={skipped} errors={errors}")
    return {"lottery": lottery_key, "total": total, "processed": processed, "skipped": skipped, "errors": errors}


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill ranking snapshots for all historical draws."
    )
    parser.add_argument(
        "--api-url",
        default="http://localhost:8000",
        help="Backend base URL (default: http://localhost:8000)",
    )
    parser.add_argument(
        "--lottery",
        choices=["euromillones", "el_gordo", "la_primitiva", "all"],
        default="all",
        help="Which lottery to backfill (default: all)",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        default=True,
        help="Skip draws that already have a ranking snapshot (default: True)",
    )
    parser.add_argument(
        "--no-skip-existing",
        dest="skip_existing",
        action="store_false",
        help="Re-process all draws even if ranking already exists",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be processed without making any API calls",
    )
    args = parser.parse_args()

    base_url = args.api_url.rstrip("/")

    # Connect to MongoDB directly for reading feature rows and checking existing rankings
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB]

    session = requests.Session()
    if "localhost" in base_url or "127.0.0.1" in base_url:
        session.trust_env = False

    lotteries = (
        list(LOTTERY_CONFIGS.keys())
        if args.lottery == "all"
        else [args.lottery]
    )

    print(f"Backfill rankings — api_url={base_url} lotteries={lotteries}")
    print(f"skip_existing={args.skip_existing} dry_run={args.dry_run}")
    print("=" * 60)

    results = []
    for lottery_key in lotteries:
        result = backfill_lottery(
            session=session,
            db=db,
            base_url=base_url,
            lottery_key=lottery_key,
            skip_existing=args.skip_existing,
            dry_run=args.dry_run,
        )
        results.append(result)

    print("\n" + "=" * 60)
    print("SUMMARY:")
    for r in results:
        print(
            f"  {r['lottery']:15s}  total={r['total']:5d}  "
            f"processed={r['processed']:5d}  skipped={r['skipped']:5d}  "
            f"errors={r['errors']:3d}"
        )

    mongo_client.close()


if __name__ == "__main__":
    main()
