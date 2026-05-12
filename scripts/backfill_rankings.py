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
        "feature_collection":  "euromillones_feature",
        "rankings_collection": "euromillones_rankings",
        "api_slug":            "euromillones",
    },
    "el_gordo": {
        "feature_collection":  "el_gordo_feature",
        "rankings_collection": "el_gordo_rankings",
        "api_slug":            "el-gordo",
    },
    "la_primitiva": {
        "feature_collection":  "la_primitiva_feature",
        "rankings_collection": "la_primitiva_rankings",
        "api_slug":            "la-primitiva",
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
    rankings_collection = cfg["rankings_collection"]
    api_slug           = cfg["api_slug"]

    rows = _load_all_feature_rows(db, feature_collection)
    total = len(rows)
    print(f"\n[{lottery_key}] {total} draws found in {feature_collection}")

    if total == 0:
        print(f"[{lottery_key}] No draws — skipping. Run build_{lottery_key}_feature.py first.")
        return {"lottery": lottery_key, "total": 0, "processed": 0, "skipped": 0, "errors": 0}

    processed = 0
    skipped   = 0
    errors    = 0

    for i, row in enumerate(rows):
        current_id = str(row.get("id_sorteo")     or "").strip()
        pre_id     = str(row.get("pre_id_sorteo") or "").strip()
        fecha      = str(row.get("fecha_sorteo")  or "").strip()
        source_idx = int(row.get("source_index",  i))

        if not current_id:
            print(f"  [{i+1}/{total}] SKIP — missing id_sorteo")
            skipped += 1
            continue

        # First draw has no pre_id — we can still run the pipeline but can't compare
        if not pre_id:
            print(f"  [{i+1}/{total}] draw={current_id} ({fecha}) — first draw, no pre_id, pipeline only")
            if not dry_run:
                try:
                    _ensure_pipeline(session, base_url, api_slug, current_id)
                    processed += 1
                except Exception as e:
                    print(f"    ERROR pipeline: {e}")
                    errors += 1
            continue

        # Skip if ranking already saved
        if skip_existing and _ranking_exists(db, rankings_collection, pre_id):
            print(f"  [{i+1}/{total}] draw={current_id} ({fecha}) — ranking for pre_id={pre_id} already exists, skip")
            skipped += 1
            continue

        print(f"  [{i+1}/{total}] draw={current_id} ({fecha}) pre_id={pre_id} source_index={source_idx}")

        if dry_run:
            processed += 1
            continue

        try:
            # 1. Run pipeline for current_id (trains model up to this draw)
            _ensure_pipeline(session, base_url, api_slug, current_id)

            # 2. Save ranking snapshot + compare result
            jackpot = _trigger_reorder(session, base_url, api_slug, current_id, pre_id)
            print(f"    ✓ ranking saved, jackpot_position={jackpot}")
            processed += 1

        except Exception as e:
            print(f"    ERROR: {e}")
            errors += 1
            # Continue with next draw — don't abort the whole backfill
            continue

    print(
        f"\n[{lottery_key}] Done — total={total} processed={processed} "
        f"skipped={skipped} errors={errors}"
    )
    return {
        "lottery":   lottery_key,
        "total":     total,
        "processed": processed,
        "skipped":   skipped,
        "errors":    errors,
    }


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
