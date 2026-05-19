"""
Historical learning pipeline backfill (draw 2 → latest).

For each consecutive feature row (pre_id → current_id):
  1. Train ML pipeline with cutoff = pre_id (all history through pre-draw).
  2. Generate full-wheel TXT ranked for pre_id.
  3. Save draw_probs snapshot for pre_id.
  4. Pre-draw ORC snapshot for pre_id.
  5. Compare full wheel (pre_id) vs actual draw (current_id).
  6. Post-draw online learning feedback (current_id, pre_id).

Requires backend API running. Very slow per draw (full wheel can take hours).

Usage:
  python scripts/backfill_learning_pipeline.py --lottery euromillones --api-url http://localhost:8000
  python scripts/backfill_learning_pipeline.py --lottery la-primitiva --start 100 --limit 10
  python scripts/backfill_learning_pipeline.py --all-lotteries --skip-existing
  python scripts/backfill_learning_pipeline.py --delete-synthetic-only
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, List, Optional

_scripts_dir = os.path.dirname(os.path.abspath(__file__))
if _scripts_dir not in sys.path:
    sys.path.insert(0, _scripts_dir)

from pymongo import ASCENDING, MongoClient

# Reuse automation HTTP helpers
from run_daily_prediction_automation import (  # noqa: E402
    LOTTERIES,
    LotteryConfig,
    _ensure_full_wheel,
    _ensure_pipeline,
    _request_json,
    _save_draw_probs,
    _split_date,
    _trigger_compare,
    _trigger_post_draw_feedback,
    _trigger_pre_draw_orc,
)

for _path in [
    os.path.join(_scripts_dir, "..", "backend", ".env"),
    os.path.join(_scripts_dir, "..", ".env"),
]:
    if os.path.isfile(_path):
        with open(_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    if k.strip() and k.strip() not in os.environ:
                        os.environ[k.strip()] = v.strip()
        break

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "lottery")

LOTTERY_DB = {
    "euromillones": {
        "feature": "euromillones_feature",
        "compare": "euromillones_compare_results",
    },
    "el-gordo": {
        "feature": "el_gordo_feature",
        "compare": "el_gordo_compare_results",
    },
    "la-primitiva": {
        "feature": "la_primitiva_feature",
        "compare": "la_primitiva_compare_results",
    },
}


@dataclass(frozen=True)
class DrawPair:
    source_index: int
    pre_id: str
    current_id: str
    draw_date: Optional[str]
    fecha_sorteo: Optional[str]


def _load_pairs(
    db,
    lottery: str,
    start_index: int = 1,
    limit: Optional[int] = None,
) -> List[DrawPair]:
    """Pairs from 2nd draw onward (index 1..n-1 in sorted feature list)."""
    cfg = LOTTERY_DB[lottery]
    docs = list(
        db[cfg["feature"]].find(
            {},
            projection={
                "id_sorteo": 1,
                "pre_id_sorteo": 1,
                "source_index": 1,
                "fecha_sorteo": 1,
            },
        ).sort("source_index", ASCENDING)
    )
    if len(docs) < 2:
        return []

    pairs: List[DrawPair] = []
    for i in range(max(1, start_index), len(docs)):
        cur = docs[i]
        pre = docs[i - 1]
        current_id = str(cur.get("id_sorteo") or "").strip()
        pre_id = str(cur.get("pre_id_sorteo") or pre.get("id_sorteo") or "").strip()
        if not current_id or not pre_id:
            continue
        fecha = str(cur.get("fecha_sorteo") or "")
        pairs.append(
            DrawPair(
                source_index=int(cur.get("source_index") or i),
                pre_id=pre_id,
                current_id=current_id,
                draw_date=_split_date(fecha),
                fecha_sorteo=fecha,
            )
        )

    if limit is not None and limit > 0:
        pairs = pairs[:limit]
    return pairs


def _compare_exists(db, lottery: str, current_id: str, pre_id: str) -> bool:
    coll = db[LOTTERY_DB[lottery]["compare"]]
    doc = coll.find_one(
        {"current_id": current_id, "pre_id": pre_id},
        projection={"jackpot_position": 1},
    )
    if not doc:
        return False
    jp = doc.get("jackpot_position")
    return jp is not None and int(jp) > 0


def delete_synthetic_compares(db, lottery: Optional[str] = None) -> int:
    total = 0
    keys = [lottery] if lottery else list(LOTTERY_DB.keys())
    for key in keys:
        coll = db[LOTTERY_DB[key]["compare"]]
        res = coll.delete_many({"pre_id": "__synthetic__"})
        total += res.deleted_count
        res2 = coll.delete_many({"pre_id": {"$regex": "^__synthetic"}})
        total += res2.deleted_count
    return total


def _reset_cutoff(session, base_url: str, cfg: LotteryConfig, cutoff_draw_id: str) -> None:
    url = (
        f"{base_url}/api/train/reset?lottery={cfg.api_slug}"
        f"&cutoff_draw_id={cutoff_draw_id}&delete_files=false&delete_compare=false"
    )
    try:
        _request_json(session, "POST", url, timeout=60)
    except Exception as e:
        print(f"  [warn] reset failed for {cutoff_draw_id}: {e}")


def process_pair(
    session,
    base_url: str,
    cfg: LotteryConfig,
    db,
    pair: DrawPair,
    *,
    skip_existing: bool,
    with_feedback: bool,
    skip_train: bool,
    skip_full_wheel: bool,
) -> bool:
    label = f"{cfg.name} idx={pair.source_index} pre={pair.pre_id} → cur={pair.current_id}"
    print(f"\n--- {label} ---")

    if skip_existing and _compare_exists(db, cfg.api_slug, pair.current_id, pair.pre_id):
        print("  [skip] compare result already exists")
        if with_feedback:
            try:
                _trigger_post_draw_feedback(session, base_url, cfg, pair.current_id, pair.pre_id)
            except Exception as e:
                print(f"  [warn] post-draw only failed: {e}")
        return True

    try:
        if not skip_train:
            _reset_cutoff(session, base_url, cfg, pair.pre_id)
            _ensure_pipeline(session, base_url, cfg, pair.pre_id)
            _save_draw_probs(session, base_url, cfg, pair.pre_id)

        if not skip_full_wheel:
            _ensure_full_wheel(session, base_url, cfg, pair.pre_id, pair.draw_date)

        _trigger_pre_draw_orc(session, base_url, cfg, pair.pre_id)

        jackpot = _trigger_compare(session, base_url, cfg, pair.current_id, pair.pre_id)
        if not jackpot or int(jackpot) <= 0:
            print("  [error] compare returned no jackpot_position")
            return False

        if with_feedback:
            _trigger_post_draw_feedback(session, base_url, cfg, pair.current_id, pair.pre_id)

        print(f"  [ok] jackpot_position={jackpot}")
        return True
    except Exception as e:
        print(f"  [error] {e}")
        return False


def run_lottery(
    session,
    base_url: str,
    cfg: LotteryConfig,
    db,
    *,
    start_index: int,
    limit: Optional[int],
    skip_existing: bool,
    with_feedback: bool,
    skip_train: bool,
    skip_full_wheel: bool,
) -> tuple[int, int]:
    pairs = _load_pairs(db, cfg.api_slug, start_index=start_index, limit=limit)
    print(f"{cfg.name}: {len(pairs)} draw pair(s) to process (from source_index>={start_index})")
    ok = fail = 0
    for n, pair in enumerate(pairs, 1):
        print(f"[{n}/{len(pairs)}]", end="")
        if process_pair(
            session,
            base_url,
            cfg,
            db,
            pair,
            skip_existing=skip_existing,
            with_feedback=with_feedback,
            skip_train=skip_train,
            skip_full_wheel=skip_full_wheel,
        ):
            ok += 1
        else:
            fail += 1
    return ok, fail


def main() -> None:
    parser = argparse.ArgumentParser(description="Historical train → wheel → compare → online learning backfill")
    parser.add_argument("--api-url", default="http://localhost:8000", help="Backend base URL")
    parser.add_argument(
        "--lottery",
        choices=list(LOTTERY_DB.keys()),
        help="Single lottery to process",
    )
    parser.add_argument("--all-lotteries", action="store_true", help="Process all three lotteries")
    parser.add_argument("--start", type=int, default=1, help="Start at feature source_index (default 1 = 2nd draw)")
    parser.add_argument("--limit", type=int, default=0, help="Max pairs to process (0 = all)")
    parser.add_argument("--skip-existing", action="store_true", help="Skip pairs that already have compare results")
    parser.add_argument("--no-feedback", action="store_true", help="Skip post-draw online learning step")
    parser.add_argument("--skip-train", action="store_true", help="Skip pipeline train (wheel/compare only)")
    parser.add_argument("--skip-full-wheel", action="store_true", help="Skip full-wheel generation")
    parser.add_argument(
        "--delete-synthetic-only",
        action="store_true",
        help="Only remove __synthetic__ rows from compare collections and exit",
    )
    parser.add_argument(
        "--no-delete-synthetic",
        action="store_true",
        help="Do not delete __synthetic__ compare rows before run (default: delete them)",
    )
    args = parser.parse_args()

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    if args.delete_synthetic_only:
        n = delete_synthetic_compares(db, args.lottery)
        print(f"Deleted {n} synthetic compare document(s).")
        return

    if not args.no_delete_synthetic:
        n = delete_synthetic_compares(db, args.lottery if not args.all_lotteries else None)
        print(f"Removed {n} synthetic compare document(s) before backfill.")

    if not args.lottery and not args.all_lotteries:
        parser.error("Specify --lottery <slug> or --all-lotteries")

    import requests

    session = requests.Session()
    if "localhost" in args.api_url or "127.0.0.1" in args.api_url:
        session.trust_env = False
    base_url = args.api_url.rstrip("/")

    limit = args.limit if args.limit > 0 else None
    cfgs = LOTTERIES if args.all_lotteries else [c for c in LOTTERIES if c.api_slug == args.lottery]

    t0 = time.time()
    total_ok = total_fail = 0
    for cfg in cfgs:
        ok, fail = run_lottery(
            session,
            base_url,
            cfg,
            db,
            start_index=args.start,
            limit=limit,
            skip_existing=args.skip_existing,
            with_feedback=not args.no_feedback,
            skip_train=args.skip_train,
            skip_full_wheel=args.skip_full_wheel,
        )
        total_ok += ok
        total_fail += fail

    elapsed = int(time.time() - t0)
    print(f"\nDone in {elapsed}s. OK={total_ok} failed={total_fail}")
    client.close()


if __name__ == "__main__":
    main()
