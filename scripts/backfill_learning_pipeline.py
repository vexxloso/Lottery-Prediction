"""
Historical learning pipeline backfill (draw 2 → latest).

Modes:
  full (default) — train → full wheel → ORC → compare → feedback per pair.
  repair-feedback — pairs that already have compare: train+ORC for pre_id (no wheel), then feedback.
  recent-real — last N pairs only: delete their compare rows, run full pipeline (best for client demo).

Requires backend API running. Full wheel is very slow (~hours per draw).

Examples:
  # Client demo: last 24 draws, real data only (recommended)
  python scripts/backfill_learning_pipeline.py --lottery euromillones --mode recent-real --last 24

  # Fix learning history for existing compares (train+ORC, no new wheel)
  python scripts/backfill_learning_pipeline.py --lottery euromillones --mode repair-feedback --last 50

  # Cleanup fake rows
  python scripts/backfill_learning_pipeline.py --delete-synthetic-only --lottery euromillones
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from typing import List, Optional

_scripts_dir = os.path.dirname(os.path.abspath(__file__))
if _scripts_dir not in sys.path:
    sys.path.insert(0, _scripts_dir)

from pymongo import ASCENDING, MongoClient

from run_daily_prediction_automation import (
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
    last_n: Optional[int] = None,
) -> List[DrawPair]:
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

    if last_n is not None and last_n > 0:
        pairs = pairs[-last_n:]
    elif limit is not None and limit > 0:
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


def _orc_exists(db, lottery: str, draw_id: str) -> bool:
    return (
        db["model_orc_snapshots"].find_one(
            {"lottery": lottery, "draw_id": draw_id},
            projection={"_id": 1},
        )
        is not None
    )


def _feedback_exists(db, lottery: str, draw_id: str) -> bool:
    return (
        db["model_feedback_log"].find_one(
            {"lottery": lottery, "draw_id": draw_id},
            projection={"_id": 1},
        )
        is not None
    )


def delete_synthetic_compares(db, lottery: Optional[str] = None) -> int:
    total = 0
    keys = [lottery] if lottery else list(LOTTERY_DB.keys())
    for key in keys:
        coll = db[LOTTERY_DB[key]["compare"]]
        total += coll.delete_many({"pre_id": "__synthetic__"}).deleted_count
        total += coll.delete_many({"pre_id": {"$regex": "^__synthetic"}}).deleted_count
    return total


def purge_compare_pairs(db, lottery: str, pairs: List[DrawPair]) -> int:
    coll = db[LOTTERY_DB[lottery]["compare"]]
    n = 0
    for p in pairs:
        res = coll.delete_one({"current_id": p.current_id, "pre_id": p.pre_id})
        n += res.deleted_count
    return n


def _reset_cutoff(session, base_url: str, cfg: LotteryConfig, cutoff_draw_id: str) -> None:
    url = (
        f"{base_url}/api/train/reset?lottery={cfg.api_slug}"
        f"&cutoff_draw_id={cutoff_draw_id}&delete_files=false&delete_compare=false"
    )
    try:
        _request_json(session, "POST", url, timeout=60)
    except Exception as e:
        print(f"  [warn] reset failed for {cutoff_draw_id}: {e}")


def _ensure_orc_for_pre_draw(
    session,
    base_url: str,
    cfg: LotteryConfig,
    db,
    pre_id: str,
    draw_date: Optional[str],
    *,
    with_full_wheel: bool,
) -> None:
    if _orc_exists(db, cfg.api_slug, pre_id):
        print(f"  [orc] already exists for pre_id={pre_id}")
        return
    print(f"  [orc] building snapshot for pre_id={pre_id} (train; full_wheel={with_full_wheel})")
    _reset_cutoff(session, base_url, cfg, pre_id)
    _ensure_pipeline(session, base_url, cfg, pre_id)
    _save_draw_probs(session, base_url, cfg, pre_id)
    if with_full_wheel:
        _ensure_full_wheel(session, base_url, cfg, pre_id, draw_date)
    _trigger_pre_draw_orc(session, base_url, cfg, pre_id)


def process_pair_full(
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
        print("  [skip] compare already exists")
        if with_feedback and not _feedback_exists(db, cfg.api_slug, pair.current_id):
            try:
                _ensure_orc_for_pre_draw(
                    session, base_url, cfg, db, pair.pre_id, pair.draw_date, with_full_wheel=False
                )
                _trigger_post_draw_feedback(session, base_url, cfg, pair.current_id, pair.pre_id)
                print("  [ok] feedback applied")
            except Exception as e:
                print(f"  [warn] repair on skip failed: {e}")
        return True

    try:
        if not skip_train:
            _reset_cutoff(session, base_url, cfg, pair.pre_id)
            _ensure_pipeline(session, base_url, cfg, pair.pre_id)
            _save_draw_probs(session, base_url, cfg, pair.pre_id)
        if not skip_full_wheel:
            _ensure_full_wheel(session, base_url, cfg, pair.pre_id, pair.draw_date)
        _ensure_orc_for_pre_draw(
            session, base_url, cfg, db, pair.pre_id, pair.draw_date, with_full_wheel=False
        )
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


def process_pair_repair_feedback(
    session,
    base_url: str,
    cfg: LotteryConfig,
    db,
    pair: DrawPair,
    *,
    with_full_wheel: bool,
) -> bool:
    label = f"{cfg.name} idx={pair.source_index} pre={pair.pre_id} → cur={pair.current_id}"
    print(f"\n--- {label} (repair-feedback) ---")

    if not _compare_exists(db, cfg.api_slug, pair.current_id, pair.pre_id):
        print("  [skip] no compare result")
        return False
    if _feedback_exists(db, cfg.api_slug, pair.current_id):
        print("  [skip] feedback already logged")
        return True

    try:
        _ensure_orc_for_pre_draw(
            session, base_url, cfg, db, pair.pre_id, pair.draw_date, with_full_wheel=with_full_wheel
        )
        _trigger_post_draw_feedback(session, base_url, cfg, pair.current_id, pair.pre_id)
        print("  [ok] feedback applied")
        return True
    except Exception as e:
        print(f"  [error] {e}")
        return False


def run_lottery(
    session,
    base_url: str,
    cfg: LotteryConfig,
    db,
    pairs: List[DrawPair],
    *,
    mode: str,
    skip_existing: bool,
    with_feedback: bool,
    skip_train: bool,
    skip_full_wheel: bool,
    repair_full_wheel: bool,
) -> tuple[int, int]:
    print(f"{cfg.name}: {len(pairs)} draw pair(s) | mode={mode}")
    ok = fail = 0
    for n, pair in enumerate(pairs, 1):
        print(f"[{n}/{len(pairs)}]", end="")
        if mode == "repair-feedback":
            success = process_pair_repair_feedback(
                session, base_url, cfg, db, pair, with_full_wheel=repair_full_wheel
            )
        else:
            success = process_pair_full(
                session,
                base_url,
                cfg,
                db,
                pair,
                skip_existing=skip_existing,
                with_feedback=with_feedback,
                skip_train=skip_train,
                skip_full_wheel=skip_full_wheel,
            )
        if success:
            ok += 1
        else:
            fail += 1
    return ok, fail


def main() -> None:
    parser = argparse.ArgumentParser(description="Historical learning pipeline backfill")
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--lottery", choices=list(LOTTERY_DB.keys()))
    parser.add_argument("--all-lotteries", action="store_true")
    parser.add_argument(
        "--mode",
        choices=["full", "repair-feedback", "recent-real"],
        default="full",
        help="full=all steps; repair-feedback=ORC+feedback for existing compare; recent-real=rebuild last N draws",
    )
    parser.add_argument("--start", type=int, default=1)
    parser.add_argument("--limit", type=int, default=0, help="Max pairs from start (ignored if --last set)")
    parser.add_argument("--last", type=int, default=0, help="Only last N draw pairs (recommended for demo)")
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--no-feedback", action="store_true")
    parser.add_argument("--skip-train", action="store_true")
    parser.add_argument("--skip-full-wheel", action="store_true")
    parser.add_argument(
        "--repair-with-full-wheel",
        action="store_true",
        help="In repair-feedback mode, also generate full wheel (slow, more accurate ORC hash)",
    )
    parser.add_argument("--delete-synthetic-only", action="store_true")
    parser.add_argument("--no-delete-synthetic", action="store_true")
    args = parser.parse_args()

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    if args.delete_synthetic_only:
        n = delete_synthetic_compares(db, args.lottery)
        print(f"Deleted {n} synthetic compare document(s).")
        return

    if not args.no_delete_synthetic:
        n = delete_synthetic_compares(db, args.lottery if not args.all_lotteries else None)
        if n:
            print(f"Removed {n} synthetic compare document(s).")

    if not args.lottery and not args.all_lotteries:
        parser.error("Specify --lottery <slug> or --all-lotteries")

    import requests

    session = requests.Session()
    if "localhost" in args.api_url or "127.0.0.1" in args.api_url:
        session.trust_env = False
    base_url = args.api_url.rstrip("/")

    last_n = args.last if args.last > 0 else None
    limit = args.limit if args.limit > 0 else None
    cfgs = LOTTERIES if args.all_lotteries else [c for c in LOTTERIES if c.api_slug == args.lottery]

    if args.mode == "recent-real" and not last_n:
        print("WARNING: --mode recent-real works best with --last N (e.g. --last 24). Using last 24.")
        last_n = 24

    t0 = time.time()
    total_ok = total_fail = 0
    for cfg in cfgs:
        pairs = _load_pairs(
            db, cfg.api_slug, start_index=args.start, limit=limit, last_n=last_n
        )
        if args.mode == "recent-real" and pairs:
            removed = purge_compare_pairs(db, cfg.api_slug, pairs)
            print(f"{cfg.name}: purged {removed} old compare row(s) for selected range (fresh real run)")

        run_mode = "full" if args.mode == "recent-real" else args.mode
        ok, fail = run_lottery(
            session,
            base_url,
            cfg,
            db,
            pairs,
            mode=run_mode,
            skip_existing=False if args.mode == "recent-real" else args.skip_existing,
            with_feedback=not args.no_feedback,
            skip_train=args.skip_train if args.mode != "recent-real" else False,
            skip_full_wheel=args.skip_full_wheel if args.mode != "recent-real" else False,
            repair_full_wheel=args.repair_with_full_wheel,
        )
        total_ok += ok
        total_fail += fail

    elapsed = int(time.time() - t0)
    print(f"\nDone in {elapsed}s. OK={total_ok} failed={total_fail}")
    client.close()


if __name__ == "__main__":
    main()
