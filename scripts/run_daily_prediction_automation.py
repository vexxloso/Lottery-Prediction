"""
Daily prediction automation runner.

Goal:
- Run every day at 01:00 local time (or once, on demand).
- For each lottery:
  1) Read latest feature-model row (id_sorteo + pre_id_sorteo).
  2) Ensure train pipeline is done for current_id (trains model, computes probs, builds pool).
  3) Save probability snapshot to draw_probs collection.
  4) Generate TXT full wheel file + ORC snapshot (pre-draw step).
  5) Compare using TXT file — saves jackpot position to compare_results.
  6) Apply online learning feedback loop (post-draw step) — updates model weights.

This script uses backend public endpoints only.
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

import requests


POLL_SECONDS = 8
PIPELINE_TIMEOUT_SECONDS = 45 * 60
FULL_WHEEL_TIMEOUT_SECONDS = 90 * 60
ONLINE_LEARNING_TIMEOUT_SECONDS = 10 * 60


@dataclass(frozen=True)
class LotteryConfig:
    name: str
    api_slug: str


LOTTERIES = [
    LotteryConfig(name="Euromillones", api_slug="euromillones"),
    LotteryConfig(name="El Gordo", api_slug="el-gordo"),
    LotteryConfig(name="La Primitiva", api_slug="la-primitiva"),
]


def _next_01_00() -> datetime:
    now = datetime.now()
    target = now.replace(hour=1, minute=0, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return target


def _split_date(value: Any) -> Optional[str]:
    s = str(value or "").strip()
    if not s:
        return None
    return s.split(" ")[0]


def _request_json(session: requests.Session, method: str, url: str, timeout: int = 30) -> dict[str, Any]:
    res = session.request(method, url, timeout=timeout)
    data: dict[str, Any] = {}
    try:
        data = res.json() if res.content else {}
    except Exception:
        pass
    if not res.ok:
        detail = data.get("detail") if isinstance(data, dict) else None
        raise RuntimeError(f"{method} {url} failed ({res.status_code}): {detail or res.text[:300]}")
    return data


def _latest_feature_ids(session: requests.Session, base_url: str, cfg: LotteryConfig) -> tuple[str, str, Optional[str]]:
    url = f"{base_url}/api/{cfg.api_slug}/feature-model?limit=1&skip=0"
    data = _request_json(session, "GET", url)
    features = data.get("features") or []
    if not isinstance(features, list) or not features:
        raise RuntimeError(f"{cfg.name}: feature-model empty")
    row = features[0] if isinstance(features[0], dict) else {}
    current_id = str(row.get("id_sorteo") or "").strip()
    pre_id = str(row.get("pre_id_sorteo") or "").strip()
    draw_date = _split_date(row.get("fecha_sorteo"))
    if not current_id or not pre_id:
        raise RuntimeError(
            f"{cfg.name}: latest feature row missing id_sorteo/pre_id_sorteo (id={current_id!r}, pre={pre_id!r})"
        )
    return current_id, pre_id, draw_date


def _get_progress(session: requests.Session, base_url: str, cfg: LotteryConfig, cutoff_draw_id: str) -> Optional[dict[str, Any]]:
    url = f"{base_url}/api/{cfg.api_slug}/train/progress?cutoff_draw_id={cutoff_draw_id}"
    data = _request_json(session, "GET", url)
    progress = data.get("progress")
    return progress if isinstance(progress, dict) else None


def _wait_pipeline_done(session: requests.Session, base_url: str, cfg: LotteryConfig, cutoff_draw_id: str) -> None:
    start = time.time()
    while True:
        progress = _get_progress(session, base_url, cfg, cutoff_draw_id) or {}
        status = str(progress.get("pipeline_status") or "").lower()
        if status == "done" or progress.get("rules_applied") is True:
            return
        if status == "error":
            raise RuntimeError(f"{cfg.name}: pipeline_status=error ({progress.get('pipeline_error')})")
        if time.time() - start > PIPELINE_TIMEOUT_SECONDS:
            raise RuntimeError(f"{cfg.name}: pipeline timeout after {PIPELINE_TIMEOUT_SECONDS}s")
        time.sleep(POLL_SECONDS)


def _wait_full_wheel_done(session: requests.Session, base_url: str, cfg: LotteryConfig, cutoff_draw_id: str) -> None:
    start = time.time()
    while True:
        progress = _get_progress(session, base_url, cfg, cutoff_draw_id) or {}
        status = str(progress.get("full_wheel_status") or "").lower()
        if status == "done" and str(progress.get("full_wheel_file_path") or "").strip():
            return
        if status == "error":
            raise RuntimeError(f"{cfg.name}: full_wheel_status=error ({progress.get('full_wheel_error')})")
        if time.time() - start > FULL_WHEEL_TIMEOUT_SECONDS:
            raise RuntimeError(f"{cfg.name}: full wheel timeout after {FULL_WHEEL_TIMEOUT_SECONDS}s")
        time.sleep(POLL_SECONDS)


def _ensure_pipeline(session: requests.Session, base_url: str, cfg: LotteryConfig, cutoff_draw_id: str) -> None:
    """
    Always retrain the pipeline for cutoff_draw_id using ALL draws from the second onward.
    We reset the pipeline_status so it retrains with the latest accumulated data,
    rather than skipping if rules_applied is already True from a previous run.
    """
    # Reset the pipeline so it retrains with all draws up to current_id
    reset_url = f"{base_url}/api/train/reset?lottery={cfg.api_slug}&delete_files=false&delete_compare=false"
    try:
        _request_json(session, "POST", reset_url, timeout=30)
        print(f"[{cfg.name}] Pipeline reset for cutoff={cutoff_draw_id} (continuous retraining)")
    except Exception as e:
        print(f"[{cfg.name}] WARNING: pipeline reset failed ({e}), proceeding anyway")

    url = f"{base_url}/api/{cfg.api_slug}/train/run-pipeline?cutoff_draw_id={cutoff_draw_id}"
    data = _request_json(session, "POST", url)
    status = str(data.get("status") or "")
    print(f"[{cfg.name}] Pipeline trigger: status={status or 'unknown'} cutoff={cutoff_draw_id}")
    _wait_pipeline_done(session, base_url, cfg, cutoff_draw_id)
    print(f"[{cfg.name}] Pipeline done (retrained with all draws up to {cutoff_draw_id})")


def _ensure_full_wheel(
    session: requests.Session, base_url: str, cfg: LotteryConfig, cutoff_draw_id: str, draw_date: Optional[str]
) -> None:
    progress = _get_progress(session, base_url, cfg, cutoff_draw_id) or {}
    if str(progress.get("full_wheel_status") or "").lower() == "done" and str(
        progress.get("full_wheel_file_path") or ""
    ).strip():
        print(f"[{cfg.name}] Full wheel already ready for cutoff={cutoff_draw_id}")
        return

    params = f"cutoff_draw_id={cutoff_draw_id}"
    if draw_date:
        params += f"&draw_date={draw_date}"
    url = f"{base_url}/api/{cfg.api_slug}/train/full-wheel?{params}"
    data = _request_json(session, "POST", url, timeout=60)
    status = str(data.get("status") or "")
    print(f"[{cfg.name}] Full wheel trigger: status={status or 'unknown'} cutoff={cutoff_draw_id}")
    _wait_full_wheel_done(session, base_url, cfg, cutoff_draw_id)
    print(f"[{cfg.name}] Full wheel done")


def _save_draw_probs(session: requests.Session, base_url: str, cfg: LotteryConfig, draw_id: str) -> None:
    """Save probability snapshot for draw_id to draw_probs collection via backfill endpoint."""
    url = f"{base_url}/api/ranking/save-draw-probs?lottery={cfg.api_slug}&draw_id={draw_id}"
    try:
        data = _request_json(session, "POST", url, timeout=30)
        print(f"[{cfg.name}] draw_probs saved for draw_id={draw_id} source={data.get('source','?')}")
    except Exception as e:
        print(f"[{cfg.name}] WARNING: could not save draw_probs for {draw_id}: {e}")


def _trigger_compare(session: requests.Session, base_url: str, cfg: LotteryConfig, current_id: str, pre_id: str) -> Optional[int]:
    """Trigger TXT-based compare. Fast — reads file sequentially until jackpot found.
    Returns jackpot_position so the feedback loop can use it."""
    url = f"{base_url}/api/{cfg.api_slug}/compare/full-wheel?current_id={current_id}&pre_id={pre_id}"
    data = _request_json(session, "GET", url, timeout=600)
    jackpot = data.get("jackpot_position")
    print(f"[{cfg.name}] Compare done (current={current_id}, pre={pre_id}, jackpot={jackpot})")
    return jackpot


def _trigger_pre_draw_orc(session: requests.Session, base_url: str, cfg: LotteryConfig, cutoff_draw_id: str) -> None:
    """
    Pre-draw step: generate .orc binary model snapshot + SHA-256 hash.
    Called AFTER the full wheel TXT is ready so the hash can cover both files.
    """
    url = f"{base_url}/api/online-learning/pre-draw?lottery={cfg.api_slug}&cutoff_draw_id={cutoff_draw_id}"
    try:
        data = _request_json(session, "POST", url, timeout=120)
        orc_hash = data.get("orc_hash", "")
        print(f"[{cfg.name}] Pre-draw ORC snapshot saved (draw_id={cutoff_draw_id}, hash={orc_hash[:12]}...)")
    except Exception as e:
        print(f"[{cfg.name}] WARNING: pre-draw ORC failed ({e}) — feedback loop will be skipped for this draw")


def _trigger_post_draw_feedback(
    session: requests.Session,
    base_url: str,
    cfg: LotteryConfig,
    current_id: str,
    pre_id: str,
) -> None:
    """
    Post-draw feedback loop:
      1. Load .orc snapshot from pre_id (model state before the draw)
      2. Compute error = jackpot_position / total_tickets
      3. Warm-start GBM with 10 more estimators weighted toward winning numbers
      4. Save updated model + new .orc for next draw cycle
    """
    url = f"{base_url}/api/online-learning/post-draw?lottery={cfg.api_slug}&current_id={current_id}&pre_id={pre_id}"
    start = time.time()
    try:
        data = _request_json(session, "POST", url, timeout=ONLINE_LEARNING_TIMEOUT_SECONDS)
        error_rate = data.get("error_rate", "?")
        new_orc = data.get("new_orc_path", "")
        elapsed = int(time.time() - start)
        print(
            f"[{cfg.name}] Post-draw feedback done "
            f"(current={current_id}, pre={pre_id}, error_rate={error_rate}, "
            f"new_orc={new_orc!r}, elapsed={elapsed}s)"
        )
    except Exception as e:
        elapsed = int(time.time() - start)
        print(f"[{cfg.name}] WARNING: post-draw feedback failed after {elapsed}s: {e}")


def run_once(base_url: str) -> None:
    session = requests.Session()
    if "localhost" in base_url or "127.0.0.1" in base_url:
        session.trust_env = False
    print(f"[automation] Start cycle base_url={base_url}")
    for cfg in LOTTERIES:
        try:
            current_id, pre_id, draw_date = _latest_feature_ids(session, base_url, cfg)
            print(f"[{cfg.name}] Feature latest: current_id={current_id}, pre_id={pre_id}, draw_date={draw_date}")

            # Step 1: run pipeline (train model, compute probs, build pool)
            _ensure_pipeline(session, base_url, cfg, current_id)

            # Step 2: save probability snapshot for current_id to draw_probs collection
            # (used by Study Progress Dashboard — lightweight, instant)
            _save_draw_probs(session, base_url, cfg, current_id)

            # Step 3: generate TXT full wheel file (original system)
            _ensure_full_wheel(session, base_url, cfg, current_id, draw_date)

            # Step 4: pre-draw ORC snapshot — captures model state AFTER full wheel is ready
            # so both .txt and .orc share the same SHA-256 validation hash
            _trigger_pre_draw_orc(session, base_url, cfg, current_id)

            # Step 5: compare using TXT file — fast sequential read
            _trigger_compare(session, base_url, cfg, current_id, pre_id)

            # Step 6: post-draw feedback loop — update model weights based on actual draw result
            # This is the continuous learning step: model accumulates knowledge with each draw
            _trigger_post_draw_feedback(session, base_url, cfg, current_id, pre_id)

        except Exception as e:
            print(f"[{cfg.name}] ERROR: {e}")
    print("[automation] Cycle finished")


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily automation: pipeline + probs + TXT full wheel + compare")
    parser.add_argument("--api-url", default="http://localhost:8000", help="Backend base URL")
    parser.add_argument("--once", action="store_true", help="Run one cycle immediately and exit")
    args = parser.parse_args()

    base_url = args.api_url.rstrip("/")

    if args.once:
        run_once(base_url)
        return

    print("Automation started. Running once now, then every day at 01:00 local time. Ctrl+C to stop.")
    run_once(base_url)
    while True:
        target = _next_01_00()
        wait_seconds = (target - datetime.now()).total_seconds()
        print(f"[automation] Next run at {target.strftime('%Y-%m-%d %H:%M')} (in {wait_seconds / 3600:.2f}h)")
        try:
            time.sleep(max(1, int(wait_seconds)))
        except KeyboardInterrupt:
            print("[automation] Stopped")
            break
        run_once(base_url)


if __name__ == "__main__":
    main()

