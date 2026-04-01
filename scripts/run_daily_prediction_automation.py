"""
Daily prediction automation runner.

Goal:
- Run every day at 01:00 local time (or once, on demand).
- For each lottery:
  1) Read latest feature-model row (id_sorteo + pre_id_sorteo).
  2) Ensure train pipeline is done for pre_id_sorteo.
  3) Ensure full-wheel generation is done for pre_id_sorteo.
  4) Trigger compare reorder for (id_sorteo, pre_id_sorteo).

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
    progress = _get_progress(session, base_url, cfg, cutoff_draw_id) or {}
    if progress.get("rules_applied") is True:
        print(f"[{cfg.name}] Pipeline already ready for cutoff={cutoff_draw_id}")
        return
    url = f"{base_url}/api/{cfg.api_slug}/train/run-pipeline?cutoff_draw_id={cutoff_draw_id}"
    data = _request_json(session, "POST", url)
    status = str(data.get("status") or "")
    print(f"[{cfg.name}] Pipeline trigger: status={status or 'unknown'} cutoff={cutoff_draw_id}")
    _wait_pipeline_done(session, base_url, cfg, cutoff_draw_id)
    print(f"[{cfg.name}] Pipeline done")


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


def _trigger_compare(session: requests.Session, base_url: str, cfg: LotteryConfig, current_id: str, pre_id: str) -> None:
    url = f"{base_url}/api/{cfg.api_slug}/compare/full-wheel/reorder?current_id={current_id}&pre_id={pre_id}"
    data = _request_json(session, "POST", url, timeout=120)
    jackpot = data.get("jackpot_position")
    print(f"[{cfg.name}] Compare done via reorder (current={current_id}, pre={pre_id}, jackpot={jackpot})")


def run_once(base_url: str) -> None:
    session = requests.Session()
    # Cursor/IDE environments can inject HTTP(S)_PROXY. For localhost automation,
    # bypass proxies so calls go directly to the local backend.
    if "localhost" in base_url or "127.0.0.1" in base_url:
        session.trust_env = False
    print(f"[automation] Start cycle base_url={base_url}")
    for cfg in LOTTERIES:
        try:
            current_id, pre_id, draw_date = _latest_feature_ids(session, base_url, cfg)
            print(f"[{cfg.name}] Feature latest: current_id={current_id}, pre_id={pre_id}, draw_date={draw_date}")
            # Per updated workflow: run pipeline + full-wheel on current_id.
            _ensure_pipeline(session, base_url, cfg, current_id)
            _ensure_full_wheel(session, base_url, cfg, current_id, draw_date)
            _trigger_compare(session, base_url, cfg, current_id, pre_id)
        except Exception as e:
            print(f"[{cfg.name}] ERROR: {e}")
    print("[automation] Cycle finished")


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily automation for compare + pipeline + full wheel")
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

