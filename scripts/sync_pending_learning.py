#!/usr/bin/env python3
"""
Create or refresh pending Learning history rows for recent draws.

Calls POST /api/online-learning/sync-pending per lottery (public endpoint).

Default: Euromillones + La Primitiva, last 30 draw pairs.
Uses **existing** compare rows only (no training progress / TXT required).

Examples:
  python scripts/sync_pending_learning.py
  python scripts/sync_pending_learning.py --lottery euromillones --last 24
  # New draws only — needs train progress + full wheel:
  python scripts/sync_pending_learning.py --run-compare --api-url http://localhost:8000
"""

from __future__ import annotations

import argparse
import os
import sys

import requests

_scripts_dir = os.path.dirname(os.path.abspath(__file__))
for _path in [
    os.path.join(_scripts_dir, "..", "backend", ".env"),
    os.path.join(_scripts_dir, "..", ".env"),
]:
    if os.path.isfile(_path):
        with open(_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    if k.strip() and k.strip() not in os.environ:
                        os.environ[k.strip()] = v.strip()
        break


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync pending learning history from recent compare results"
    )
    parser.add_argument("--api-url", default=os.getenv("API_URL", "http://localhost:8000"))
    parser.add_argument(
        "--lottery",
        action="append",
        choices=["euromillones", "la-primitiva", "el-gordo"],
        help="Repeat for multiple lotteries (default: euromillones + la-primitiva)",
    )
    parser.add_argument("--last", type=int, default=30, help="Recent draw pairs per lottery")
    parser.add_argument(
        "--run-compare",
        action="store_true",
        help="Run full-wheel compare when missing (needs train progress; not for old data)",
    )
    args = parser.parse_args()

    lotteries = args.lottery or ["euromillones", "la-primitiva"]
    base = args.api_url.rstrip("/")
    session = requests.Session()
    if "localhost" in base or "127.0.0.1" in base:
        session.trust_env = False

    failed = 0
    for lottery in lotteries:
        url = (
            f"{base}/api/online-learning/sync-pending"
            f"?lottery={lottery}&last={args.last}&run_compare={str(args.run_compare).lower()}"
        )
        print(f"\n=== {lottery} (last {args.last}) ===")
        try:
            res = session.post(url, timeout=3600)
            data = res.json() if res.content else {}
            if not res.ok:
                print(f"ERROR {res.status_code}: {data.get('detail', res.text[:300])}")
                failed += 1
                continue
            print(
                f"pairs={data.get('pairs_checked')} refreshed={data.get('refreshed')} "
                f"skipped_no_compare={data.get('skipped_no_compare')} "
                f"new_compares={data.get('compared_new')} feedback_scheduled={data.get('feedback_scheduled')}"
            )
            diag = data.get("diagnostics") or {}
            print(
                f"totals: compares={diag.get('compare_count')} feedback={diag.get('feedback_count')} "
                f"pending={diag.get('pending_feedback')}"
            )
            for err in data.get("errors") or []:
                print(f"  warn: {err}")
        except Exception as e:
            print(f"ERROR: {e}")
            failed += 1

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
