"""
Daily scrape: runs automatically every day at 00:02 (local time).
Scrapes 3 days ago → today for all lotteries. Order: Euromillones → La Primitiva → El Gordo.
Last history date is read from DB only (no file/cache). Leave this script running (e.g. in a terminal).
"""
import os
import sys
import time
from datetime import datetime, timedelta

# Allow running from project root or from scripts/
_script_dir = os.path.dirname(os.path.abspath(__file__))
if _script_dir not in sys.path:
    sys.path.insert(0, _script_dir)

from backfill_common import run_daily
from build_euromillones_feature import build as build_euromillones_feature
from build_la_primitiva_feature import build as build_la_primitiva_feature
from build_el_gordo_feature import build as build_el_gordo_feature


def next_00_02():
    """Return the next 00:02 (today if not yet passed, else tomorrow)."""
    now = datetime.now()
    target = now.replace(hour=0, minute=2, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return target


def run_all_feature_updates():
    """Rebuild feature-model collections for all lotteries after scraping."""
    print("Rebuilding feature collections (euromillones_feature / la_primitiva_feature / el_gordo_feature)…")
    try:
        build_euromillones_feature(limit=None)
        print("Euromillones feature rebuilt.")
    except Exception as e:
        print(f"Error rebuilding euromillones_feature: {e}")
    try:
        build_la_primitiva_feature(limit=None)
        print("La Primitiva feature rebuilt.")
    except Exception as e:
        print(f"Error rebuilding la_primitiva_feature: {e}")
    try:
        build_el_gordo_feature(limit=None)
        print("El Gordo feature rebuilt.")
    except Exception as e:
        print(f"Error rebuilding el_gordo_feature: {e}")


if __name__ == "__main__":
    print("Daily scrape started. Running scrape once now, then every day at 00:02. Press Ctrl+C to stop.")
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Running scrape now...")
    try:
        results = run_daily()
        print("Scrape done.", results)
        run_all_feature_updates()
    except Exception as e:
        print(f"Scrape error: {e}")
    print("\nWaiting for next 00:02 to run again.\n")
    while True:
        target = next_00_02()
        wait_seconds = (target - datetime.now()).total_seconds()
        print(f"Next scrape at {target.strftime('%Y-%m-%d %H:%M')} (in {wait_seconds / 3600:.1f} hours)")
        try:
            time.sleep(min(wait_seconds, 86400))
        except KeyboardInterrupt:
            print("\nStopped.")
            break
        if (target - datetime.now()).total_seconds() > 60:
            continue
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Running daily scrape...")
        try:
            results = run_daily()
            print("Daily scrape done.", results)
            run_all_feature_updates()
        except Exception as e:
            print(f"Scrape error: {e}")
        print("Waiting for next 00:02.\n")
