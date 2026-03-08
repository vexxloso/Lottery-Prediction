"""
Combined bot: run one or all three lotteries (El Gordo, Euromillones, La Primitiva) in one process.

  python run_bot.py              # poll all three queues; run first job that claims
  python run_bot.py --lottery el_gordo
  python run_bot.py --lottery euromillones
  python run_bot.py --lottery la_primitiva

Same .env as single bots. On Ctrl+C or crash the current job is marked failed.
"""
import argparse
import logging
import os
import signal
import sys
import time
from typing import Any, Callable, List, Optional, Tuple

_this_dir = os.path.dirname(os.path.abspath(__file__))
_env = os.path.join(_this_dir, ".env")
if os.path.isfile(_env):
    try:
        from dotenv import load_dotenv
        load_dotenv(_env)
    except Exception:
        pass

# Import after .env so API_URL etc are set (run from bot/ so these are same-dir modules)
import el_gordo
import euromillones
import la_primitiva

logger = logging.getLogger(__name__)

POLL_INTERVAL_SEC = 10
LOTTERIES = ("el_gordo", "euromillones", "la_primitiva")

# (name, claim_fn, complete_fn, run_buy_fn)
_JOBS: List[Tuple[str, Callable, Callable[[str, bool, Optional[str]], None], Callable]] = [
    ("el_gordo", el_gordo.claim_job, el_gordo.complete_job, el_gordo.run_buy),
    ("euromillones", euromillones.claim_job, euromillones.complete_job, euromillones.run_buy),
    ("la_primitiva", la_primitiva.claim_job, la_primitiva.complete_job, la_primitiva.run_buy),
]

_current: Optional[Tuple[Callable[[str, bool, Optional[str]], None], str]] = None


def _on_stop(*_args: Any) -> None:
    global _current
    if _current:
        complete_fn, queue_id = _current
        logger.info("Stopping: marking job %s as failed", queue_id)
        complete_fn(queue_id, success=False, error="Bot stopped by user or signal")
        _current = None
    sys.exit(0)


def main() -> None:
    global _current
    parser = argparse.ArgumentParser(description="Run lottery buy-queue bot (one or all)")
    parser.add_argument(
        "--lottery",
        choices=["all"] + list(LOTTERIES),
        default="all",
        help="Which lottery to poll (default: all)",
    )
    args = parser.parse_args()

    if args.lottery == "all":
        jobs = _JOBS
    else:
        jobs = [(n, c, co, r) for n, c, co, r in _JOBS if n == args.lottery]
    if not jobs:
        logger.error("No lottery selected")
        sys.exit(1)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logger.info("Combined bot starting (lottery=%s). Polling: %s", args.lottery, [j[0] for j in jobs])

    signal.signal(signal.SIGINT, _on_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _on_stop)

    while True:
        try:
            claimed = False
            for name, claim_fn, complete_fn, run_fn in jobs:
                data = claim_fn()
                if data and data.get("claimed") and data.get("queue_id"):
                    queue_id = data["queue_id"]
                    tickets = data.get("tickets") or []
                    _current = (complete_fn, queue_id)
                    logger.info("[%s] Claimed job %s, tickets=%s", name, queue_id, len(tickets))
                    try:
                        result = run_fn(tickets, progress_callback=lambda s: logger.info("bot: %s", s))
                        success = result.get("bought") is True
                        complete_fn(queue_id, success=success, error=None if success else "Bot did not report success")
                    except Exception as e:
                        logger.exception("bot run failed: %s", e)
                        complete_fn(queue_id, success=False, error=str(e))
                    finally:
                        _current = None
                    claimed = True
                    break
            if not claimed:
                time.sleep(POLL_INTERVAL_SEC)
        except KeyboardInterrupt:
            _on_stop()
        except Exception as e:
            logger.exception("loop: %s", e)
            time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
