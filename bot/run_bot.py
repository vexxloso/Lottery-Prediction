"""
Combined bot: run one or all three lotteries (El Gordo, Euromillones, La Primitiva) in one process.

  python run_bot.py              # poll all three queues; run first job that claims
  python run_bot.py --lottery el_gordo
  python run_bot.py --lottery euromillones
  python run_bot.py --lottery la_primitiva

- On first run a browser opens (visible). Log in manually with your username and password
  in that browser, then press Enter in the terminal. The bot keeps the same browser and
  never quits it between jobs; no sign-in form is used after the first login.
- After each job (success or failed) the next job loads the buy page (juegos.loteriasyapuestas.es/jugar/*/apuesta)
  and continues; no username/password input needed.
- Browser is closed only on Ctrl+C or exit. Same .env as single bots.
"""
import argparse
import logging
import os
import signal
import sys
import time
from typing import Any, Callable, List, Optional, Tuple

# When run as bot.exe (PyInstaller), load .env from the folder containing the exe
if getattr(sys, "frozen", False):
    _this_dir = os.path.dirname(sys.executable)
else:
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
_driver = None  # one browser reused for all jobs; quit on exit


def _on_stop(*_args: Any) -> None:
    global _current, _driver
    if _driver:
        try:
            _driver.quit()
        except Exception:
            pass
        _driver = None
    if _current:
        complete_fn, queue_id = _current
        logger.info("Stopping: marking job %s as failed", queue_id)
        complete_fn(queue_id, success=False, error="Bot stopped by user or signal")
        _current = None
    sys.exit(0)


def main() -> None:
    global _current, _driver
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
    api_url = os.environ.get("API_URL", "http://localhost:8000").rstrip("/")
    logger.info("Combined bot starting (lottery=%s). API_URL=%s Polling: %s", args.lottery, api_url, [j[0] for j in jobs])

    # Quick connectivity check so user sees if API is unreachable
    try:
        s = __import__("requests").Session()
        if "localhost" in api_url or "127.0.0.1" in api_url:
            s.trust_env = False
        r = s.get(f"{api_url}/api/metadata/next-draws", timeout=8)
        if r.status_code == 200:
            logger.info("Backend reachable at %s", api_url)
        else:
            logger.warning("Backend returned %s at %s", r.status_code, api_url)
    except Exception as e:
        logger.warning("Cannot reach backend at %s: %s (check API_URL in .env if bot runs on another PC)", api_url, e)

    # Create shared visible browser up front and navigate to main site.
    # User can log in manually; the same browser is reused for all jobs.
    try:
        logger.info("Creating shared browser (visible) at start; you can log in manually.")
        _driver = el_gordo._create_chrome_driver(force_visible=True)
        try:
            _driver.get("https://www.loteriasyapuestas.es/en")
            print()
            print("  >>> Browser opened at https://www.loteriasyapuestas.es/en")
            print("  >>> Log in with your username and password in the browser.")
            print("  >>> The bot will monitor the queue and start buying when there are waiting jobs.")
            print()
        except Exception as e:
            logger.warning("Could not navigate shared browser to main site: %s", e)
    except Exception as e:
        logger.exception("Failed to create shared browser at start: %s", e)
        _driver = None

    signal.signal(signal.SIGINT, _on_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _on_stop)

    poll_cycles = 0
    while True:
        try:
            claimed = False
            for name, claim_fn, complete_fn, run_fn in jobs:
                data = claim_fn()
                if data is None:
                    logger.warning("[%s] claim failed — backend unreachable? Check API_URL in .env", name)
                    continue
                if data and data.get("claimed") and data.get("queue_id"):
                    queue_id = data["queue_id"]
                    tickets = data.get("tickets") or []
                    _current = (complete_fn, queue_id)
                    logger.info("[%s] Claimed job %s, tickets=%s", name, queue_id, len(tickets))
                    # Optional: credentials from backend (lottery modules only use them when they create
                    # their own browser; with the shared driver they rely on the manual login session).
                    username, password = el_gordo._get_login_credentials()
                    if _driver is None:
                        logger.info("Recreating shared browser (previous one was closed or invalid)")
                        try:
                            _driver = el_gordo._create_chrome_driver(force_visible=True)
                            try:
                                _driver.get("https://www.loteriasyapuestas.es/en")
                            except Exception as nav_err:
                                logger.warning("Could not navigate recreated browser to main site: %s", nav_err)
                        except Exception as drv_err:
                            logger.exception("Failed to recreate shared browser: %s", drv_err)
                            complete_fn(queue_id, success=False, error="No browser available for bot")
                            _current = None
                            claimed = True
                            poll_cycles = 0
                            break
                    try:
                        result = run_fn(
                            tickets,
                            progress_callback=lambda s: logger.info("bot: %s", s),
                            username=username,
                            password=password,
                            driver=_driver,
                        )
                        success = result.get("bought") is True
                        err_msg = result.get("error") if not success else None
                        if not err_msg:
                            err_msg = None if success else "Bot did not report success"
                        complete_fn(queue_id, success=success, error=err_msg)
                    except Exception as e:
                        logger.exception("bot run failed: %s", e)
                        complete_fn(queue_id, success=False, error=str(e))
                        err_lower = str(e).lower()
                        if "invalid session" in err_lower or "target window already closed" in err_lower or "no such window" in err_lower:
                            if _driver:
                                try:
                                    _driver.quit()
                                except Exception:
                                    pass
                                _driver = None
                                logger.info("Browser closed or invalid; next job will open a new one")
                    finally:
                        _current = None
                    claimed = True
                    poll_cycles = 0
                    break
            if not claimed:
                poll_cycles += 1
                if poll_cycles % 3 == 1 and poll_cycles > 0:
                    logger.info("No waiting job from any lottery; retry in %ss (check queue has status=waiting)", POLL_INTERVAL_SEC)
                time.sleep(POLL_INTERVAL_SEC)
        except KeyboardInterrupt:
            _on_stop()
        except Exception as e:
            logger.exception("loop: %s", e)
            time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        msg = traceback.format_exc()
        print(msg, file=sys.stderr)
        # When running as .exe on Windows, keep console open so user can see the error
        if getattr(sys, "frozen", False) and sys.platform.startswith("win"):
            input("Press Enter to close...")
        sys.exit(1)
