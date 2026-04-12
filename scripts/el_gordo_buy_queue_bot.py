"""
Standalone El Gordo buy-queue bot. Run this process separately from the backend.

- Monitors MongoDB collection `el_gordo_buy_queue` for docs with status "waiting".
- Claims one (sets status to "in_progress"), runs the Selenium bot to buy tickets,
  then marks the doc "bought" (with saved_status=False) or "failed".
- Saving to el_gordo_train_progress.bought_tickets is done by backend (save-bought-from-queue), not by the bot.

Backend does NOT run the bot; it provides enqueue, buy-queue, and save-bought-from-queue.
Dashboard shows waiting / in_progress / bought / failed from the API.

Usage (from project root, with venv that has pymongo + selenium):
  python scripts/el_gordo_buy_queue_bot.py

Env: MONGO_URI, MONGO_DB (same as backend). .env from project root or backend.
Optional: LOTTERY_BUY_QUEUE_BOT_MAX_ATTEMPTS (default 4) — retries before marking a queue item failed.
"""
import logging
import os
import sys
import time
from datetime import datetime as dt

# Load .env from project root or backend
try:
    from dotenv import load_dotenv
    _root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for _path in (os.path.join(_root, ".env"), os.path.join(_root, "backend", ".env")):
        if os.path.isfile(_path):
            load_dotenv(_path)
            break
    else:
        load_dotenv()
except Exception:
    pass

try:
    from pymongo import MongoClient, ReturnDocument
except ImportError:
    print("pymongo required. Install with: pip install pymongo", file=sys.stderr)
    sys.exit(1)

_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from el_gordo_real_platform_bot import run_el_gordo_real_platform_bot

logger = logging.getLogger(__name__)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "lottery")
EL_GORDO_BUY_QUEUE_COLLECTION = "el_gordo_buy_queue"

POLL_INTERVAL_SEC = 10
# After this many bot runs for the same queue item, stop re-queuing (transient site/login issues).
try:
    BOT_MAX_ATTEMPTS = max(1, int((os.getenv("LOTTERY_BUY_QUEUE_BOT_MAX_ATTEMPTS", "4") or "4").strip()))
except ValueError:
    BOT_MAX_ATTEMPTS = 4


def _finish_failed_or_requeue(coll, oid, doc: dict, error_msg: str) -> None:
    attempts = int(doc.get("bot_attempt_count") or 0)
    finished_at = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    if attempts >= BOT_MAX_ATTEMPTS:
        coll.update_one(
            {"_id": oid},
            {"$set": {"status": "failed", "error": error_msg, "finished_at": finished_at}},
        )
        logger.warning("el_gordo_buy_queue_bot: id=%s failed permanently after %s attempts: %s", oid, attempts, error_msg)
    else:
        coll.update_one(
            {"_id": oid},
            {
                "$set": {
                    "status": "waiting",
                    "error": error_msg,
                    "finished_at": None,
                    "started_at": None,
                }
            },
        )
        logger.info(
            "el_gordo_buy_queue_bot: id=%s re-queued for retry (%s/%s): %s",
            oid,
            attempts,
            BOT_MAX_ATTEMPTS,
            error_msg,
        )


def process_one(client) -> bool:
    """Process one waiting item; return True if we processed one."""
    db = client[MONGO_DB]
    coll = db[EL_GORDO_BUY_QUEUE_COLLECTION]
    doc = coll.find_one_and_update(
        {"status": "waiting"},
        {
            "$set": {"status": "in_progress", "started_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")},
            "$inc": {"bot_attempt_count": 1},
        },
        sort=[("created_at", 1)],
        return_document=ReturnDocument.AFTER,
    )
    if not doc:
        return False
    oid = doc["_id"]
    tickets = doc.get("tickets") or []
    draw_date = (doc.get("draw_date") or "").strip()[:10] or None
    cutoff_draw_id = (doc.get("cutoff_draw_id") or "").strip() or None
    logger.info("el_gordo_buy_queue_bot: processing queue id=%s tickets=%s attempt=%s", oid, len(tickets), doc.get("bot_attempt_count"))
    try:
        result = run_el_gordo_real_platform_bot(tickets, progress_callback=lambda s: logger.info("bot: %s", s))
        if result.get("bought") is True:
            coll.update_one(
                {"_id": oid},
                {"$set": {"status": "bought", "saved_status": False, "finished_at": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}},
            )
            # Saving to el_gordo_train_progress.bought_tickets is done by backend/frontend (save-bought-from-queue), not by the bot
        else:
            _finish_failed_or_requeue(coll, oid, doc, "Bot did not report success")
    except Exception as e:
        logger.exception("el_gordo_buy_queue_bot: %s", e)
        _finish_failed_or_requeue(coll, oid, doc, str(e))
    return True


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logger.info("El Gordo buy-queue bot starting (monitors DB, runs Selenium for waiting tickets)")
    client = MongoClient(MONGO_URI)
    try:
        while True:
            try:
                process_one(client)
            except Exception as e:
                logger.exception("loop: %s", e)
            time.sleep(POLL_INTERVAL_SEC)
    finally:
        client.close()


if __name__ == "__main__":
    main()
