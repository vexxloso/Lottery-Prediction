"""
Lottery Prediction API — scrape lottery draws and save to MongoDB.
Uses Selenium with a new Chrome instance (pattern from refer.py).
Three collections: la_primitiva, euromillones, el_gordo.
Stores combinacion (main), parsed numbers/C/R, and joker combinacion.
"""
import json
import logging
import os
import re
import sys
from contextlib import asynccontextmanager
from statistics import mean
from typing import Optional, List, Dict

from dotenv import load_dotenv

load_dotenv()

from datetime import datetime as dt, timedelta

from bson import ObjectId
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

from simulation.euromillones.frequency_model import (
    predict_next_frequency_scores,
    save_frequency_simulation_result,
    train_all_frequency_models,
)
from simulation.euromillones.gap_model import (
    predict_next_gap_scores,
    save_gap_simulation_result,
    train_all_gap_models,
)
from simulation.euromillones.hot_model import (
    predict_next_hot_scores,
    save_hot_simulation_result,
    train_all_hot_models,
)
from simulation.euromillones.prediction_compare import compare_prediction_with_result
from simulation.euromillones.candidate_pool import build_candidate_pool
from simulation.el_gordo.candidate_pool import build_el_gordo_candidate_pool
from simulation.el_gordo.frequency_model import (
    predict_next_el_gordo_frequency_scores,
    save_el_gordo_frequency_simulation_result,
    train_all_el_gordo_frequency_models,
)
from simulation.el_gordo.gap_model import (
    predict_next_el_gordo_gap_scores,
    save_el_gordo_gap_simulation_result,
    train_all_el_gordo_gap_models,
)
from simulation.el_gordo.hot_model import (
    predict_next_el_gordo_hot_scores,
    save_el_gordo_hot_simulation_result,
    train_all_el_gordo_hot_models,
)
from simulation.el_gordo.simple_simulation import run_el_gordo_simple_simulation

# Lottery slug -> game_id for loteriasyapuestas.es API (El Gordo = ELGR per site)
GAME_IDS = {
    "la-primitiva": "LAPR",
    "euromillones": "EMIL",
    "el-gordo": "ELGR",
}

# Results page path per lottery (load this first so real Chrome has correct referer/cookies)
RESULTS_PATHS = {
    "la-primitiva": "/es/resultados/la-primitiva",
    "euromillones": "/es/resultados/euromillones",
    "el-gordo": "/es/resultados/gordo-primitiva",
}

# Daily scrape at 00:02 and Update button: process in this order
LOTTERY_DAILY_ORDER = ["euromillones", "la-primitiva", "el-gordo"]

BASE_URL = "https://www.loteriasyapuestas.es/servicios/buscadorSorteos"
SITE_ORIGIN = "https://www.loteriasyapuestas.es"

# One collection per lottery (El Gordo = ELGR)
COLLECTIONS = {"LAPR": "la_primitiva", "EMIL": "euromillones", "ELGR": "el_gordo"}
METADATA_COLLECTION = "scraper_metadata"

logger = logging.getLogger("lottery")

# MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "lottery")

client: MongoClient | None = None
db = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global client, db
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    for coll_name in COLLECTIONS.values():
        db[coll_name].create_index("id_sorteo", unique=True)
    yield
    if client:
        client.close()


def parse_combinacion(combinacion: str) -> dict:
    """
    Parse "04 - 12 - 16 - 37 - 39 - 45 C(44) R(9)" into numbers array, C, R.
    """
    numbers = []
    complementario = None
    reintegro = None
    if not combinacion or not isinstance(combinacion, str):
        return {"numbers": numbers, "complementario": complementario, "reintegro": reintegro}
    match_c = re.search(r"C\((\d+)\)", combinacion)
    match_r = re.search(r"R\((\d+)\)", combinacion)
    if match_c:
        complementario = int(match_c.group(1))
    if match_r:
        reintegro = int(match_r.group(1))
    main_part = re.split(r"\s+C\(|\s+R\(", combinacion)[0].strip()
    for part in main_part.split("-"):
        part = part.strip()
        if part.isdigit():
            numbers.append(int(part))
    return {"numbers": numbers, "complementario": complementario, "reintegro": reintegro}


def normalize_draw(draw: dict) -> dict:
    """Add parsed numbers, C, R, and joker_combinacion; keep all original fields."""
    out = dict(draw)
    combinacion = draw.get("combinacion") or ""
    parsed = parse_combinacion(combinacion)
    out["numbers"] = parsed["numbers"]
    out["complementario"] = parsed["complementario"]
    out["reintegro"] = parsed["reintegro"]
    joker = draw.get("joker") or {}
    millon = draw.get("millon") or {}
    out["joker_combinacion"] = (
        (joker.get("combinacion") if isinstance(joker, dict) else None)
        or (millon.get("combinacion") if isinstance(millon, dict) else None)
    )
    return out


app = FastAPI(title="Lottery Prediction API", lifespan=lifespan)

# Allow frontend on common dev ports (5173 = default Vite, 5174+ when 5173 is in use)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://151.241.216.178:5173"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health():
    return {"status": "ok"}


def create_driver() -> webdriver.Chrome:
    """
    Create a new Chrome browser via Selenium (same pattern as refer.py).
    Headless on all platforms for API use; Linux gets extra stability flags.
    """
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280,800")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    if sys.platform.startswith("linux"):
        options.add_argument("--disable-setuid-sandbox")
        try:
            import shutil
            for path in ("/usr/bin/google-chrome", "/usr/bin/google-chrome-stable"):
                if shutil.which(path):
                    options.binary_location = path
                    break
        except Exception:
            pass
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(30)
    driver.set_script_timeout(30)
    return driver


def _parse_proximo_bote_from_page(driver) -> Optional[float]:
    """
    Parse Próx. bote (next jackpot) from the loaded results page.
    Looks for span.c-elemento-destacado_cantidad-millones (e.g. "193") and treats as millions.
    Returns value in euros (e.g. 193 -> 193_000_000.0), or None if not found.
    """
    try:
        # Pattern from loteriasyapuestas.es: number in span with class cantidad-millones
        el = driver.find_element(By.CSS_SELECTOR, ".c-elemento-destacado_cantidad-millones")
        text = (el.text or "").strip().replace(",", ".").replace("\u00a0", "").replace(" ", "")
        if not text:
            return None
        num = float(text)
        # Class name indicates "millones"; 193 -> 193 million euros
        return num * 1_000_000.0
    except Exception:
        try:
            # Fallback: any element with cantidad-millones in class
            el = driver.find_element(By.CSS_SELECTOR, "[class*='cantidad-millones']")
            text = (el.text or "").strip().replace(",", ".").replace("\u00a0", "").replace(" ", "")
            if not text:
                return None
            num = float(text)
            return num * 1_000_000.0
        except Exception:
            return None


def _scrape_with_selenium(api_url: str, results_page_url: str) -> tuple:
    """
    Launch Chrome, load results page, parse Próx. bote from HTML, fetch API, return (draws, proximo_bote_eur).
    proximo_bote_eur is None if parsing failed.
    """
    driver = None
    proximo_bote_eur: Optional[float] = None
    try:
        driver = create_driver()
        driver.get(results_page_url)
        proximo_bote_eur = _parse_proximo_bote_from_page(driver)
        data = driver.execute_async_script(
            """
            var url = arguments[0];
            var callback = arguments[arguments.length - 1];
            fetch(url)
                .then(function(r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
                .then(callback)
                .catch(function(e) { callback({__error: e.message}); });
            """,
            api_url,
        )
        if isinstance(data, dict) and data.get("__error"):
            raise RuntimeError(data["__error"])
        return (data, proximo_bote_eur)
    except Exception:
        raise
    finally:
        if driver:
            try:
                driver.quit()
            except Exception as e:
                logger.warning("Driver quit failed (ignored): %s", e)


@app.get("/api/scrape")
def scrape(
    start_date: str = Query(..., description="YYYYMMDD"),
    end_date: str = Query(..., description="YYYYMMDD"),
    lottery: str = Query(..., description="la-primitiva | euromillones | el-gordo"),
):
    """Fetch draws from loteriasyapuestas.es using Selenium Chrome, save to MongoDB."""
    game_id = GAME_IDS.get(lottery)
    if not game_id:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery}")

    api_url = (
        f"{BASE_URL}"
        f"?game_id={game_id}"
        "&celebrados=true"
        f"&fechaInicioInclusiva={start_date}"
        f"&fechaFinInclusiva={end_date}"
    )
    results_path = RESULTS_PATHS.get(lottery, RESULTS_PATHS["la-primitiva"])
    results_page_url = f"{SITE_ORIGIN}{results_path}"

    try:
        data, proximo_bote_eur = _scrape_with_selenium(api_url, results_page_url)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Selenium scrape error")
        raise HTTPException(502, detail=f"Chrome scrape failed: {e!s}")

    if not isinstance(data, list):
        got = type(data).__name__
        if isinstance(data, dict):
            got = f"dict keys: {list(data.keys())[:10]}"
        raise HTTPException(
            502,
            detail=f"API did not return a list of draws (got {got})",
        )

    saved, errors = _save_draws_to_db(data)
    max_date = _max_date_from_draws(data)
    if not max_date:
        max_date = _get_max_draw_date_from_db(game_id)
    if max_date:
        _set_last_draw_date(lottery, max_date)
    _update_next_funds_metadata(lottery, scraped_bote=proximo_bote_eur)

    return {
        "saved": saved,
        "total": len(data),
        "lottery": lottery,
        "game_id": game_id,
        "start_date": start_date,
        "end_date": end_date,
        "proximo_bote_eur": proximo_bote_eur,
        "message": f"Saved {saved} draws to MongoDB.",
        "errors": errors[:5] if errors else None,
    }


def _get_last_draw_date(lottery: str) -> str | None:
    """Get last_draw_date for a lottery from scraper_metadata."""
    if db is None:
        return None
    doc = db[METADATA_COLLECTION].find_one({"lottery": lottery}, projection=["last_draw_date"])
    return (doc.get("last_draw_date") or "").strip() or None


@app.get("/api/metadata/next-draws")
def get_next_draws_metadata():
    """
    Return last_draw_date and next_draw_date for each lottery from scraper_metadata.

    Response example:
      {
        "items": [
          { "lottery": "euromillones", "last_draw_date": "2026-02-27", "next_draw_date": "2026-03-03" },
          { "lottery": "la-primitiva", "last_draw_date": "...", "next_draw_date": "..." },
          { "lottery": "el-gordo", "last_draw_date": "...", "next_draw_date": "..." }
        ]
      }
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    cursor = db[METADATA_COLLECTION].find(
        {},
        projection={
            "_id": 1,
            "lottery": 1,
            "last_draw_date": 1,
            "next_draw_date": 1,
            "next_bote": 1,
            "next_premios": 1,
            "next_funds_updated_at": 1,
        },
    )
    items = []
    for doc in cursor:
        d = _item_to_json(doc)
        next_bote = doc.get("next_bote")
        next_premios = doc.get("next_premios")
        if next_bote is None and doc.get("next_funds_prediction"):
            old = doc["next_funds_prediction"] or {}
            next_bote = (old.get("bote_stats") or {}).get("median")
            next_premios = next_premios or (old.get("premios_stats") or {}).get("median")
        if isinstance(next_bote, (int, float)):
            next_bote = float(next_bote)
        else:
            next_bote = None
        if isinstance(next_premios, (int, float)):
            next_premios = float(next_premios)
        else:
            next_premios = None
        d["next_funds_prediction"] = {
            "bote_stats": {"median": next_bote} if next_bote is not None else {},
            "premios_stats": {"median": next_premios} if next_premios is not None else {},
        }
        items.append(d)
    return JSONResponse(content={"items": items})


def _compute_next_draw_date(lottery_slug: str, last_date_str: str) -> str | None:
    """
    Given the date of the last draw, compute the next draw date for that lottery.

    This approximates the "Próximo sorteo" / closing-of-sales date using only
    the draw dates we already have in the database.
    """
    try:
        d = dt.strptime(last_date_str, "%Y-%m-%d").date()
    except ValueError:
        return None

    wd = d.weekday()  # Monday=0 .. Sunday=6

    if lottery_slug == "euromillones":
        # Euromillones draws Tuesday (1) and Friday (4)
        if wd == 1:  # Tuesday -> next Friday
            delta = 3
        elif wd == 4:  # Friday -> next Tuesday
            delta = 4
        else:
            delta = 1
            while True:
                cand = d + timedelta(days=delta)
                if cand.weekday() in (1, 4):
                    break
                delta += 1
    elif lottery_slug == "la-primitiva":
        # La Primitiva draws Monday (0), Thursday (3) and Saturday (5)
        valid_days = (0, 3, 5)
        if wd in valid_days:
            order = [0, 3, 5]
            idx = order.index(wd)
            next_wd = order[(idx + 1) % len(order)]
            delta = (next_wd - wd) % 7 or 7
        else:
            delta = 1
            while True:
                cand = d + timedelta(days=delta)
                if cand.weekday() in valid_days:
                    break
                delta += 1
    elif lottery_slug == "el-gordo":
        # El Gordo weekly on Sunday (6)
        delta = (6 - wd) % 7 or 7
    else:
        return None

    return (d + timedelta(days=delta)).strftime("%Y-%m-%d")


def _set_last_draw_date(lottery: str, date_str: str) -> None:
    """Set last_draw_date (and next_draw_date) for a lottery in scraper_metadata."""
    if db is None:
        return

    update_doc: dict = {"last_draw_date": date_str}
    next_draw = _compute_next_draw_date(lottery, date_str)
    if next_draw:
        update_doc["next_draw_date"] = next_draw

    db[METADATA_COLLECTION].update_one(
        {"lottery": lottery},
        {"$set": update_doc},
        upsert=True,
    )


def _max_date_from_draws(data: list) -> str | None:
    """Return max fecha_sorteo date (YYYY-MM-DD) from a list of draws."""
    out = None
    for draw in data:
        if not isinstance(draw, dict):
            continue
        f = (draw.get("fecha_sorteo") or "").strip()
        if f:
            d = f.split(" ")[0]
            if d and (out is None or d > out):
                out = d
    return out


def _get_max_draw_date_from_db(game_id: str) -> str | None:
    """Return latest fecha_sorteo date (YYYY-MM-DD) from the DB for a given game_id."""
    if db is None:
        return None
    coll_name = COLLECTIONS.get(game_id)
    if not coll_name:
        return None
    doc = db[coll_name].find_one(sort=[("fecha_sorteo", -1)], projection={"fecha_sorteo": 1})
    if not doc:
        return None
    f = (doc.get("fecha_sorteo") or "").strip()
    if not f:
        return None
    return f.split(" ")[0]


def _save_draws_to_db(data: list) -> tuple[int, list]:
    """Upsert draws into the correct collection (la_primitiva / euromillones / el_gordo)."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    saved = 0
    errors = []
    for draw in data:
        if not isinstance(draw, dict):
            continue
        draw_id = draw.get("id_sorteo")
        game_id = draw.get("game_id")
        if not draw_id or not game_id:
            continue
        coll_name = COLLECTIONS.get(game_id)
        if not coll_name:
            continue
        try:
            doc = normalize_draw(draw)
            db[coll_name].replace_one(
                {"id_sorteo": draw_id},
                doc,
                upsert=True,
            )
            saved += 1
        except PyMongoError as e:
            errors.append(str(e))
    return saved, errors


# game_id -> display name for API responses
GAME_ID_TO_NAME = {"LAPR": "La Primitiva", "EMIL": "Euromillones", "ELGR": "El Gordo"}


def _doc_to_json(doc: dict) -> dict:
    """Convert a MongoDB document to a JSON-serializable dict (all keys from DB)."""
    out = {}
    for k, v in doc.items():
        if k == "_id":
            out[k] = str(v) if isinstance(v, ObjectId) else v
        elif isinstance(v, ObjectId):
            out[k] = str(v)
        elif isinstance(v, dt):
            out[k] = v.isoformat() if hasattr(v, "isoformat") else str(v)
        elif isinstance(v, list):
            out[k] = [_item_to_json(x) for x in v]
        elif isinstance(v, dict):
            out[k] = _doc_to_json(v)
        else:
            out[k] = v
    return out


def _item_to_json(x):
    if isinstance(x, ObjectId):
        return str(x)
    if isinstance(x, dt):
        return x.isoformat() if hasattr(x, "isoformat") else str(x)
    if isinstance(x, dict):
        return _doc_to_json(x)
    if isinstance(x, list):
        return [_item_to_json(i) for i in x]
    return x


# Keys we send to frontend; we always set these from the raw doc so combinacion_acta and escrutinio are never missing
DRAW_KEYS = (
    "id_sorteo",
    "fecha_sorteo",
    "game_id",
    "combinacion",
    "combinacion_acta",
    "numbers",
    "complementario",
    "reintegro",
    "joker_combinacion",
    "premio_bote",
    "escrutinio",
    # Euromillones extra stats (if present in DB)
    "apuestas",  # bets received
    "aquestas",  # fallback name if mis-typed in DB
    "recaudacion",
    "recaudacion_europea",
    "premios",
    "escrutinio_millon",
)


def _build_draw(doc: dict, game_id: str) -> dict:
    """Build one draw for API: always include combinacion_acta and escrutinio from doc."""
    draw = {}
    for k in DRAW_KEYS:
        v = doc.get(k)
        draw[k] = _item_to_json(v) if v is not None else None
    draw["game_id"] = game_id
    draw["game_name"] = GAME_ID_TO_NAME.get(game_id, game_id)
    if draw.get("joker_combinacion") is None:
        millon = doc.get("millon") or {}
        draw["joker_combinacion"] = millon.get("combinacion") if isinstance(millon, dict) else None
    return draw


@app.get("/api/draws")
def get_draws(
    lottery: str = Query(None, description="la-primitiva | euromillones | el-gordo"),
    from_date: str = Query(None, description="YYYY-MM-DD"),
    to_date: str = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
):
    """Fetch draws: use find() then build each draw in Python so combinacion_acta and escrutinio are always returned."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    query = {}
    from_ = (from_date or "").strip()
    to_ = (to_date or "").strip()
    if from_ or to_:
        query["fecha_sorteo"] = {}
        if from_:
            query["fecha_sorteo"]["$gte"] = from_ + " 00:00:00"
        if to_:
            query["fecha_sorteo"]["$lte"] = to_ + " 23:59:59"

    if lottery and lottery in GAME_IDS:
        coll_name = COLLECTIONS.get(GAME_IDS[lottery])
        collections_to_query = [(coll_name, GAME_IDS[lottery])] if coll_name else []
    else:
        collections_to_query = [(name, gid) for gid, name in COLLECTIONS.items()]

    all_draws = []
    total = 0
    one_lottery = len(collections_to_query) == 1

    for coll_name, game_id in collections_to_query:
        cursor = db[coll_name].find(query).sort("fecha_sorteo", -1)
        if one_lottery:
            total = db[coll_name].count_documents(query)
            for doc in cursor.skip(skip).limit(limit):
                all_draws.append(_build_draw(doc, game_id))
        else:
            for doc in cursor:
                total += 1
                all_draws.append(_build_draw(doc, game_id))

    if not one_lottery:
        all_draws.sort(key=lambda d: d.get("fecha_sorteo") or "", reverse=True)
        total = len(all_draws)
        all_draws = all_draws[skip : skip + limit]

    return JSONResponse(content={"draws": all_draws, "total": total})


@app.get("/api/debug/euromillones-one")
def debug_euromillones_one():
    """Return one raw document from euromillones to verify combinacion_acta and escrutinio."""
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    doc = db["euromillones"].find_one(sort=[("fecha_sorteo", -1)])
    if not doc:
        return JSONResponse(content={"error": "No document in euromillones"})
    return JSONResponse(content=_doc_to_json(doc))


@app.get("/api/euromillones/features")
def get_euromillones_features(
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
    draw_id: str | None = Query(
        None,
        description="Optional: filter by draw_id to get a specific feature row.",
    ),
):
    """
    Return per-draw Euromillones feature rows from `euromillones_draw_features`.

    Each document contains:
      - main_numbers, star_numbers
      - draw_date, weekday
      - hot/cold numbers
      - frequency and gap arrays
      - previous-draw snapshot fields
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["euromillones_draw_features"]

    if draw_id:
        cursor = coll.find({"draw_id": draw_id})
        docs = [_doc_to_json(doc) for doc in cursor]
        total = len(docs)
        return JSONResponse(content={"features": docs, "total": total})

    total = coll.count_documents({})
    cursor = coll.find().sort("draw_date", -1).skip(skip).limit(limit)
    docs = [_doc_to_json(doc) for doc in cursor]

    return JSONResponse(content={"features": docs, "total": total})


@app.get("/api/euromillones/feature-model")
def get_euromillones_feature_model(
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
    draw_id: str | None = Query(
        None,
        description="Optional: filter by id_sorteo to get one feature row.",
    ),
):
    """
    Return rows from `euromillones_feature` (new feature model).
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["euromillones_feature"]

    if draw_id:
        cursor = coll.find({"id_sorteo": draw_id})
        docs = [_doc_to_json(doc) for doc in cursor]
        return JSONResponse(content={"features": docs, "total": len(docs)})

    total = coll.count_documents({})
    cursor = coll.find().sort("fecha_sorteo", -1).skip(skip).limit(limit)
    docs = [_doc_to_json(doc) for doc in cursor]
    return JSONResponse(content={"features": docs, "total": total})


@app.get("/api/la-primitiva/features")
def get_la_primitiva_features(
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
    draw_id: str | None = Query(
        None,
        description="Optional: filter by draw_id to get a specific feature row.",
    ),
):
    """
    Return per-draw La Primitiva feature rows from `la_primitiva_draw_features`.

    Each document contains:
      - main_numbers, complementario, reintegro
      - draw_date, weekday
      - hot/cold numbers
      - frequency arrays
      - previous-draw snapshot fields
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["la_primitiva_draw_features"]

    if draw_id:
        cursor = coll.find({"draw_id": draw_id})
        docs = [_doc_to_json(doc) for doc in cursor]
        total = len(docs)
        return JSONResponse(content={"features": docs, "total": total})

    total = coll.count_documents({})
    cursor = coll.find().sort("draw_date", -1).skip(skip).limit(limit)
    docs = [_doc_to_json(doc) for doc in cursor]

    return JSONResponse(content={"features": docs, "total": total})


@app.get("/api/el-gordo/features")
def get_el_gordo_features(
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
    draw_id: str | None = Query(
        None,
        description="Optional: filter by draw_id to get a specific feature row.",
    ),
):
    """
    Return per-draw El Gordo feature rows from `el_gordo_draw_features`.

    Each document contains:
      - main_numbers (5), clave (0-9)
      - draw_date, weekday
      - hot/cold numbers
      - frequency arrays
      - previous-draw snapshot fields
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["el_gordo_draw_features"]

    if draw_id:
        cursor = coll.find({"draw_id": draw_id})
        docs = [_doc_to_json(doc) for doc in cursor]
        total = len(docs)
        return JSONResponse(content={"features": docs, "total": total})

    total = coll.count_documents({})
    cursor = coll.find().sort("draw_date", -1).skip(skip).limit(limit)
    docs = [_doc_to_json(doc) for doc in cursor]

    return JSONResponse(content={"features": docs, "total": total})


@app.get("/api/euromillones/gaps")
def get_euromillones_gaps(
    type: str = Query("main", pattern="^(main|star)$"),
    end_date: str | None = Query(
        None,
        description="YYYY-MM-DD. If not provided, uses today.",
    ),
    window_days: int = Query(
        31,
        ge=1,
        le=365,
        description="Number of days to include ending at end_date.",
    ),
):
    """
    Return per-number appearance history for Euromillones.

    Response format:
      {
        "points": [
          { "type": "main", "number": 3, "draw_index": 12, "date": "2026-01-24" },
          ...
        ]
      }
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    from datetime import datetime, timedelta

    coll = db["euromillones_number_history"]
    docs = list(coll.find({"type": type}))

    points: list[dict] = []

    # Determine time window
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(400, detail="Invalid end_date format, expected YYYY-MM-DD")
    else:
        end_dt = datetime.utcnow()
    start_dt = end_dt - timedelta(days=window_days)

    for doc in docs:
        number = doc.get("number")
        appearances = doc.get("appearances") or []
        for appo in appearances:
            date_str = (appo.get("date") or "").split(" ")[0]
            if not date_str:
                continue
            try:
                app_dt = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                continue
            if not (start_dt <= app_dt <= end_dt):
                continue
            points.append(
                {
                    "type": type,
                    "number": number,
                    "draw_index": appo.get("draw_index"),
                    "date": date_str,
                }
            )

    # Sort points by date ascending so charts have ordered Y-axis
    points.sort(key=lambda p: p.get("date") or "")

    return JSONResponse(content={"points": points})


@app.get("/api/euromillones/apuestas")
def get_euromillones_apuestas(
    window: str = Query(
        "3m",
        pattern="^(2m|3m|6m|1y|all)$",
        description="Time window: last 2m, 3m, 6m, 1y or all history.",
    ),
):
    """
    Time series for Euromillones apuestas / premios / premio_bote.

    Returns draws ordered ascending by fecha_sorteo. Each point:
      {
        "draw_id": "...",
        "date": "YYYY-MM-DD",
        "apuestas": int | null,
        "premios": float | null,
        "premio_bote": float | null
      }
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    from datetime import datetime, timedelta

    coll = db["euromillones"]

    # Determine date window
    if window == "all":
        query: dict = {}
    else:
        today = datetime.utcnow().date()
        if window == "2m":
            delta_days = 60
        elif window == "3m":
            delta_days = 90
        elif window == "6m":
            delta_days = 180
        else:  # "1y"
            delta_days = 365
        start_date = today - timedelta(days=delta_days)
        query = {
            "fecha_sorteo": {
                "$gte": start_date.strftime("%Y-%m-%d") + " 00:00:00",
                "$lte": today.strftime("%Y-%m-%d") + " 23:59:59",
            }
        }

    cursor = coll.find(
        query,
        projection={
            "id_sorteo": 1,
            "fecha_sorteo": 1,
            "apuestas": 1,
            "aquestas": 1,
            "premio_bote": 1,
            "premios": 1,
        },
    ).sort("fecha_sorteo", 1)

    points: list[dict] = []
    for doc in cursor:
        draw_id = str(doc.get("id_sorteo"))
        fecha_full = (doc.get("fecha_sorteo") or "").strip()
        if not draw_id or not fecha_full:
            continue
        date = fecha_full.split(" ")[0]

        raw_apuestas = doc.get("apuestas")
        if raw_apuestas in (None, ""):
            raw_apuestas = doc.get("aquestas")
        try:
            apuestas = int(str(raw_apuestas).replace(".", "").replace(",", "")) if raw_apuestas not in (None, "") else None
        except Exception:
            apuestas = None

        def _to_float(val):
            if val in (None, ""):
                return None
            try:
                s = str(val).replace(".", "").replace(",", ".")
                return float(s)
            except Exception:
                return None

        premios = _to_float(doc.get("premios"))
        if premios is not None:
            # Valores de premios vienen 100x; normalizar a euros reales
            premios = premios / 100.0
        premio_bote = _to_float(doc.get("premio_bote"))

        points.append(
            {
                "draw_id": draw_id,
                "date": date,
                "apuestas": apuestas,
                "premios": premios,
                "premio_bote": premio_bote,
            }
        )

    return JSONResponse(content={"points": points})


def _apuestas_time_series_for_lottery(lottery_slug: str, window: str):
    """
    Helper to build apuestas / premios / premio_bote time series for a given lottery.
    lottery_slug: 'la-primitiva' | 'el-gordo'
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    from datetime import datetime, timedelta

    game_id = GAME_IDS.get(lottery_slug)
    if not game_id:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery_slug}")
    coll_name = COLLECTIONS.get(game_id)
    if not coll_name:
        raise HTTPException(400, detail=f"No collection for lottery: {lottery_slug}")

    coll = db[coll_name]

    # Determine date window
    if window == "all":
        query: dict = {}
    else:
        today = datetime.utcnow().date()
        if window == "2m":
            delta_days = 60
        elif window == "3m":
            delta_days = 90
        elif window == "6m":
            delta_days = 180
        else:  # "1y"
            delta_days = 365
        start_date = today - timedelta(days=delta_days)
        query = {
            "fecha_sorteo": {
                "$gte": start_date.strftime("%Y-%m-%d") + " 00:00:00",
                "$lte": today.strftime("%Y-%m-%d") + " 23:59:59",
            }
        }

    cursor = coll.find(
        query,
        projection={
            "id_sorteo": 1,
            "fecha_sorteo": 1,
            "apuestas": 1,
            "recaudacion": 1,
            "premio_bote": 1,
            "premios": 1,
        },
    ).sort("fecha_sorteo", 1)

    def _to_float(val):
        if val in (None, ""):
            return None
        try:
            s = str(val).replace(".", "").replace(",", ".")
            return float(s)
        except Exception:
            return None

    points: list[dict] = []
    for doc in cursor:
        draw_id = str(doc.get("id_sorteo"))
        fecha_full = (doc.get("fecha_sorteo") or "").strip()
        if not draw_id or not fecha_full:
            continue
        date = fecha_full.split(" ")[0]

        raw_apuestas = doc.get("apuestas")
        try:
            apuestas = int(str(raw_apuestas).replace(".", "").replace(",", "")) if raw_apuestas not in (None, "") else None
        except Exception:
            apuestas = None

        premios = _to_float(doc.get("premios"))
        if premios is not None:
            premios = premios / 100.0
        premio_bote = _to_float(doc.get("premio_bote"))

        points.append(
            {
                "draw_id": draw_id,
                "date": date,
                "apuestas": apuestas,
                "premios": premios,
                "premio_bote": premio_bote,
            }
        )

    return points


def _money_to_float(val) -> Optional[float]:
    """
    Convert Spanish-formatted money string/number to float.

    Examples:
      "1.234.567,89" -> 1234567.89
      "123.456"      -> 123456.0
    """
    if val in (None, ""):
        return None
    try:
        s = str(val).replace(".", "").replace(",", ".")
        return float(s)
    except Exception:
        return None


def _weekday_name(idx: int) -> str:
    names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    return names[idx] if 0 <= idx < 7 else str(idx)


def _predict_next_funds_for_lottery(lottery_slug: str) -> Dict[str, object]:
    """
    Predict only Premios (total premios) for the next draw.
    Próx. bote is not predicted; it comes from scraping the results page.

    Algorithm:
      - Resolve next_draw_date from scraper_metadata (or from last_draw_date / DB).
      - Take historical draws on the same weekday; build premios distribution.
      - Return median and summary stats for premios only.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    game_id = GAME_IDS.get(lottery_slug)
    coll_name = COLLECTIONS.get(game_id) if game_id else None
    if not game_id or not coll_name:
        raise HTTPException(400, detail=f"Unknown lottery: {lottery_slug}")

    meta = db[METADATA_COLLECTION].find_one(
        {"lottery": lottery_slug}, projection={"last_draw_date": 1, "next_draw_date": 1}
    )
    next_date = (meta or {}).get("next_draw_date")
    last_date = (meta or {}).get("last_draw_date")

    if not next_date:
        if isinstance(last_date, str) and last_date:
            next_date = _compute_next_draw_date(lottery_slug, last_date)
        if not next_date:
            latest = _get_max_draw_date_from_db(game_id)
            if latest:
                next_date = _compute_next_draw_date(lottery_slug, latest)
    if not next_date:
        raise HTTPException(400, detail="No se ha podido determinar el próximo sorteo.")

    try:
        next_dt = dt.strptime(next_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, detail=f"Fecha próxima sorteo inválida: {next_date}")

    target_wd = next_dt.weekday()

    coll = db[coll_name]
    cursor = coll.find(
        {},
        projection={"fecha_sorteo": 1, "premios": 1},
    )

    premios_values: List[float] = []

    for doc in cursor:
        fecha_full = (doc.get("fecha_sorteo") or "").strip()
        if not fecha_full:
            continue
        date_str = fecha_full.split(" ")[0]
        try:
            d_date = dt.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        if d_date.weekday() != target_wd:
            continue

        premios_val = _money_to_float(doc.get("premios"))
        if premios_val is not None:
            premios_val = premios_val / 100.0
            premios_values.append(premios_val)

    if not premios_values:
        raise HTTPException(
            400,
            detail=(
                "No hay históricos suficientes para este día de la semana "
                "para calcular una predicción de premios."
            ),
        )

    def _summary(arr: List[float]) -> Dict[str, float]:
        if not arr:
            return {}
        arr_sorted = sorted(arr)
        n = len(arr_sorted)
        mid = n // 2
        if n % 2 == 1:
            median_val = arr_sorted[mid]
        else:
            median_val = 0.5 * (arr_sorted[mid - 1] + arr_sorted[mid])

        def _percentile(p: float) -> float:
            if n == 1:
                return arr_sorted[0]
            k = (n - 1) * p
            f = int(k)
            c = min(f + 1, n - 1)
            if f == c:
                return arr_sorted[f]
            return arr_sorted[f] + (arr_sorted[c] - arr_sorted[f]) * (k - f)

        return {
            "count": float(n),
            "mean": float(mean(arr_sorted)),
            "median": float(median_val),
            "p10": float(_percentile(0.10)),
            "p25": float(_percentile(0.25)),
            "p75": float(_percentile(0.75)),
            "p90": float(_percentile(0.90)),
        }

    premios_stats = _summary(premios_values)

    return {
        "lottery": lottery_slug,
        "next_draw_date": next_date,
        "weekday_index": target_wd,
        "weekday_name": _weekday_name(target_wd),
        "premios_stats": premios_stats,
    }


def _update_next_funds_metadata(
    lottery_slug: str,
    scraped_bote: Optional[float] = None,
) -> Dict[str, object]:
    """
    Persist next-funds as two values in scraper_metadata: next_bote, next_premios.
    Próx. bote: from scraping only (when provided). Premios: from prediction (median).
    Returns a dict with bote_stats.median and premios_stats.median for API compatibility.
    """
    next_bote: Optional[float] = scraped_bote
    next_premios: Optional[float] = None
    next_draw_date: Optional[str] = None

    if db is not None:
        current = db[METADATA_COLLECTION].find_one(
            {"lottery": lottery_slug},
            projection={"next_bote": 1, "next_premios": 1},
        )
        if next_bote is None and current:
            next_bote = current.get("next_bote")
            if isinstance(next_bote, (int, float)):
                next_bote = float(next_bote)
            else:
                next_bote = None

    try:
        pred = _predict_next_funds_for_lottery(lottery_slug)
        premios_stats = pred.get("premios_stats") or {}
        next_premios = premios_stats.get("median")
        next_draw_date = pred.get("next_draw_date")
        if next_premios is not None:
            next_premios = float(next_premios)
    except HTTPException:
        pass

    if db is not None:
        update_doc: dict = {
            "next_funds_updated_at": dt.utcnow(),
        }
        if next_premios is not None:
            update_doc["next_premios"] = next_premios
        if scraped_bote is not None:
            update_doc["next_bote"] = scraped_bote
        db[METADATA_COLLECTION].update_one(
            {"lottery": lottery_slug},
            {"$set": update_doc},
            upsert=True,
        )

    return {
        "lottery": lottery_slug,
        "next_draw_date": next_draw_date,
        "bote_stats": {"median": next_bote} if next_bote is not None else {},
        "premios_stats": {"median": next_premios} if next_premios is not None else {},
    }


@app.get("/api/la-primitiva/apuestas")
def get_la_primitiva_apuestas(
    window: str = Query(
        "3m",
        pattern="^(2m|3m|6m|1y|all)$",
        description="Time window: last 2m, 3m, 6m, 1y or all history.",
    ),
):
    points = _apuestas_time_series_for_lottery("la-primitiva", window)
    return JSONResponse(content={"points": points})


@app.get("/api/el-gordo/apuestas")
def get_el_gordo_apuestas(
    window: str = Query(
        "3m",
        pattern="^(2m|3m|6m|1y|all)$",
        description="Time window: last 2m, 3m, 6m, 1y or all history.",
    ),
):
    points = _apuestas_time_series_for_lottery("el-gordo", window)
    return JSONResponse(content={"points": points})


@app.get("/api/euromillones/prediction/next-funds")
def predict_euromillones_next_funds():
    """
    Return next-funds metadata: Próx. bote from last scrape (no prediction);
    Premios from weekday-conditioned prediction.
    """
    result = _update_next_funds_metadata("euromillones")
    return JSONResponse(content=_item_to_json(result))


@app.get("/api/la-primitiva/prediction/next-funds")
def predict_la_primitiva_next_funds():
    """
    Return next-funds metadata: Próx. bote from last scrape (no prediction);
    Premios from weekday-conditioned prediction.
    """
    result = _update_next_funds_metadata("la-primitiva")
    return JSONResponse(content=_item_to_json(result))


@app.get("/api/el-gordo/prediction/next-funds")
def predict_el_gordo_next_funds():
    """
    Return next-funds metadata: Próx. bote from last scrape (no prediction);
    Premios from weekday-conditioned prediction.
    """
    result = _update_next_funds_metadata("el-gordo")
    return JSONResponse(content=_item_to_json(result))

@app.get("/api/euromillones/number-history")
def get_euromillones_number_history():
    """
    Return full per-number appearance history for Euromillones, grouped by type.

    Response format:
      {
        "main": [
          { "number": 1, "dates": ["2026-01-13", "2026-01-27", ...] },
          ...
        ],
        "star": [
          { "number": 1, "dates": ["2026-01-20", ...] },
          ...
        ]
      }
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["euromillones_number_history"]

    docs = list(
        coll.find(
            {},
            projection={
                "_id": 0,
                "type": 1,
                "number": 1,
                "appearances.date": 1,
            },
        )
    )

    main: list[dict] = []
    star: list[dict] = []

    for doc in docs:
        t = doc.get("type")
        number = doc.get("number")
        appearances = doc.get("appearances") or []

        dates_set: set[str] = set()
        for appo in appearances:
            date_str = (appo.get("date") or "").split(" ")[0]
            if date_str:
                dates_set.add(date_str)

        dates = sorted(dates_set)

        target = main if t == "main" else star if t == "star" else None
        if target is not None:
            target.append({"number": number, "dates": dates})

    return JSONResponse(content={"main": main, "star": star})


@app.post("/api/euromillones/simulation/frequency/train")
def train_euromillones_frequency_models():
    """
    Train / retrain Euromillones frequency-based models (main + stars).

    This can be triggered from the UI. It runs synchronously and may take
    some seconds depending on history size.
    """
    try:
        train_all_frequency_models()
    except Exception as e:
        raise HTTPException(500, detail=f"Error training frequency models: {e}")
    return {"status": "ok"}


@app.get("/api/euromillones/simulation/frequency")
def simulate_euromillones_frequency(
    cutoff_draw_id: str | None = Query(
        None,
        description="Optional draw_id; if provided, simulate as of the draw after this one.",
    )
):
    """
    Run Euromillones frequency-based simulation for the next draw.

    Returns probability per number for mains (1–50) and stars (1–12),
    sorted descending by probability.
    """
    try:
        scores = predict_next_frequency_scores(cutoff_draw_id=cutoff_draw_id)
        sim_id = save_frequency_simulation_result(scores)
    except RuntimeError as e:
        raise HTTPException(500, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=f"Error running frequency simulation: {e}")

    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["euromillones_simulations"]
    from bson import ObjectId  # type: ignore[import-not-found]

    doc = coll.find_one({"_id": ObjectId(sim_id)})
    if not doc:
        raise HTTPException(500, detail="Saved frequency simulation not found")

    public = _doc_to_json(doc)
    public["simulation_id"] = sim_id
    return JSONResponse(content=public)


@app.get("/api/euromillones/simulation/frequency/history")
def get_euromillones_frequency_simulation_history(
    cutoff_draw_id: str | None = Query(
        None,
        description="Optional draw_id to filter simulations by cutoff_draw_id.",
    ),
    limit: int = Query(
        1,
        ge=1,
        le=50,
        description="Maximum number of simulation records to return.",
    ),
):
    """
    Return saved Euromillones frequency simulations.

    If cutoff_draw_id is provided, only simulations for that draw are returned,
    ordered from newest to oldest.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["euromillones_simulations"]
    query: dict = {}
    if cutoff_draw_id:
        query["cutoff_draw_id"] = cutoff_draw_id

    cursor = coll.find(query).sort("created_at", -1).limit(limit)
    docs = [_doc_to_json(doc) for doc in cursor]
    return JSONResponse(content={"simulations": docs})


@app.post("/api/el-gordo/simulation/frequency/train")
def train_el_gordo_frequency_models():
    """
    Train / retrain El Gordo frequency-based models (main + clave).
    """
    try:
        train_all_el_gordo_frequency_models()
    except Exception as e:
        raise HTTPException(500, detail=f"Error training El Gordo frequency models: {e}")
    return {"status": "ok"}


@app.get("/api/el-gordo/simulation/frequency")
def simulate_el_gordo_frequency(
    cutoff_draw_id: str | None = Query(
        None,
        description=(
            "Optional draw_id; if provided, simulate as of the draw after this one."
        ),
    )
):
    """
    Run El Gordo frequency-based simulation for the next draw.
    """
    try:
        scores = predict_next_el_gordo_frequency_scores(cutoff_draw_id=cutoff_draw_id)
        sim_id = save_el_gordo_frequency_simulation_result(scores)
    except RuntimeError as e:
        raise HTTPException(500, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=f"Error running El Gordo frequency simulation: {e}")

    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["el_gordo_simulations"]
    from bson import ObjectId  # type: ignore[import-not-found]

    doc = coll.find_one({"_id": ObjectId(sim_id)})
    if not doc:
        raise HTTPException(500, detail="Saved El Gordo frequency simulation not found")

    public = _doc_to_json(doc)
    public["simulation_id"] = sim_id
    return JSONResponse(content=public)


@app.get("/api/el-gordo/simulation/frequency/history")
def get_el_gordo_frequency_simulation_history(
    cutoff_draw_id: str | None = Query(
        None,
        description="Optional draw_id to filter El Gordo simulations by cutoff_draw_id.",
    ),
    limit: int = Query(
        1,
        ge=1,
        le=50,
        description="Maximum number of simulation records to return.",
    ),
):
    """
    Return saved El Gordo frequency simulations.
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["el_gordo_simulations"]
    query: dict = {}
    if cutoff_draw_id:
        query["cutoff_draw_id"] = cutoff_draw_id

    cursor = coll.find(query).sort("created_at", -1).limit(limit)
    docs = [_doc_to_json(doc) for doc in cursor]
    return JSONResponse(content={"simulations": docs})


@app.get("/api/el-gordo/simulation/simple")
def simulate_el_gordo_simple(
    cutoff_draw_id: str | None = Query(
        None,
        description=(
            "Optional draw_id; if provided, simulate El Gordo as of the draw after this one."
        ),
    )
):
    """
    Run a simple El Gordo simulation (frequency / gap / hot-cold) for the next draw.

    This uses `el_gordo_draw_features` to compute per-number scores and stores the result
    in `el_gordo_simulations` so that the candidate pool and wheeling engines can reuse it.
    """
    try:
        sim_doc = run_el_gordo_simple_simulation(cutoff_draw_id=cutoff_draw_id)
    except RuntimeError as e:
        raise HTTPException(400, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=f"Error running El Gordo simulation: {e}")

    return JSONResponse(content=_doc_to_json(sim_doc))


@app.post("/api/el-gordo/simulation/gap/train")
def train_el_gordo_gap_models():
    """
    Train / retrain El Gordo gap-based models (main + clave).
    """
    try:
        train_all_el_gordo_gap_models()
    except Exception as e:
        raise HTTPException(500, detail=f"Error training El Gordo gap models: {e}")
    return {"status": "ok"}


@app.get("/api/el-gordo/simulation/gap")
def simulate_el_gordo_gap(
    cutoff_draw_id: str | None = Query(
        None,
        description=(
            "Optional draw_id; if provided, simulate as of the draw after this one."
        ),
    )
):
    """
    Run El Gordo gap-based simulation for the next draw.
    """
    try:
        scores = predict_next_el_gordo_gap_scores(cutoff_draw_id=cutoff_draw_id)
        sim_id = save_el_gordo_gap_simulation_result(scores)
    except RuntimeError as e:
        raise HTTPException(500, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=f"Error running El Gordo gap simulation: {e}")

    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["el_gordo_simulations"]
    from bson import ObjectId  # type: ignore[import-not-found]

    doc = coll.find_one({"_id": ObjectId(sim_id)})
    if not doc:
        raise HTTPException(500, detail="Saved El Gordo gap simulation not found")

    public = _doc_to_json(doc)
    public["simulation_id"] = sim_id
    return JSONResponse(content=public)


@app.post("/api/euromillones/simulation/gap/train")
def train_euromillones_gap_models():
    """
    Train / retrain Euromillones gap-based models (main + stars).
    """
    try:
        train_all_gap_models()
    except Exception as e:
        raise HTTPException(500, detail=f"Error training gap models: {e}")
    return {"status": "ok"}


@app.get("/api/euromillones/simulation/gap")
def simulate_euromillones_gap(
    cutoff_draw_id: str | None = Query(
        None,
        description="Optional draw_id; if provided, simulate as of the draw after this one.",
    )
):
    """
    Run Euromillones gap-based simulation for the next draw.
    """
    try:
        scores = predict_next_gap_scores(cutoff_draw_id=cutoff_draw_id)
        sim_id = save_gap_simulation_result(scores)
    except RuntimeError as e:
        raise HTTPException(500, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=f"Error running gap simulation: {e}")

    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["euromillones_simulations"]
    from bson import ObjectId  # type: ignore[import-not-found]

    doc = coll.find_one({"_id": ObjectId(sim_id)})
    if not doc:
        raise HTTPException(500, detail="Saved gap simulation not found")

    public = _doc_to_json(doc)
    public["simulation_id"] = sim_id
    return JSONResponse(content=public)


@app.post("/api/euromillones/simulation/hot/train")
def train_euromillones_hot_models():
    """
    Train / retrain Euromillones hot/cold-based models (main + stars).
    """
    try:
        train_all_hot_models()
    except Exception as e:
        raise HTTPException(500, detail=f"Error training hot/cold models: {e}")
    return {"status": "ok"}


@app.get("/api/euromillones/simulation/hot")
def simulate_euromillones_hot(
    cutoff_draw_id: str | None = Query(
        None,
        description="Optional draw_id; if provided, simulate as of the draw after this one.",
    )
):
    """
    Run Euromillones hot/cold-based simulation for the next draw.
    """
    try:
        scores = predict_next_hot_scores(cutoff_draw_id=cutoff_draw_id)
        sim_id = save_hot_simulation_result(scores)
    except RuntimeError as e:
        raise HTTPException(500, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=f"Error running hot/cold simulation: {e}")

    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["euromillones_simulations"]
    from bson import ObjectId  # type: ignore[import-not-found]

    doc = coll.find_one({"_id": ObjectId(sim_id)})
    if not doc:
        raise HTTPException(500, detail="Saved hot/cold simulation not found")

    public = _doc_to_json(doc)
    public["simulation_id"] = sim_id
    return JSONResponse(content=public)


@app.get("/api/euromillones/simulation/prediction/compare")
def compare_euromillones_prediction_with_result(
    result_draw_id: str = Query(
        ...,
        description=(
            "id_sorteo del sorteo real; se compara contra la simulación de predicción "
            "generada para el sorteo anterior (prev_draw_id)."
        ),
    ),
):
    """
    Comparar la predicción de Euromillones (freq/gap/hot) contra el resultado real.

    Devuelve métricas como número de aciertos en el top-K para mains y stars,
    así como detalles por número para los números que salieron en el sorteo real.
    """
    try:
        result = compare_prediction_with_result(result_draw_id=result_draw_id)
    except RuntimeError as e:
        raise HTTPException(400, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=f"Error comparando predicción de Euromillones: {e}")

    # Ensure datetimes and ObjectIds inside result are JSON serializable
    return JSONResponse(content=_item_to_json(result))


@app.post("/api/el-gordo/simulation/hot/train")
def train_el_gordo_hot_models():
    """
    Train / retrain El Gordo hot/cold-based models (main + clave).
    """
    try:
        train_all_el_gordo_hot_models()
    except Exception as e:
        raise HTTPException(500, detail=f"Error training El Gordo hot/cold models: {e}")
    return {"status": "ok"}


@app.get("/api/el-gordo/simulation/hot")
def simulate_el_gordo_hot(
    cutoff_draw_id: str | None = Query(
        None,
        description=(
            "Optional draw_id; if provided, simulate as of the draw after this one."
        ),
    )
):
    """
    Run El Gordo hot/cold-based simulation for the next draw.
    """
    try:
        scores = predict_next_el_gordo_hot_scores(cutoff_draw_id=cutoff_draw_id)
        sim_id = save_el_gordo_hot_simulation_result(scores)
    except RuntimeError as e:
        raise HTTPException(500, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=f"Error running El Gordo hot/cold simulation: {e}")

    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["el_gordo_simulations"]
    from bson import ObjectId  # type: ignore[import-not-found]

    doc = coll.find_one({"_id": ObjectId(sim_id)})
    if not doc:
        raise HTTPException(500, detail="Saved El Gordo hot/cold simulation not found")

    public = _doc_to_json(doc)
    public["simulation_id"] = sim_id
    return JSONResponse(content=public)


@app.get("/api/euromillones/simulation/candidate-pool")
def get_euromillones_candidate_pool(
    cutoff_draw_id: str | None = Query(
        None,
        description=(
            "Optional draw_id; if provided, candidate pool is built from the "
            "latest simulation document for this cutoff."
        ),
    ),
    k_main: int = Query(
        20,
        ge=1,
        le=50,
        description="Size of main-number candidate pool.",
    ),
    k_star: int = Query(
        6,
        ge=1,
        le=12,
        description="Size of star-number candidate pool.",
    ),
    w_freq_main: float = Query(
        0.4,
        ge=0.0,
        le=1.0,
        description="Weight of frequency probability for main numbers.",
    ),
    w_gap_main: float = Query(
        0.3,
        ge=0.0,
        le=1.0,
        description="Weight of gap probability for main numbers.",
    ),
    w_hot_main: float = Query(
        0.3,
        ge=0.0,
        le=1.0,
        description="Weight of hot/cold probability for main numbers.",
    ),
    w_freq_star: float = Query(
        0.5,
        ge=0.0,
        le=1.0,
        description="Weight of frequency probability for star numbers.",
    ),
    w_gap_star: float = Query(
        0.25,
        ge=0.0,
        le=1.0,
        description="Weight of gap probability for star numbers.",
    ),
    w_hot_star: float = Query(
        0.25,
        ge=0.0,
        le=1.0,
        description="Weight of hot/cold probability for star numbers.",
    ),
):
    """
    Build a Euromillones candidate pool for the wheeling engine.

    Uses the unified `euromillones_simulations` document (freq/gap/hot per number)
    and combines them into a single score per number using the provided weights.

    Default configuration:
      mains: freq=0.4, gap=0.3, hot=0.3, k_main=20
      stars: freq=0.5, gap=0.25, hot=0.25, k_star=6
    """
    try:
        pool = build_candidate_pool(
            cutoff_draw_id=cutoff_draw_id,
            k_main=k_main,
            k_star=k_star,
            main_weights={
                "freq": w_freq_main,
                "gap": w_gap_main,
                "hot": w_hot_main,
            },
            star_weights={
                "freq": w_freq_star,
                "gap": w_gap_star,
                "hot": w_hot_star,
            },
        )
    except ValueError as e:
        raise HTTPException(400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(400, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=f"Error building candidate pool: {e}")

    return JSONResponse(content=pool)


@app.get("/api/el-gordo/simulation/candidate-pool")
def get_el_gordo_candidate_pool(
    cutoff_draw_id: str | None = Query(
        None,
        description=(
            "Optional draw_id; if provided, candidate pool is built from the "
            "latest El Gordo simulation document for this cutoff."
        ),
    ),
    k_main: int = Query(
        20,
        ge=1,
        le=54,
        description="Tamaño del pool de números principales (1–54).",
    ),
    k_clave: int = Query(
        6,
        ge=1,
        le=10,
        description="Tamaño del pool de números clave (0–9).",
    ),
    w_freq_main: float = Query(
        0.4,
        ge=0.0,
        le=1.0,
        description="Peso de frecuencia para números principales.",
    ),
    w_gap_main: float = Query(
        0.3,
        ge=0.0,
        le=1.0,
        description="Peso de gap para números principales.",
    ),
    w_hot_main: float = Query(
        0.3,
        ge=0.0,
        le=1.0,
        description="Peso hot/cold para números principales.",
    ),
    w_freq_clave: float = Query(
        0.4,
        ge=0.0,
        le=1.0,
        description="Peso de frecuencia para número clave.",
    ),
    w_gap_clave: float = Query(
        0.3,
        ge=0.0,
        le=1.0,
        description="Peso de gap para número clave.",
    ),
    w_hot_clave: float = Query(
        0.3,
        ge=0.0,
        le=1.0,
        description="Peso hot/cold para número clave.",
    ),
):
    """
    Build an El Gordo candidate pool for the wheeling engine.
    """
    try:
        pool = build_el_gordo_candidate_pool(
            cutoff_draw_id=cutoff_draw_id,
            k_main=k_main,
            k_clave=k_clave,
            main_weights={
                "freq": w_freq_main,
                "gap": w_gap_main,
                "hot": w_hot_main,
            },
            clave_weights={
                "freq": w_freq_clave,
                "gap": w_gap_clave,
                "hot": w_hot_clave,
            },
        )
    except ValueError as e:
        raise HTTPException(400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(400, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=f"Error building El Gordo candidate pool: {e}")

    return JSONResponse(content=pool)


@app.get("/api/la-primitiva/number-history")
def get_la_primitiva_number_history():
    """
    Return full per-number appearance history for La Primitiva, grouped by type.

    Response format:
      {
        "main": [
          { "number": 1, "dates": ["2026-01-13", ...] },
          ...
        ],
        "complementario": [
          { "number": 1, "dates": ["2026-01-20", ...] },
          ...
        ],
        "reintegro": [
          { "number": 0, "dates": ["2026-01-20", ...] },
          ...
        ]
      }
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["la_primitiva_number_history"]

    docs = list(
        coll.find(
            {},
            projection={
                "_id": 0,
                "type": 1,
                "number": 1,
                "appearances.date": 1,
            },
        )
    )

    main: list[dict] = []
    complementario: list[dict] = []
    reintegro: list[dict] = []

    for doc in docs:
        t = doc.get("type")
        number = doc.get("number")
        appearances = doc.get("appearances") or []

        dates_set: set[str] = set()
        for appo in appearances:
            date_str = (appo.get("date") or "").split(" ")[0]
            if date_str:
                dates_set.add(date_str)

        dates = sorted(dates_set)

        if t == "main":
            main.append({"number": number, "dates": dates})
        elif t == "complementario":
            complementario.append({"number": number, "dates": dates})
        elif t == "reintegro":
            reintegro.append({"number": number, "dates": dates})

    return JSONResponse(
        content={
            "main": main,
            "complementario": complementario,
            "reintegro": reintegro,
        }
    )


@app.get("/api/el-gordo/number-history")
def get_el_gordo_number_history():
    """
    Return full per-number appearance history for El Gordo, grouped by type.

    Response format:
      {
        "main": [ { "number": 1, "dates": ["2026-01-13", ...] }, ... ],
        "clave": [ { "number": 0, "dates": ["2026-01-20", ...] }, ... ]
      }
    """
    if db is None:
        raise HTTPException(500, detail="Database not connected")

    coll = db["el_gordo_number_history"]

    docs = list(
        coll.find(
            {},
            projection={
                "_id": 0,
                "type": 1,
                "number": 1,
                "appearances.date": 1,
            },
        )
    )

    main: list[dict] = []
    clave: list[dict] = []

    for doc in docs:
        t = doc.get("type")
        number = doc.get("number")
        appearances = doc.get("appearances") or []

        dates_set: set[str] = set()
        for appo in appearances:
            date_str = (appo.get("date") or "").split(" ")[0]
            if date_str:
                dates_set.add(date_str)

        dates = sorted(dates_set)

        if t == "main":
            main.append({"number": number, "dates": dates})
        elif t == "clave":
            clave.append({"number": number, "dates": dates})

    return JSONResponse(content={"main": main, "clave": clave})


@app.post("/api/scrape/daily")
def scrape_daily():
    """
    Daily scrape for all lotteries: from (today - 3 days) to today.
    Call at 00:02 via scheduler or manually. Saves/updates draws; updates last_draw_date.
    """
    from datetime import datetime, timedelta
    if db is None:
        raise HTTPException(500, detail="Database not connected")
    today = datetime.now().strftime("%Y-%m-%d")
    today_yyyymmdd = today.replace("-", "")
    from_d = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
    start_yyyymmdd = from_d.replace("-", "")
    results = []
    for lottery in LOTTERY_DAILY_ORDER:
        game_id = GAME_IDS[lottery]
        api_url = (
            f"{BASE_URL}?game_id={game_id}&celebrados=true"
            f"&fechaInicioInclusiva={start_yyyymmdd}&fechaFinInclusiva={today_yyyymmdd}"
        )
        results_path = RESULTS_PATHS.get(lottery, RESULTS_PATHS["la-primitiva"])
        results_page_url = f"{SITE_ORIGIN}{results_path}"
        try:
            data, proximo_bote_eur = _scrape_with_selenium(api_url, results_page_url)
            if not isinstance(data, list):
                results.append({"lottery": lottery, "saved": 0, "message": "Invalid response"})
                continue
            saved, _ = _save_draws_to_db(data)
            max_date = _max_date_from_draws(data)
            if not max_date:
                max_date = _get_max_draw_date_from_db(game_id)
            if max_date:
                _set_last_draw_date(lottery, max_date)
            try:
                _update_next_funds_metadata(lottery, scraped_bote=proximo_bote_eur)
            except Exception:
                pass
            results.append({
                "lottery": lottery,
                "saved": saved,
                "proximo_bote_eur": proximo_bote_eur,
                "message": f"Saved {saved} draws",
            })
        except Exception as e:
            results.append({"lottery": lottery, "saved": 0, "message": str(e)})
    return {"results": results, "date": today}


@app.post("/api/scrape/import")
async def scrape_import(body: list):
    """
    Save draws from pasted JSON (e.g. when the lottery API returns 403).
    Open the buscadorSorteos URL in your browser, copy the JSON array, paste here.
    """
    if not isinstance(body, list):
        raise HTTPException(400, detail="Body must be a JSON array of draws")
    saved, errors = _save_draws_to_db(body)
    return {
        "saved": saved,
        "total": len(body),
        "message": f"Saved {saved} draws to MongoDB.",
        "errors": errors[:5] if errors else None,
    }
