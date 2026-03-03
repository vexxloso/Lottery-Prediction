import os
import random
import re
from itertools import combinations
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv  # type: ignore[import-untyped]
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "lottery")

MAX_GENERATED_TICKETS = 3000
EL_GORDO_TICKET_COST = 1.5

# Regex para extraer patrones tipo "5+1" de los textos de categoría
HITS_PATTERN = re.compile(r"(\d)\s*\+\s*(\d)")


def _get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI)


def _load_simulation_for_cutoff(
    cutoff_draw_id: Optional[str],
) -> Tuple[Optional[dict], MongoClient]:
    """
    Load the latest el_gordo_simulations document for a given cutoff_draw_id.
    """
    client = _get_mongo_client()
    db = client[MONGO_DB]
    coll = db["el_gordo_simulations"]

    query: Dict[str, object] = {}
    if cutoff_draw_id is not None:
        query["cutoff_draw_id"] = cutoff_draw_id

    doc = coll.find_one(query, sort=[("created_at", -1)])
    return doc, client


def _build_pair_trio_score_maps(
    db,
    cutoff_draw_index: Optional[int],
) -> Tuple[Dict[Tuple[int, int], float], Dict[Tuple[int, int, int], float]]:
    """
    Build normalised frequency maps for El Gordo main-number pairs and trios
    up to cutoff_draw_index.
    """
    coll = db["el_gordo_pair_trio_history"]

    pair_counts: Dict[Tuple[int, int], int] = {}
    trio_counts: Dict[Tuple[int, int, int], int] = {}
    max_pair = 0
    max_trio = 0

    # Pairs
    for doc in coll.find(
        {"scope": "main", "type": "pair"},
        projection={"combo": 1, "appearances.draw_index": 1},
    ):
        combo = doc.get("combo") or []
        if len(combo) != 2:
            continue
        a, b = sorted(int(x) for x in combo)
        apps = doc.get("appearances") or []
        if cutoff_draw_index is not None:
            count = sum(
                1
                for app in apps
                if isinstance(app.get("draw_index"), int)
                and app["draw_index"] <= cutoff_draw_index
            )
        else:
            count = len(apps)
        if count <= 0:
            continue
        key = (a, b)
        pair_counts[key] = count
        if count > max_pair:
            max_pair = count

    # Trios
    for doc in coll.find(
        {"scope": "main", "type": "trio"},
        projection={"combo": 1, "appearances.draw_index": 1},
    ):
        combo = doc.get("combo") or []
        if len(combo) != 3:
            continue
        a, b, c = sorted(int(x) for x in combo)
        apps = doc.get("appearances") or []
        if cutoff_draw_index is not None:
            count = sum(
                1
                for app in apps
                if isinstance(app.get("draw_index"), int)
                and app["draw_index"] <= cutoff_draw_index
            )
        else:
            count = len(apps)
        if count <= 0:
            continue
        key3 = (a, b, c)
        trio_counts[key3] = count
        if count > max_trio:
            max_trio = count

    pair_scores: Dict[Tuple[int, int], float] = {}
    trio_scores: Dict[Tuple[int, int, int], float] = {}

    if max_pair > 0:
        for k, v in pair_counts.items():
            pair_scores[k] = v / float(max_pair)
    if max_trio > 0:
        for k, v in trio_counts.items():
            trio_scores[k] = v / float(max_trio)

    return pair_scores, trio_scores


def _attach_pair_trio_scores(
    db,
    tickets: List[Dict[str, object]],
    cutoff_draw_index: Optional[int],
) -> List[Dict[str, object]]:
    """
    Enrich El Gordo tickets with pair/trio-based scores using history up to cutoff_draw_index.
    """
    pair_scores, trio_scores = _build_pair_trio_score_maps(db, cutoff_draw_index)

    scored: List[Dict[str, object]] = []
    for t in tickets:
        mains = t.get("mains") or []
        pairs = list(combinations(mains, 2))
        trios = list(combinations(mains, 3))

        avg_pair = 0.0
        if pairs:
            avg_pair = sum(
                pair_scores.get(tuple(sorted(p)), 0.0) for p in pairs
            ) / float(len(pairs))

        avg_trio = 0.0
        if trios:
            avg_trio = sum(
                trio_scores.get(tuple(sorted(tr)), 0.0) for tr in trios
            ) / float(len(trios))

        score_total = 0.5 * avg_pair + 0.5 * avg_trio

        scored.append(
            {
                "mains": mains,
                "clave": t.get("clave"),
                "score_pair": avg_pair,
                "score_trio": avg_trio,
                "score_total": score_total,
            }
        )

    scored.sort(key=lambda x: x.get("score_total", 0.0), reverse=True)
    return scored


def _generate_unique_tickets(
    main_pool: List[int],
    clave_pool: List[int],
    n_tickets: int,
    main_per_ticket: int = 5,
    max_attempts_factor: int = 20,
) -> List[Dict[str, object]]:
    """
    Simple random wheeling generator for El Gordo using the candidate pools.

    Each ticket has:
      - 5 main numbers from main_pool
      - 1 clave number from clave_pool
    """
    if len(main_pool) < main_per_ticket:
        raise ValueError("El pool de números principales es demasiado pequeño.")
    if len(clave_pool) < 1:
        raise ValueError("El pool de números clave es demasiado pequeño.")

    tickets: List[Dict[str, object]] = []
    seen: set[Tuple[Tuple[int, ...], int]] = set()

    max_attempts = n_tickets * max_attempts_factor
    attempts = 0

    while len(tickets) < n_tickets and attempts < max_attempts:
        attempts += 1
        mains = sorted(random.sample(main_pool, main_per_ticket))
        clave = int(random.choice(clave_pool))
        key = (tuple(mains), clave)
        if key in seen:
            continue
        seen.add(key)
        tickets.append({"mains": mains, "clave": clave})

    return tickets


def generate_el_gordo_wheeling_tickets(
    cutoff_draw_id: Optional[str],
    n_tickets: int = 20,
) -> Dict[str, object]:
    """
    Generate El Gordo wheeling tickets from the saved candidate pool.

    This version does NOT apply odd/even filtering; all 5-number combinations
    from the candidate pool are eligible.
    """
    if n_tickets <= 0:
        raise ValueError("El número de boletos debe ser positivo.")
    if n_tickets > MAX_GENERATED_TICKETS:
        raise ValueError(
            f"El número de boletos no puede ser mayor que {MAX_GENERATED_TICKETS}."
        )

    sim_doc, client = _load_simulation_for_cutoff(cutoff_draw_id)
    try:
        if not sim_doc:
            raise RuntimeError(
                "No se ha encontrado ninguna simulación de El Gordo para este sorteo. "
                "Ejecuta primero la simulación y el generador de pool."
            )

        candidate_pool = sim_doc.get("candidate_pool") or {}
        main_pool = list(candidate_pool.get("main_pool") or [])
        clave_pool = list(candidate_pool.get("clave_pool") or [])

        if not main_pool or not clave_pool:
            raise RuntimeError(
                "No hay pool de candidatos guardado para este sorteo. "
                "Genera primero el pool de candidatos."
            )

        # Always generate up to MAX_GENERATED_TICKETS tickets (or fewer if combinations run out),
        # regardless of how many we plan to display in the UI. The n_tickets argument only
        # controls how many are returned in the response, not how many are stored.
        raw_tickets = _generate_unique_tickets(
            main_pool,
            clave_pool,
            n_tickets=MAX_GENERATED_TICKETS,
        )

        cutoff_index_val = sim_doc.get("cutoff_draw_index")
        cutoff_index = int(cutoff_index_val) if isinstance(cutoff_index_val, int) else None

        db = client[MONGO_DB]
        tickets_with_scores = _attach_pair_trio_scores(
            db=db,
            tickets=raw_tickets,
            cutoff_draw_index=cutoff_index,
        )

        coll = db["el_gordo_simulations"]
        coll.update_one(
            {"_id": sim_doc["_id"]},
            {
                "$set": {
                    "wheeling_tickets": tickets_with_scores,
                    "wheeling_ticket_count": len(tickets_with_scores),
                },
                "$currentDate": {"updated_at": True},
            },
        )

        tickets_selected = tickets_with_scores[:n_tickets]

        return {
            "cutoff_draw_id": sim_doc.get("cutoff_draw_id"),
            "cutoff_draw_index": cutoff_index,
            "cutoff_draw_date": sim_doc.get("cutoff_draw_date"),
            "tickets": tickets_selected,
            "count": len(tickets_selected),
        }
    finally:
        client.close()


def _parse_prize_value(val) -> float:
    """
    Convert prize field from escrutinio (string with '.' thousands and ',' decimals) to float.
    """
    if val in (None, ""):
        return 0.0
    try:
        s = str(val).strip()
        s = s.replace(".", "").replace(",", ".")
        return float(s)
    except Exception:
        return 0.0


def compare_el_gordo_wheeling_with_result(
    result_draw_id: str,
    n_tickets: Optional[int] = None,
) -> Dict[str, object]:
    """
    Compare stored El Gordo wheeling tickets against the real result of the next draw.

    Logic mirrors Euromillones compare_wheeling_with_result but adapted to:
      - 5 main numbers + 1 clave
      - ticket cost 1.5 €
    """
    client = _get_mongo_client()
    try:
        db = client[MONGO_DB]

        # 1) Real result and cutoff (previous draw)
        features_coll = db["el_gordo_draw_features"]
        current_doc = features_coll.find_one({"draw_id": result_draw_id})
        if not current_doc:
            raise RuntimeError(
                "No se ha encontrado el sorteo real en el_gordo_draw_features."
            )

        cutoff_draw_id = current_doc.get("prev_draw_id")
        if not cutoff_draw_id:
            raise RuntimeError(
                "El sorteo real no tiene información del sorteo anterior (prev_draw_id)."
            )

        prev_doc = features_coll.find_one({"draw_id": cutoff_draw_id})
        if not prev_doc:
            raise RuntimeError(
                "No se ha encontrado el sorteo anterior en el_gordo_draw_features."
            )

        cutoff_index_val = prev_doc.get("draw_index")
        if not isinstance(cutoff_index_val, int):
            raise RuntimeError("El sorteo anterior no tiene un índice de sorteo válido.")
        cutoff_index = int(cutoff_index_val)

        # 2) Load wheeling simulation for cutoff_draw_id
        sim_coll = db["el_gordo_simulations"]
        sim_doc = sim_coll.find_one(
            {"cutoff_draw_id": cutoff_draw_id},
            sort=[("created_at", -1)],
        )
        if not sim_doc:
            raise RuntimeError(
                "No se ha encontrado ningún resultado de wheeling para el sorteo anterior."
            )

        tickets = sim_doc.get("wheeling_tickets") or []
        if not tickets:
            raise RuntimeError(
                "No hay boletos de wheeling guardados para el sorteo anterior."
            )

        # 3) Check cache
        sim_updated_at = sim_doc.get("updated_at")
        cached_all = (sim_doc.get("wheeling_compare_by_result") or {}).get(result_draw_id)
        if cached_all and cached_all.get("source_updated_at") == sim_updated_at:
            cached_tickets = cached_all.get("tickets") or []
            if n_tickets is not None and n_tickets >= 1:
                cached_tickets = cached_tickets[: int(n_tickets)]

            main_numbers = [int(n) for n in (cached_all.get("result_main_numbers") or [])]
            clave_number = cached_all.get("result_clave")
            result_date = cached_all.get("result_draw_date")
            if len(main_numbers) != 5 or clave_number is None:
                pass
            else:
                categories_summary: Dict[Tuple[int, int], Dict[str, object]] = {}
                total_return = 0.0
                jackpot_hits = 0
                for t in cached_tickets:
                    hm = int(t.get("hits_main") or 0)
                    hc = int(t.get("hits_clave") or 0)
                    prize_val = float(t.get("prize") or 0.0)
                    if hm == 5 and hc == 1:
                        jackpot_hits += 1
                    key = (hm, hc)
                    if key not in categories_summary:
                        categories_summary[key] = {
                            "name": t.get("category") or "",
                            "hits_main": hm,
                            "hits_clave": hc,
                            "count": 0,
                            "prize_per_ticket": prize_val,
                            "total_return": 0.0,
                        }
                    entry = categories_summary[key]
                    entry["count"] = int(entry["count"]) + 1
                    entry["total_return"] = float(entry["total_return"]) + prize_val
                    total_return += prize_val

                categories_list = sorted(
                    categories_summary.values(),
                    key=lambda x: (-int(x["hits_main"]), -int(x["hits_clave"])),
                )

                total_tickets = len(cached_tickets)
                tickets_out = [
                    {
                        "mains": list(t.get("mains") or []),
                        "clave": t.get("clave"),
                    }
                    for t in cached_tickets
                ]

                return {
                    "cutoff_draw_id": cutoff_draw_id,
                    "cutoff_draw_index": cutoff_index,
                    "cutoff_draw_date": prev_doc.get("draw_date"),
                    "result_draw_id": result_draw_id,
                    "result_draw_date": result_date,
                    "result_main_numbers": main_numbers,
                    "result_clave": clave_number,
                    "total_tickets": total_tickets,
                    "jackpot_hits": jackpot_hits,
                    "categories": categories_list,
                    "total_return": total_return,
                    "tickets": tickets_out,
                }

        # No valid cache: continue with full recompute
        if n_tickets is not None and n_tickets >= 1:
            tickets = tickets[: int(n_tickets)]

        # 4) Real result numbers (current draw)
        result_date = current_doc.get("draw_date")
        main_numbers = [int(n) for n in (current_doc.get("main_numbers") or [])]
        clave_val = current_doc.get("clave")
        try:
            clave_number = int(clave_val) if clave_val is not None else None
        except (TypeError, ValueError):
            clave_number = None

        if len(main_numbers) != 5 or clave_number is None:
            raise RuntimeError("El sorteo real no tiene una combinación válida de 5+1.")

        # Read categories and prizes from el_gordo escrutinio
        draws_coll = db["el_gordo"]
        draw_doc = draws_coll.find_one({"id_sorteo": result_draw_id})
        escrutinio = (draw_doc or {}).get("escrutinio") or []

        category_by_hits: Dict[Tuple[int, int], Dict[str, object]] = {}
        for idx, item in enumerate(escrutinio):
            raw_tipo = str(item.get("tipo") or "").strip()
            hm: Optional[int] = None
            hc: Optional[int] = None

            if raw_tipo:
                m = HITS_PATTERN.search(raw_tipo)
                if m:
                    try:
                        hm = int(m.group(1))
                        hc = int(m.group(2))
                    except Exception:
                        hm = hc = None

            if hm is None or hc is None:
                continue

            prize_val = _parse_prize_value(item.get("premio"))
            name = raw_tipo or f"Categoría {item.get('categoria') or (idx + 1)}"
            category_by_hits[(hm, hc)] = {
                "name": name,
                "hits_main": hm,
                "hits_clave": hc,
                "prize": prize_val,
            }

        if not category_by_hits:
            raise RuntimeError(
                "No se han podido leer las categorías de premios del sorteo real de El Gordo."
            )

        main_set = set(main_numbers)
        clave_set = {clave_number}

        total_tickets = len(tickets)
        jackpot_hits = 0
        categories_summary: Dict[Tuple[int, int], Dict[str, object]] = {}
        total_return = 0.0

        # For cache: full analysis over all stored tickets (up to 3000)
        full_tickets = sim_doc.get("wheeling_tickets") or []
        per_ticket_cache: List[Dict[str, object]] = []
        running_prize = 0.0

        # Summary for selected tickets in this response
        for t in tickets:
            mains = set(t.get("mains") or [])
            clave = t.get("clave")
            try:
                clave_int = int(clave) if clave is not None else None
            except (TypeError, ValueError):
                clave_int = None

            hm = len(mains & main_set)
            hc = 1 if clave_int in clave_set else 0

            if hm == 5 and hc == 1:
                jackpot_hits += 1

            cat = category_by_hits.get((hm, hc))
            if not cat:
                continue

            key = (hm, hc)
            entry = categories_summary.get(key)
            if not entry:
                entry = {
                    "name": cat["name"],
                    "hits_main": hm,
                    "hits_clave": hc,
                    "count": 0,
                    "prize_per_ticket": float(cat["prize"]),
                    "total_return": 0.0,
                }
                categories_summary[key] = entry

            entry["count"] = int(entry["count"]) + 1
            entry["total_return"] = float(entry["total_return"]) + float(cat["prize"])
            total_return += float(cat["prize"])

        # Build per-ticket cache for full ticket list (3000)
        for idx, t in enumerate(full_tickets):
            mains_list = list(t.get("mains") or [])
            clave_val_t = t.get("clave")
            try:
                clave_t = int(clave_val_t) if clave_val_t is not None else None
            except (TypeError, ValueError):
                clave_t = None

            mains = set(mains_list)
            hm = len(mains & main_set)
            hc = 1 if clave_t in clave_set else 0

            cat = category_by_hits.get((hm, hc))
            prize_val = float(cat["prize"]) if cat else 0.0
            running_prize += prize_val
            cost_acc = (idx + 1) * EL_GORDO_TICKET_COST
            gain = running_prize - cost_acc
            per_ticket_cache.append(
                {
                    "index": idx + 1,
                    "mains": mains_list,
                    "clave": clave_t,
                    "hits_main": hm,
                    "hits_clave": hc,
                    "category": cat["name"] if cat else "",
                    "prize": prize_val,
                    "prize_accumulated": running_prize,
                    "cost_accumulated": cost_acc,
                    "gain": gain,
                }
            )

        # Store cache in simulation document
        sim_coll.update_one(
            {"_id": sim_doc["_id"]},
            {
                "$set": {
                    f"wheeling_compare_by_result.{result_draw_id}": {
                        "source_updated_at": sim_doc.get("updated_at"),
                        "result_main_numbers": main_numbers,
                        "result_clave": clave_number,
                        "result_draw_date": result_date,
                        "tickets": per_ticket_cache,
                    }
                }
            },
        )

        categories_list = sorted(
            categories_summary.values(),
            key=lambda x: (-int(x["hits_main"]), -int(x["hits_clave"])),
        )

        tickets_out = [
            {
                "mains": list(t.get("mains") or []),
                "clave": t.get("clave"),
            }
            for t in tickets
        ]

        return {
            "cutoff_draw_id": cutoff_draw_id,
            "cutoff_draw_index": cutoff_index,
            "cutoff_draw_date": prev_doc.get("draw_date"),
            "result_draw_id": result_draw_id,
            "result_draw_date": result_date,
            "result_main_numbers": main_numbers,
            "result_clave": clave_number,
            "total_tickets": total_tickets,
            "jackpot_hits": jackpot_hits,
            "categories": categories_list,
            "total_return": total_return,
            "tickets": tickets_out,
        }
    finally:
        client.close()

