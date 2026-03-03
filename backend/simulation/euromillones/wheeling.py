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

# Número máximo de boletos que calculamos y guardamos en BD
MAX_GENERATED_TICKETS = 3000
EUROMILLONES_TICKET_COST = 2.5

# Regex para extraer patrones tipo "5+2" de los textos de categoría
HITS_PATTERN = re.compile(r"(\d)\s*\+\s*(\d)")

# Mapeo aproximado de categorías estándar de Euromillones (1..N) a aciertos (mains, stars)
EUROMILLONES_CATEGORY_HITS: Dict[int, Tuple[int, int]] = {
    1: (5, 2),
    2: (5, 1),
    3: (5, 0),
    4: (4, 2),
    5: (4, 1),
    6: (4, 0),
    7: (3, 2),
    8: (2, 2),
    9: (3, 1),
    10: (3, 0),
    11: (1, 2),
    12: (2, 1),
    13: (2, 0),
    14: (0, 2),
}


def _get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI)


def _load_simulation_for_cutoff(
    cutoff_draw_id: Optional[str],
) -> Tuple[Optional[dict], MongoClient]:
    """
    Load the latest euromillones_simulations document for a given cutoff_draw_id.
    """
    client = _get_mongo_client()
    db = client[MONGO_DB]
    coll = db["euromillones_simulations"]

    query: Dict[str, object] = {}
    if cutoff_draw_id is not None:
        query["cutoff_draw_id"] = cutoff_draw_id

    doc = coll.find_one(query, sort=[("created_at", -1)])
    return doc, client


def _generate_unique_tickets(
    main_pool: List[int],
    star_pool: List[int],
    n_tickets: int,
    main_per_ticket: int = 5,
    stars_per_ticket: int = 2,
    max_attempts_factor: int = 20,
) -> List[Dict[str, List[int]]]:
    """
    Simple random wheeling generator using the candidate pools.

    It draws unique tickets (combinations of mains + stars) without repetition.
    """
    if len(main_pool) < main_per_ticket:
        raise ValueError("El pool de números principales es demasiado pequeño.")
    if len(star_pool) < stars_per_ticket:
        raise ValueError("El pool de estrellas es demasiado pequeño.")

    tickets: List[Dict[str, List[int]]] = []
    seen: set[Tuple[Tuple[int, ...], Tuple[int, ...]]] = set()

    max_attempts = n_tickets * max_attempts_factor
    attempts = 0

    while len(tickets) < n_tickets and attempts < max_attempts:
        attempts += 1
        mains = sorted(random.sample(main_pool, main_per_ticket))
        stars = sorted(random.sample(star_pool, stars_per_ticket))
        key = (tuple(mains), tuple(stars))
        if key in seen:
            continue
        seen.add(key)
        tickets.append({"mains": mains, "stars": stars})

    return tickets


def _build_pair_trio_score_maps(
    db,
    cutoff_draw_index: Optional[int],
) -> Tuple[Dict[Tuple[int, int], float], Dict[Tuple[int, int, int], float]]:
    """
    Build normalised frequency maps for main-number pairs and trios up to cutoff_draw_index.
    """
    coll = db["euromillones_pair_trio_history"]

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
    tickets: List[Dict[str, List[int]]],
    cutoff_draw_index: Optional[int],
) -> List[Dict[str, object]]:
    """
    Enrich tickets with pair/trio-based scores using history up to cutoff_draw_index.
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

        # Simple equal-weight combination; can be tuned later.
        score_total = 0.5 * avg_pair + 0.5 * avg_trio

        scored.append(
            {
                "mains": mains,
                "stars": t.get("stars") or [],
                "score_pair": avg_pair,
                "score_trio": avg_trio,
                "score_total": score_total,
            }
        )

    scored.sort(key=lambda x: x.get("score_total", 0.0), reverse=True)
    return scored


def generate_wheeling_tickets(
    cutoff_draw_id: Optional[str],
    n_tickets: int = 20,
) -> Dict[str, object]:
    """
    Generate a set of Euromillones wheeling tickets based on the candidate pool.

    The tickets are also stored back into the `euromillones_simulations` document
    under the `wheeling_tickets` field, including pair/trio-based scores.
    """
    if n_tickets <= 0:
        raise ValueError("El número de boletos debe ser positivo.")
    if n_tickets > MAX_GENERATED_TICKETS:
        raise ValueError(f"El número de boletos no puede ser mayor que {MAX_GENERATED_TICKETS}.")

    sim_doc, client = _load_simulation_for_cutoff(cutoff_draw_id)
    try:
        if not sim_doc:
            raise RuntimeError(
                "No se ha encontrado ninguna simulación para este sorteo. "
                "Ejecuta primero la simulación y el generador de pool."
            )

        candidate_pool = sim_doc.get("candidate_pool") or {}
        main_pool = list(candidate_pool.get("main_pool") or [])
        star_pool = list(candidate_pool.get("star_pool") or [])

        if not main_pool or not star_pool:
            raise RuntimeError(
                "No hay pool de candidatos guardado para este sorteo. "
                "Genera primero el pool de candidatos."
            )

        # Siempre generamos hasta MAX_GENERATED_TICKETS (o menos si no es posible)
        raw_tickets = _generate_unique_tickets(
            main_pool,
            star_pool,
            n_tickets=min(MAX_GENERATED_TICKETS, n_tickets),
        )

        # Regla adicional: ignorar boletos cuyo conteo de pares/impares en los
        # números principales sea demasiado desequilibrado.
        # Solo mantenemos boletos con al menos 2 impares y al menos 2 pares.
        filtered_tickets: List[Dict[str, List[int]]] = []
        for t in raw_tickets:
            mains = t.get("mains") or []
            odd_count = sum(1 for n in mains if n % 2 == 1)
            even_count = sum(1 for n in mains if n % 2 == 0)
            if odd_count < 2 or even_count < 2:
                continue
            filtered_tickets.append(t)

        cutoff_index_val = sim_doc.get("cutoff_draw_index")
        cutoff_index = int(cutoff_index_val) if isinstance(cutoff_index_val, int) else None

        db = client[MONGO_DB]
        tickets_with_scores = _attach_pair_trio_scores(
            db=db,
            tickets=filtered_tickets,
            cutoff_draw_index=cutoff_index,
        )

        coll = db["euromillones_simulations"]
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

        # Solo devolvemos al cliente los primeros n_tickets ordenados por score_total
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
        # remove thousand separators and normalise decimal
        s = s.replace(".", "").replace(",", ".")
        return float(s)
    except Exception:
        return 0.0


def compare_wheeling_with_result(
    result_draw_id: str,
    n_tickets: Optional[int] = None,
) -> Dict[str, object]:
    """
    Compare stored wheeling tickets against the real result of the next draw.

    If n_tickets is set, only the first n_tickets are used (simulates buying that many).

    Uses:
      - wheeling_tickets from euromillones_simulations
      - next draw after cutoff_draw_index from euromillones_draw_features
      - prize categories from euromillones.escrutinio
    """
    client = _get_mongo_client()
    try:
        db = client[MONGO_DB]

        # 1) Find the current draw (result) and its draw_index
        features_coll = db["euromillones_draw_features"]
        current_doc = features_coll.find_one({"draw_id": result_draw_id})
        if not current_doc:
            raise RuntimeError(
                "No se ha encontrado el sorteo real en euromillones_draw_features."
            )

        # Use prev_draw_id stored in the current feature row to locate the cutoff draw
        cutoff_draw_id = current_doc.get("prev_draw_id")
        if not cutoff_draw_id:
            raise RuntimeError(
                "El sorteo real no tiene información del sorteo anterior (prev_draw_id)."
            )

        # Load previous draw document to obtain its draw_index and date
        prev_doc = features_coll.find_one({"draw_id": cutoff_draw_id})
        if not prev_doc:
            raise RuntimeError(
                "No se ha encontrado el sorteo anterior en euromillones_draw_features."
            )

        cutoff_index_val = prev_doc.get("draw_index")
        if not isinstance(cutoff_index_val, int):
            raise RuntimeError("El sorteo anterior no tiene un índice de sorteo válido.")
        cutoff_index = int(cutoff_index_val)

        # 3) Load wheeling simulation for the previous draw (cutoff_draw_id)
        sim_coll = db["euromillones_simulations"]
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

        # Check for cached comparison for this result_draw_id that matches current tickets
        sim_updated_at = sim_doc.get("updated_at")
        cached_all = (sim_doc.get("wheeling_compare_by_result") or {}).get(result_draw_id)
        if cached_all and cached_all.get("source_updated_at") == sim_updated_at:
            cached_tickets = cached_all.get("tickets") or []
            if n_tickets is not None and n_tickets >= 1:
                cached_tickets = cached_tickets[: int(n_tickets)]

            main_numbers = [int(n) for n in (cached_all.get("result_main_numbers") or [])]
            star_numbers = [int(s) for s in (cached_all.get("result_star_numbers") or [])]
            result_date = cached_all.get("result_draw_date")
            if len(main_numbers) != 5 or len(star_numbers) != 2:
                # If cached shape is unexpected, fall back to full recompute
                pass
            else:
                # Rebuild categories summary from cached per-ticket hits/prize
                categories_summary: Dict[Tuple[int, int], Dict[str, object]] = {}
                total_return = 0.0
                jackpot_hits = 0
                for t in cached_tickets:
                    hm = int(t.get("hits_main") or 0)
                    hs = int(t.get("hits_star") or 0)
                    prize_val = float(t.get("prize") or 0.0)
                    if hm == 5 and hs == 2:
                        jackpot_hits += 1
                    key = (hm, hs)
                    if key not in categories_summary:
                        categories_summary[key] = {
                            "name": t.get("category") or "",
                            "hits_main": hm,
                            "hits_star": hs,
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
                    key=lambda x: (-int(x["hits_main"]), -int(x["hits_star"])),
                )

                total_tickets = len(cached_tickets)
                tickets_out = [
                    {
                        "mains": list(t.get("mains") or []),
                        "stars": list(t.get("stars") or []),
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
                    "result_star_numbers": star_numbers,
                    "total_tickets": total_tickets,
                    "jackpot_hits": jackpot_hits,
                    "categories": categories_list,
                    "total_return": total_return,
                    "tickets": tickets_out,
                }

        # No valid cache: continue with full recompute below
        if n_tickets is not None and n_tickets >= 1:
            tickets = tickets[: int(n_tickets)]

        # 4) Real result numbers (current draw)
        result_date = current_doc.get("draw_date")
        main_numbers = [int(n) for n in (current_doc.get("main_numbers") or [])]
        star_numbers = [int(s) for s in (current_doc.get("star_numbers") or [])]
        if len(main_numbers) != 5 or len(star_numbers) != 2:
            raise RuntimeError("El sorteo real no tiene una combinación válida de 5+2.")

        # Leer categorías y premios reales desde la colección euromillones
        draws_coll = db["euromillones"]
        draw_doc = draws_coll.find_one({"id_sorteo": result_draw_id})
        escrutinio = (draw_doc or {}).get("escrutinio") or []

        category_by_hits: Dict[Tuple[int, int], Dict[str, object]] = {}
        for idx, item in enumerate(escrutinio):
            raw_tipo = str(item.get("tipo") or "").strip()
            hm: Optional[int] = None
            hs: Optional[int] = None

            # 1) Intentar extraer "5+2" desde el texto tipo
            if raw_tipo:
                m = HITS_PATTERN.search(raw_tipo)
                if m:
                    try:
                        hm = int(m.group(1))
                        hs = int(m.group(2))
                    except Exception:
                        hm = hs = None

            # 2) Si no hay patrón, probar con el índice de categoría estándar
            if hm is None or hs is None:
                cat_raw = item.get("categoria")
                try:
                    cat_idx = int(cat_raw)
                except Exception:
                    cat_idx = idx + 1  # fallback incremental
                mapping = EUROMILLONES_CATEGORY_HITS.get(cat_idx)
                if mapping:
                    hm, hs = mapping

            if hm is None or hs is None:
                # Skip rows that do not encode a valid hits pattern
                continue

            prize_val = _parse_prize_value(item.get("premio"))
            # Use "tipo" as human label, fall back to categoria index
            name = raw_tipo or f"Categoría {item.get('categoria') or (idx + 1)}"
            category_by_hits[(hm, hs)] = {
                "name": name,
                "hits_main": hm,
                "hits_star": hs,
                "prize": prize_val,
            }

        if not category_by_hits:
            # We can still compute hit counts but we won't know real prize money
            raise RuntimeError("No se han podido leer las categorías de premios del sorteo real.")

        main_set = set(main_numbers)
        star_set = set(star_numbers)

        total_tickets = len(tickets)
        jackpot_hits = 0
        categories_summary: Dict[Tuple[int, int], Dict[str, object]] = {}
        total_return = 0.0

        # For caching: full 3000-ticket analysis (independent of n_tickets)
        full_tickets = sim_doc.get("wheeling_tickets") or []
        per_ticket_cache: List[Dict[str, object]] = []
        running_prize = 0.0

        # Summary over selected tickets for current response
        for t in tickets:
            mains = set(t.get("mains") or [])
            stars = set(t.get("stars") or [])
            hm = len(mains & main_set)
            hs = len(stars & star_set)

            # Jackpot in Euromillones is 5+2
            if hm == 5 and hs == 2:
                jackpot_hits += 1

            cat = category_by_hits.get((hm, hs))
            if not cat:
                continue

            key = (hm, hs)
            entry = categories_summary.get(key)
            if not entry:
                entry = {
                    "name": cat["name"],
                    "hits_main": hm,
                    "hits_star": hs,
                    "count": 0,
                    "prize_per_ticket": float(cat["prize"]),
                    "total_return": 0.0,
                }
                categories_summary[key] = entry

            entry["count"] = int(entry["count"]) + 1
            entry["total_return"] = float(entry["total_return"]) + float(cat["prize"])
            total_return += float(cat["prize"])

        # Build cached per-ticket data for the full ticket list (up to 3000)
        for idx, t in enumerate(full_tickets):
            mains_list = list(t.get("mains") or [])
            stars_list = list(t.get("stars") or [])
            mains = set(mains_list)
            stars = set(stars_list)
            hm = len(mains & main_set)
            hs = len(stars & star_set)
            cat = category_by_hits.get((hm, hs))
            prize_val = float(cat["prize"]) if cat else 0.0
            running_prize += prize_val
            cost_acc = (idx + 1) * EUROMILLONES_TICKET_COST
            gain = running_prize - cost_acc
            per_ticket_cache.append(
                {
                    "index": idx + 1,
                    "mains": mains_list,
                    "stars": stars_list,
                    "hits_main": hm,
                    "hits_star": hs,
                    "category": cat["name"] if cat else None,
                    "prize": prize_val,
                    "prize_accumulated": running_prize,
                    "cost_accumulated": cost_acc,
                    "gain": gain,
                }
            )

        # Save cache for this result_draw_id for future compare calls
        try:
            sim_coll.update_one(
                {"_id": sim_doc["_id"]},
                {
                    "$set": {
                        f"wheeling_compare_by_result.{result_draw_id}": {
                            "source_updated_at": sim_updated_at,
                            "result_draw_id": result_draw_id,
                            "cutoff_draw_id": cutoff_draw_id,
                            "result_draw_date": result_date,
                            "result_main_numbers": main_numbers,
                            "result_star_numbers": star_numbers,
                            "ticket_cost": EUROMILLONES_TICKET_COST,
                            "n_tickets": len(full_tickets),
                            "tickets": per_ticket_cache,
                        }
                    }
                },
            )
        except Exception:
            # Cache write failure shouldn't break the compare API
            pass

        categories_list = sorted(
            categories_summary.values(),
            key=lambda x: (-int(x["hits_main"]), -int(x["hits_star"])),
        )

        tickets_out = [
            {"mains": list(t.get("mains") or []), "stars": list(t.get("stars") or [])}
            for t in tickets
        ]

        return {
            "cutoff_draw_id": cutoff_draw_id,
            "cutoff_draw_index": cutoff_index,
            "cutoff_draw_date": prev_doc.get("draw_date"),
            "result_draw_id": result_draw_id,
            "result_draw_date": result_date,
            "result_main_numbers": main_numbers,
            "result_star_numbers": star_numbers,
            "total_tickets": total_tickets,
            "jackpot_hits": jackpot_hits,
            "categories": categories_list,
            "total_return": total_return,
            "tickets": tickets_out,
        }
    finally:
        client.close()


