import argparse
import os
import random
import re
from itertools import combinations
from pathlib import Path
from typing import Iterable, Iterator, List, Sequence


def normalize_draw_date_to_iso(raw: str) -> str:
    """Normalize a draw date string to YYYY-MM-DD."""
    s = (raw or "").strip()
    if not s:
        raise ValueError("empty draw date")
    s = s.split()[0].replace("/", "-")
    if len(s) >= 10 and s[4:5] == "-" and s[7:8] == "-":
        return s[:10]
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) >= 8:
        d8 = digits[:8]
        return f"{d8[:4]}-{d8[4:6]}-{d8[6:8]}"
    raise ValueError(f"unrecognized draw date: {raw!r}")


def compact_from_iso(iso_yyyy_mm_dd: str) -> str:
    return (iso_yyyy_mm_dd or "").replace("-", "").strip()[:8]


def full_wheel_ticket_id(lottery_token: str, date_compact: str, position: int) -> str:
    return f"{lottery_token}_DRAW_{date_compact}_COMBO_{position}"


def la_primitiva_ticket_tier(mains: Sequence[int]) -> int:
    """
    Returns 0 (best) .. 3 (worst).

    Mirrors the tiering used by the backend for full-wheel reordering:
    - Penalize long consecutive runs.
    - Penalize all-in-one decade or all same last digit.
    - Penalize all odd or all even.
    """
    nums = sorted(int(n) for n in mains)
    score = 0

    # Consecutive runs
    longest_run = 1
    current_run = 1
    for i in range(1, len(nums)):
        if nums[i] == nums[i - 1] + 1:
            current_run += 1
            longest_run = max(longest_run, current_run)
        else:
            current_run = 1
    if longest_run >= 4:
        score += 3
    elif longest_run == 3:
        score += 1

    # Same decade (10s) or same last digit
    decades = {n // 10 for n in nums}
    last_digits = {n % 10 for n in nums}
    if len(decades) == 1:
        score += 2
    if len(last_digits) == 1:
        score += 2

    # All odd or all even
    odds = sum(1 for n in nums if n % 2 == 1)
    evens = len(nums) - odds
    if odds == len(nums) or evens == len(nums):
        score += 2

    if score >= 5:
        return 3
    if score >= 3:
        return 2
    if score >= 1:
        return 1
    return 0


def iter_la_primitiva_tickets_from_pool(mains_pool: Iterable[int]) -> Iterator[List[int]]:
    """
    Yield ALL La Primitiva tickets for the given mains pool (choose 6).

    Note: order is deterministic but intentionally not lexicographic, to avoid
    obvious structures. It shuffles the index-combination list using a seed
    derived from the mains set.
    """
    mains_list: List[int] = sorted(set(int(x) for x in mains_pool))
    if len(mains_list) < 6:
        raise ValueError(f"Need at least 6 mains, got {len(mains_list)}")

    main_idx_combos = list(combinations(range(len(mains_list)), 6))
    seed = hash(tuple(mains_list)) & 0xFFFFFFFF
    rng = random.Random(seed)
    rng.shuffle(main_idx_combos)

    for idxs in main_idx_combos:
        yield [mains_list[i] for i in idxs]


def _parse_mains_arg(raw: str) -> List[int]:
    s = (raw or "").strip()
    if not s:
        return []
    parts = re.split(r"[,\s]+", s)
    out: List[int] = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        if not p.isdigit():
            raise ValueError(f"Invalid mains token: {p!r}")
        out.append(int(p))
    return out


def generate_la_primitiva_fullwheel_file(
    draw_date: str,
    output_dir: str | os.PathLike[str] = "la_primitiva_pools",
    mains_pool: Iterable[int] | None = None,
    *,
    tiering: bool = True,
    lottery_token: str = "LA_PRIMITIVA",
) -> Path:
    """
    Generate La Primitiva full wheel from the given mains pool and save to TXT.

    - Output folder default matches backend: la_primitiva_pools/
    - Filename matches backend: la_primitiva_<YYYYMMDD>.txt
    - Each line matches backend/UI expectations:
        LA_PRIMITIVA_DRAW_<YYYYMMDD>_COMBO_<position>;<position>;<m1,...,m6>
    """
    iso = normalize_draw_date_to_iso(draw_date)
    compact = compact_from_iso(iso)

    if mains_pool is None:
        raise ValueError("mains_pool is required (e.g. 1..49 or a custom set)")

    mains_list = sorted(set(int(x) for x in mains_pool))
    if any(n < 1 or n > 49 for n in mains_list):
        bad = [n for n in mains_list if n < 1 or n > 49][:10]
        raise ValueError(f"All mains must be within 1..49. Bad sample: {bad}")
    if len(mains_list) < 6:
        raise ValueError(f"Need at least 6 distinct mains, got {len(mains_list)}")

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"la_primitiva_{compact}.txt"

    position = 0
    with out_path.open("w", encoding="utf-8", buffering=1024 * 1024) as f:
        if not tiering:
            for mains in iter_la_primitiva_tickets_from_pool(mains_list):
                position += 1
                mains_str = ",".join(str(n) for n in mains)
                tid = full_wheel_ticket_id(lottery_token, compact, position)
                f.write(f"{tid};{position};{mains_str}\n")
        else:
            # Group by tier 0..3, shuffle within small buffers to avoid long structural runs.
            for tier in range(4):
                seed = hash((iso, "la-primitiva", tier)) & 0xFFFFFFFF
                rng = random.Random(seed)
                buffer: List[List[int]] = []
                buffer_size = 1000

                for mains in iter_la_primitiva_tickets_from_pool(mains_list):
                    if la_primitiva_ticket_tier(mains) != tier:
                        continue
                    buffer.append(list(mains))
                    if len(buffer) >= buffer_size:
                        rng.shuffle(buffer)
                        for m in buffer:
                            position += 1
                            mains_str = ",".join(str(n) for n in m)
                            tid = full_wheel_ticket_id(lottery_token, compact, position)
                            f.write(f"{tid};{position};{mains_str}\n")
                        buffer.clear()

                if buffer:
                    rng.shuffle(buffer)
                    for m in buffer:
                        position += 1
                        mains_str = ",".join(str(n) for n in m)
                        tid = full_wheel_ticket_id(lottery_token, compact, position)
                        f.write(f"{tid};{position};{mains_str}\n")

    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Generate La Primitiva full wheel (all 6-number combinations from a mains pool) "
            "into la_primitiva_<YYYYMMDD>.txt.\n\n"
            "WARNING: The full 1..49 pool generates C(49,6)=13,983,816 lines (huge). "
            "Use a smaller pool unless you really want the full domain."
        )
    )
    parser.add_argument(
        "--draw-date",
        required=True,
        help="Draw date (YYYY-MM-DD or YYYYMMDD). Used in filename and COMBO ids.",
    )
    parser.add_argument(
        "--mains",
        required=True,
        help="Comma/space-separated mains pool, e.g. '1,2,3,...,49' or '3 7 12 18 22 41 49'.",
    )
    parser.add_argument(
        "--output-dir",
        default="la_primitiva_pools",
        help="Directory where the TXT file will be written (default: la_primitiva_pools).",
    )
    parser.add_argument(
        "--no-tiering",
        action="store_true",
        help="Write tickets in one pass (no tier grouping). Default uses tiering like the backend.",
    )
    args = parser.parse_args()

    mains_list = _parse_mains_arg(args.mains)
    out_path = generate_la_primitiva_fullwheel_file(
        draw_date=args.draw_date,
        output_dir=args.output_dir,
        mains_pool=mains_list,
        tiering=not bool(args.no_tiering),
    )
    print(f"Written La Primitiva full wheel: {out_path} ({out_path.stat().st_size} bytes)")


if __name__ == "__main__":
    main()

