import argparse
import os
from itertools import combinations
from pathlib import Path
from typing import Iterable, List, Tuple


def iter_euromillones_tickets(
    mains_pool: Iterable[int],
    stars_pool: Iterable[int],
) -> Iterable[Tuple[int, Tuple[int, ...], Tuple[int, ...]]]:
    """
    Yield all Euromillones tickets for the given pools.

    Each ticket is:
    - 5 distinct mains from mains_pool
    - 2 distinct stars from stars_pool

    Yields tuples of (position, mains_tuple, stars_tuple), where
    position is 1-based and increases with the generation order.
    """
    mains_list: List[int] = sorted(set(int(x) for x in mains_pool))
    stars_list: List[int] = sorted(set(int(x) for x in stars_pool))

    if len(mains_list) < 5:
        raise ValueError(f"Need at least 5 mains, got {len(mains_list)}")
    if len(stars_list) < 2:
        raise ValueError(f"Need at least 2 stars, got {len(stars_list)}")

    pos = 0
    for mains in combinations(mains_list, 5):
        for stars in combinations(stars_list, 2):
            pos += 1
            yield pos, mains, stars


def generate_euromillones_file(
    draw_date: str,
    output_dir: str | os.PathLike[str],
    mains_pool: Iterable[int] | None = None,
    stars_pool: Iterable[int] | None = None,
) -> Path:
    """
    Generate ALL Euromillones tickets for the given pools and save to a TXT file.

    - Filename: euromillones_<draw_date>.txt  (e.g. euromillones_2026-03-08.txt)
    - One line per ticket:
        position;m1,m2,m3,m4,m5;s1,s2

    By default:
    - mains_pool = 1..50
    - stars_pool = 1..12

    IMPORTANT: Using the full 1..50 and 1..12 domain generates 139,838,160 tickets.
    This is a heavy operation and should be run offline, not inside a web request.
    """
    if mains_pool is None:
        mains_pool = range(1, 51)
    if stars_pool is None:
        stars_pool = range(1, 13)

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"euromillones_{draw_date}.txt"

    # Stream tickets directly to disk to avoid keeping them in memory.
    with out_path.open("w", encoding="utf-8", buffering=1024 * 1024) as f:
        for position, mains, stars in iter_euromillones_tickets(mains_pool, stars_pool):
            mains_str = ",".join(str(n) for n in mains)
            stars_str = ",".join(str(n) for n in stars)
            f.write(f"{position};{mains_str};{stars_str}\n")

    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Generate ALL Euromillones tickets for a given draw date and save to "
            "a TXT file named euromillones_<YYYY-MM-DD>.txt."
        )
    )
    parser.add_argument(
        "--draw-date",
        required=True,
        help="Draw date in YYYY-MM-DD format (used only in the filename).",
    )
    parser.add_argument(
        "--output-dir",
        default="euromillones_pools",
        help="Directory where the TXT file will be written (default: euromillones_pools).",
    )
    args = parser.parse_args()

    out_path = generate_euromillones_file(
        draw_date=args.draw_date,
        output_dir=args.output_dir,
    )
    print(f"Written Euromillones ticket file: {out_path} ({out_path.stat().st_size} bytes)")


if __name__ == "__main__":
    main()

