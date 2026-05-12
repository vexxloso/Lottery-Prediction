"""
Direct fix: find and insert missing La Primitiva tickets.
Finds the first gap in positions and inserts from there.
"""
import os
from itertools import combinations
from datetime import datetime as dt

from dotenv import load_dotenv
from pymongo import MongoClient

_scripts_dir = os.path.dirname(os.path.abspath(__file__))
for _path in [
    os.path.join(_scripts_dir, "..", "backend", ".env"),
    os.path.join(_scripts_dir, "..", ".env"),
]:
    if os.path.isfile(_path):
        load_dotenv(_path)
        break

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "lottery")

MAINS      = list(range(1, 50))
REINS      = list(range(0, 10))
TOTAL      = 139_838_160
BATCH_SIZE = 2000


def tier(mains):
    nums = sorted(mains)
    s = run = cur = 1
    s = 0
    for i in range(1, len(nums)):
        if nums[i] == nums[i-1]+1: cur += 1; run = max(run, cur)
        else: cur = 1
    if run >= 4: s += 3
    elif run == 3: s += 1
    if len({n//10 for n in nums}) == 1: s += 2
    if len({n%10  for n in nums}) == 1: s += 2
    odds = sum(1 for n in nums if n % 2 == 1)
    if odds == len(nums) or odds == 0: s += 2
    return 3 if s >= 5 else 2 if s >= 3 else 1 if s >= 1 else 0


def find_resume_point(coll) -> int:
    """Find the first position gap using binary search on counts."""
    total_in_db = coll.count_documents({"lottery": "la_primitiva"})
    print(f"Documents in DB: {total_in_db:,} / {TOTAL:,}  (missing: {TOTAL - total_in_db:,})")

    if total_in_db >= TOTAL:
        return TOTAL  # nothing to do

    # Binary search: find the boundary where count < position
    lo, hi = 0, TOTAL
    while lo < hi - 1:
        mid = (lo + hi) // 2
        count = coll.count_documents({"lottery": "la_primitiva", "position": {"$lte": mid}})
        if count == mid:
            lo = mid   # all positions up to mid exist
        else:
            hi = mid   # gap starts at or before mid
    print(f"First gap at position: {lo + 1:,}")
    return lo


def main():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    coll = db["la_primitiva_tickets"]

    resume_from = find_resume_point(coll)
    if resume_from >= TOTAL:
        print("Already complete — nothing to insert.")
        client.close()
        return

    print(f"Inserting from position {resume_from + 1:,} to {TOTAL:,} ...")
    now = dt.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    pos = 0
    inserted = 0
    batch = []

    for mains_combo in combinations(MAINS, 6):
        block_end = pos + len(REINS)
        if block_end <= resume_from:
            pos = block_end
            continue
        for rein in REINS:
            pos += 1
            if pos <= resume_from:
                continue
            batch.append({
                "lottery":   "la_primitiva",
                "position":  pos,
                "mains":     list(mains_combo),
                "reintegro": rein,
                "tier":      tier(list(mains_combo)),
            })
            if len(batch) >= BATCH_SIZE:
                try:
                    coll.insert_many(batch, ordered=False)
                    inserted += len(batch)
                except Exception:
                    pass
                batch.clear()
                print(f"  inserted {inserted:,} (pos {pos:,})", end="\r")

    if batch:
        try:
            coll.insert_many(batch, ordered=False)
            inserted += len(batch)
        except Exception:
            pass

    final = coll.count_documents({"lottery": "la_primitiva"})
    print(f"\nDone. Inserted {inserted:,}. Total now: {final:,} / {TOTAL:,}")
    client.close()


if __name__ == "__main__":
    main()
