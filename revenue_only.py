"""Staged monthly revenue refresh; never fetch or rewrite daily prices."""

import argparse
import lzma
import shutil
from pathlib import Path

import database
from data_correctness import sync_revenue
from fetch_precompute import refresh_latest_stock_snapshot
from fetch_revenue import update_all_stocks


def update_revenue_only(limit=None, offset=0, progress_path=None):
    rows = update_all_stocks(start_date="2024-01-01", batch_size=50,
                             strict_source=True, max_requests=limit,
                             offset=offset, progress_path=progress_path)
    if rows <= 0:
        print("STOCK_REVENUE_NO_CHANGE: no new monthly revenue; skip publication")
        return False

    conn = database.get_connection()
    try:
        sync_revenue(conn)
        refresh_latest_stock_snapshot(conn)
        conn.commit()
    finally:
        conn.close()

    # The staged runner validates the entire xz against this exact DB before
    # promotion.  Do not VACUUM/prune historical quotes in a revenue-only run.
    with database.DB_PATH.open("rb") as source, lzma.open(database.DB_XZ_PATH, "wb", preset=6) as target:
        shutil.copyfileobj(source, target, length=1024 * 1024)
    print(f"STOCK_REVENUE_UPDATED rows={rows}")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=40)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--progress-path", type=Path, required=True)
    args = parser.parse_args()
    update_revenue_only(args.limit, args.offset, args.progress_path)
