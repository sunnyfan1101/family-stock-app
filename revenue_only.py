"""Staged monthly revenue refresh; never fetch or rewrite daily prices."""

import lzma
import shutil

import database
from data_correctness import sync_revenue
from fetch_precompute import refresh_latest_stock_snapshot
from fetch_revenue import update_all_stocks


def update_revenue_only():
    rows = update_all_stocks(start_date="2024-01-01", batch_size=50)
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
    update_revenue_only()
