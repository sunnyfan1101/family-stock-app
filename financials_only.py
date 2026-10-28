"""Quarterly FinMind archive on a staged DB; never fetch or rewrite quotes.

This is an opt-in runner, not part of the production cron. One request per
stock. A failed source response aborts the stage instead of masquerading as
"no new reports". Stock summary metrics remain owned by the existing flow.
"""

import argparse
import lzma
import shutil
from datetime import date, datetime

import requests

import database
from financial_cursor import reserve_requests, save as write_progress
from fetch_fundamentals_finmind import FINMIND_API_URL, FINMIND_TOKEN
from fetch_precompute import refresh_latest_stock_snapshot


METRICS = {
    "Revenue": "revenue",
    "GrossProfit": "gross_profit",
    "OperatingIncome": "operating_income",
    "PreTaxIncome": "pretax_income",
    "IncomeAfterTaxes": "net_income",
    "EPS": "eps",
}
MONETARY_METRICS = set(METRICS.values()) - {"eps"}
FIELDS = tuple(METRICS.values()) + (
    "gross_margin", "operating_margin", "pretax_margin", "net_margin"
)


def report_period(raw_date):
    period = date.fromisoformat(raw_date)
    if (period.month, period.day) not in ((3, 31), (6, 30), (9, 30), (12, 31)):
        raise ValueError("unexpected financial-report period")
    return period.year, (period.month - 1) // 3 + 1


def fetch_reports(stock_id, start_date="2024-01-01", session=requests):
    params = {"dataset": "TaiwanStockFinancialStatements", "data_id": stock_id,
              "start_date": start_date}
    headers = {"Authorization": "Bearer " + FINMIND_TOKEN} if FINMIND_TOKEN else None
    try:
        response = session.get(FINMIND_API_URL, params=params, headers=headers, timeout=20)
        response.raise_for_status()
    except requests.RequestException:
        # HTTP exceptions may contain the request URL and its token query
        # parameter. Never echo that exception into a cron log.
        raise RuntimeError("FinMind quarterly source request failed") from None
    try:
        body = response.json()
    except ValueError:
        raise ValueError("FinMind quarterly response is not JSON") from None
    if not isinstance(body, dict) or not isinstance(body.get("data"), list):
        raise ValueError("FinMind financial response has no data list")
    if body.get("status") not in (None, 200):
        raise ValueError("FinMind financial response reported an error")
    periods = {}
    for item in body["data"]:
        metric = METRICS.get(item.get("type"))
        if metric is None:
            continue
        key = report_period(item["date"])
        value = item.get("value")
        if value is None:
            continue
        numeric = float(value)
        if not (-float("inf") < numeric < float("inf")):
            raise ValueError("non-finite financial value")
        # FinMind financial-statement amounts are TWD, whereas this DB's
        # financial_reports monetary columns are explicitly TWD thousands.
        # EPS is per-share TWD and must not be scaled.
        if metric in MONETARY_METRICS:
            numeric /= 1000
        row = periods.setdefault(key, {})
        if metric in row and row[metric] != numeric:
            raise ValueError("conflicting financial report rows")
        row[metric] = numeric
    for row in periods.values():
        revenue = row.get("revenue")
        if revenue not in (None, 0):
            for amount, ratio in (("gross_profit", "gross_margin"),
                                  ("operating_income", "operating_margin"),
                                  ("pretax_income", "pretax_margin"),
                                  ("net_income", "net_margin")):
                if amount in row:
                    row[ratio] = row[amount] / revenue * 100
    return periods


def store_reports(connection, stock_id, periods):
    changed = 0
    columns = ",".join(FIELDS)
    for (year, quarter), values in sorted(periods.items()):
        if not values:
            continue
        existing = connection.execute(
            f"SELECT {columns} FROM financial_reports WHERE stock_id=? AND year=? AND quarter=?",
            (stock_id, year, quarter),
        ).fetchone()
        # Missing source fields preserve prior values; never replace with zero.
        merged = tuple(values.get(field, old if existing else None)
                       for field, old in zip(FIELDS, existing or (None,) * len(FIELDS)))
        if existing == merged:
            continue
        connection.execute(
            f"INSERT INTO financial_reports(stock_id,year,quarter,{columns},updated_at) "
            f"VALUES (?,?,?,{','.join('?' for _ in FIELDS)},?) "
            "ON CONFLICT(stock_id,year,quarter) DO UPDATE SET "
            + ",".join(f"{field}=excluded.{field}" for field in FIELDS)
            + ",updated_at=excluded.updated_at",
            (stock_id, year, quarter, *merged, datetime.now().isoformat(timespec="seconds")),
        )
        changed += 1
    return changed


def sync_quarterly_summary(connection, stock_id):
    """Update latest reported margins without replacing adjusted EPS/PE.

    FinMind warns that historical per-share figures may need retrospective
    adjustment after splits/bonus issues. Summing raw quarters into a TTM EPS
    would silently replace the existing stock-level EPS and PE with an
    incomparable value. Keep the raw quarterly EPS in financial_reports until
    an adjusted, comparable series is available.
    """
    reports = connection.execute(
        "SELECT gross_margin,operating_margin,pretax_margin,net_margin "
        "FROM financial_reports WHERE stock_id=? ORDER BY year DESC,quarter DESC LIMIT 1",
        (stock_id,),
    ).fetchall()
    if not reports:
        return
    values = dict(zip(("gross_margin", "operating_margin", "pretax_margin", "net_margin"), reports[0]))
    updates = {key: value for key, value in values.items() if value is not None}
    if updates:
        connection.execute(
            "UPDATE stocks SET " + ",".join(f"{key}=?" for key in updates) + " WHERE stock_id=?",
            (*updates.values(), stock_id),
        )


def update_financials_only(limit=150, offset=0, start_date="2024-01-01",
                           fetcher=fetch_reports, progress_path=None, budget_path=None):
    if limit < 1 or limit > 250 or offset < 0:
        raise ValueError("batch limit must be 1..250 and offset nonnegative")
    connection = database.get_connection()
    try:
        total = connection.execute(
            "SELECT COUNT(*) FROM stocks WHERE stock_id NOT LIKE '00%'"
        ).fetchone()[0]
        effective_offset = 0 if offset >= total else offset
        ids = [row[0] for row in connection.execute(
            "SELECT stock_id FROM stocks WHERE stock_id NOT LIKE '00%' "
            "ORDER BY stock_id LIMIT ? OFFSET ?", (limit, effective_offset)
        )]
        next_offset = 0 if effective_offset + len(ids) >= total else effective_offset + len(ids)
        if budget_path is not None:
            reserve_requests(budget_path, len(ids))
        changed = 0
        changed_ids = []
        for stock_id in ids:
            # All requests finish before commit. Failure leaves this staged DB
            # unpublished and the caller exits nonzero.
            stock_changes = store_reports(connection, stock_id, fetcher(stock_id, start_date))
            changed += stock_changes
            if stock_changes:
                changed_ids.append(stock_id)
        for stock_id in changed_ids:
            sync_quarterly_summary(connection, stock_id)
        if changed:
            refresh_latest_stock_snapshot(connection)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    batch = {"offset": offset, "next_offset": next_offset,
             "checked": len(ids), "total": total}
    if not changed:
        if progress_path is not None:
            write_progress(progress_path, batch)
        print(f"STOCK_FINANCIALS_NO_CHANGE checked={len(ids)} offset={offset}")
        return False
    with database.DB_PATH.open("rb") as source, lzma.open(database.DB_XZ_PATH, "wb", preset=6) as target:
        shutil.copyfileobj(source, target, length=1024 * 1024)
    if progress_path is not None:
        write_progress(progress_path, batch)
    print(f"STOCK_FINANCIALS_UPDATED rows={changed} checked={len(ids)} offset={offset}")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=150)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--progress-path")
    parser.add_argument("--budget-path")
    args = parser.parse_args()
    update_financials_only(args.limit, args.offset, args.start_date,
                           progress_path=args.progress_path, budget_path=args.budget_path)
