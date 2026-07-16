#!/usr/bin/env python3
"""
db_health_check.py - validate the local stock database before publishing it.
"""

import lzma
import sqlite3
import sys
from datetime import datetime, time
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen


PROJECT_DIR = Path(__file__).resolve().parent
DB_PATH = PROJECT_DIR / "stock_data.db"
DB_XZ_PATH = PROJECT_DIR / "stock_data.db.xz"

MIN_STOCKS = 2000
MIN_DAILY_PRICES = 1_000_000
MIN_LATEST_DAY_STOCKS = 1500
MAX_PRICE_AGE_DAYS = 10

MIN_NONZERO_RATIOS = {
    "revenue_growth": 0.50,
    "gross_margin": 0.50,
    "position_1y": 0.80,
    "vol_spike": 0.80,
}


def fail(message):
    print(f"❌ {message}")
    return False


def ok(message):
    print(f"✅ {message}")
    return True


def check_compressed_db():
    if not DB_XZ_PATH.exists():
        return fail("找不到 stock_data.db.xz")

    try:
        with lzma.open(DB_XZ_PATH, "rb") as f:
            header = f.read(16)
    except Exception as exc:
        return fail(f"stock_data.db.xz 無法解壓讀取: {exc}")

    if header != b"SQLite format 3\x00":
        return fail("stock_data.db.xz 解壓後不是 SQLite database")

    return ok("stock_data.db.xz 可解壓且格式正確")


def fetch_one(cursor, sql, params=()):
    cursor.execute(sql, params)
    return cursor.fetchone()[0]


def twse_has_today_data(today):
    query = urlencode({
        "date": today.strftime("%Y%m%d"),
        "type": "ALLBUT0999",
        "response": "json",
    })
    url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?{query}"
    try:
        with urlopen(url, timeout=15) as response:
            body = response.read().decode("utf-8", errors="ignore")
    except Exception as exc:
        print(f"⚠️ 無法確認 TWSE 今日資料狀態，略過今日性硬檢查: {exc}")
        return False

    return '"stat":"OK"' in body or '"stat": "OK"' in body


def check_database():
    if not DB_PATH.exists():
        return fail("找不到 stock_data.db")

    checks = []
    conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        cursor = conn.cursor()

        for table in ("stocks", "daily_prices"):
            exists = fetch_one(
                cursor,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            )
            checks.append(ok(f"{table} 表存在") if exists else fail(f"{table} 表不存在"))

        stock_count = fetch_one(cursor, "SELECT COUNT(*) FROM stocks")
        checks.append(
            ok(f"stocks 筆數 {stock_count}")
            if stock_count >= MIN_STOCKS
            else fail(f"stocks 筆數過少: {stock_count}")
        )

        daily_count = fetch_one(cursor, "SELECT COUNT(*) FROM daily_prices")
        checks.append(
            ok(f"daily_prices 筆數 {daily_count}")
            if daily_count >= MIN_DAILY_PRICES
            else fail(f"daily_prices 筆數過少: {daily_count}")
        )

        latest_date = fetch_one(cursor, "SELECT MAX(date) FROM daily_prices")
        if not latest_date:
            checks.append(fail("daily_prices 沒有最新日期"))
        else:
            latest_dt = datetime.strptime(latest_date, "%Y-%m-%d").date()
            now = datetime.now()
            today = now.date()
            age_days = (today - latest_dt).days
            checks.append(
                ok(f"最新股價日 {latest_date}，距今 {age_days} 天")
                if age_days <= MAX_PRICE_AGE_DAYS
                else fail(f"股價資料過舊: {latest_date}，距今 {age_days} 天")
            )

            if now.weekday() < 5 and now.time() >= time(16, 0) and twse_has_today_data(today):
                checks.append(
                    ok(f"最新股價日已是今日 {latest_date}")
                    if latest_dt == today
                    else fail(f"TWSE 今日已有收盤資料，但 DB 最新股價日仍是 {latest_date}")
                )

            latest_day_stocks = fetch_one(
                cursor,
                "SELECT COUNT(DISTINCT stock_id) FROM daily_prices WHERE date = ?",
                (latest_date,),
            )
            checks.append(
                ok(f"最新交易日股票數 {latest_day_stocks}")
                if latest_day_stocks >= MIN_LATEST_DAY_STOCKS
                else fail(f"最新交易日股票數過少: {latest_day_stocks}")
            )

        for col, min_ratio in MIN_NONZERO_RATIOS.items():
            nonzero_count = fetch_one(
                cursor,
                f"SELECT SUM(COALESCE({col}, 0) != 0) FROM stocks",
            ) or 0
            ratio = nonzero_count / stock_count if stock_count else 0
            checks.append(
                ok(f"{col} 非零覆蓋率 {ratio:.1%}")
                if ratio >= min_ratio
                else fail(f"{col} 非零覆蓋率過低: {ratio:.1%}")
            )

    except sqlite3.Error as exc:
        checks.append(fail(f"SQLite 檢查失敗: {exc}"))
    finally:
        conn.close()

    return all(checks)


def main():
    print("🔍 開始 DB 健康檢查...")
    passed = check_database() and check_compressed_db()
    if passed:
        print("🎉 DB 健康檢查通過，可以推送")
        return 0

    print("🛑 DB 健康檢查失敗，禁止推送")
    return 1


if __name__ == "__main__":
    sys.exit(main())
