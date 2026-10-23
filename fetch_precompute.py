# fetch_precompute.py - 預先計算指標更新腳本
# 在 fetch_data.py 每日更新後執行，預先計算所有動態指標

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import database
from data_correctness import refresh_price_summary, ensure_revenue_columns

def get_connection():
    return database.get_connection()

def precompute_position(stock_id, conn):
    """
    預先計算位階 (position_1y, position_2y)
    """
    cursor = conn.cursor()
    
    # 取得最近一日收盤價
    cursor.execute('''
        SELECT close FROM daily_prices 
        WHERE stock_id = ? AND close>0
        ORDER BY date DESC LIMIT 1
    ''', (stock_id,))
    
    row = cursor.fetchone()
    if not row:
        return None, None
    
    current_close = row[0]
    
    # 計算近一年位階
    cursor.execute('''
        SELECT MAX(close), MIN(close) FROM (
            SELECT close FROM daily_prices WHERE stock_id = ? AND close>0 ORDER BY date DESC LIMIT 250
        )
    ''', (stock_id,))
    
    row = cursor.fetchone()
    if row and row[0] and row[1]:
        year_high, year_low = row[0], row[1]
        if year_high > year_low:
            position_1y = (current_close - year_low) / (year_high - year_low)
        else:
            position_1y = 0.5
    else:
        position_1y = None
    
    # 計算近二年位階
    cursor.execute('''
        SELECT MAX(close), MIN(close) FROM (
            SELECT close FROM daily_prices WHERE stock_id = ? AND close>0 ORDER BY date DESC LIMIT 500
        )
    ''', (stock_id,))
    
    row = cursor.fetchone()
    if row and row[0] and row[1]:
        year_high_2y, year_low_2y = row[0], row[1]
        if year_high_2y > year_low_2y:
            position_2y = (current_close - year_low_2y) / (year_high_2y - year_low_2y)
        else:
            position_2y = 0.5
    else:
        position_2y = None
    
    return position_1y, position_2y


def precompute_bias(stock_id, conn):
    """
    預先計算乖離率 (bias_20, bias_60)
    """
    cursor = conn.cursor()
    
    # 取得最近一日收盤價與均線
    cursor.execute('''
        SELECT close, ma_20, ma_60 
        FROM daily_prices 
        WHERE stock_id = ? AND close>0
        ORDER BY date DESC LIMIT 1
    ''', (stock_id,))
    
    row = cursor.fetchone()
    if not row:
        return None, None
    
    close, ma_20, ma_60 = row
    
    # 計算乖離率
    bias_20 = None
    bias_60 = None
    
    if close and ma_20 and ma_20 != 0:
        bias_20 = (close - ma_20) / ma_20
    
    if close and ma_60 and ma_60 != 0:
        bias_60 = (close - ma_60) / ma_60
    
    return bias_20, bias_60


def precompute_vol_spike(stock_id, conn):
    """
    預先計算爆量倍數 (vol_spike)
    """
    cursor = conn.cursor()
    
    # 取得最近一日成交量與 20 日均量
    cursor.execute('''
        SELECT volume, vol_ma_20 
        FROM daily_prices d
        JOIN stocks s ON d.stock_id = s.stock_id
        WHERE d.stock_id = ? AND d.close>0
        ORDER BY d.date DESC LIMIT 1
    ''', (stock_id,))
    
    row = cursor.fetchone()
    if not row:
        return None
    
    volume, vol_ma_20 = row
    
    # 計算爆量倍數
    if volume is not None and vol_ma_20 and vol_ma_20 > 0:
        vol_spike = volume / vol_ma_20
    else:
        vol_spike = None
    
    return vol_spike


def precompute_consolidation_log(stock_id, conn):
    """
    預先計算盤整天數對數
    """
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT consolidation_days FROM stocks WHERE stock_id = ?
    ''', (stock_id,))
    
    row = cursor.fetchone()
    if row and row[0]:
        consolidation_days = row[0]
        consolidation_log = np.log1p(consolidation_days)
        return consolidation_log
    
    return None


def sync_revenue_yoy_to_stocks(conn):
    from data_correctness import sync_revenue
    sync_revenue(conn)


def refresh_latest_stock_snapshot(conn=None):
    """
    建立首頁篩選用快照表，避免 Streamlit 每次查詢都 JOIN 全量 daily_prices。
    """
    should_close = False
    if conn is None:
        conn = get_connection()
        should_close = True

    cursor = conn.cursor()
    print("📸 開始刷新 latest_stock_snapshot...")
    ensure_revenue_columns(conn)

    cursor.execute("DROP TABLE IF EXISTS latest_stock_snapshot")
    cursor.execute('''
        CREATE TABLE latest_stock_snapshot AS
        SELECT
            s.stock_id, s.name, s.industry, s.market_type,
            s.pe_ratio, s.yield_rate, s.pb_ratio, s.eps, s.beta, s.market_cap,
            s.revenue_growth, s.revenue_monthly_yoy, s.revenue_ytd_yoy,
            s.revenue_ttm, s.revenue_period, s.revenue_streak, s.capital,
            s.vol_ma_5, s.vol_ma_20,
            s.eps_growth, s.gross_margin,
            s.operating_margin, s.pretax_margin, s.net_margin,
            s.consolidation_days, s.consolidation_days_20,
            s.position_1y, s.position_2y, s.bias_20, s.bias_60,
            s.vol_spike, s.consolidation_log,
            s.year_high, s.year_low, s.year_high_2y, s.year_low_2y,
            d.date, d.close, d.change_pct, d.volume, d.ma_5, d.ma_20, d.ma_60
        FROM stocks s
        JOIN daily_prices d ON s.stock_id = d.stock_id AND d.close>0
        WHERE d.date = (
            SELECT MAX(date)
            FROM daily_prices dp
            WHERE dp.stock_id = s.stock_id AND dp.close>0
        )
    ''')

    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_snapshot_stock_id ON latest_stock_snapshot(stock_id)",
        "CREATE INDEX IF NOT EXISTS idx_snapshot_industry ON latest_stock_snapshot(industry)",
        "CREATE INDEX IF NOT EXISTS idx_snapshot_position_1y ON latest_stock_snapshot(position_1y)",
        "CREATE INDEX IF NOT EXISTS idx_snapshot_position_2y ON latest_stock_snapshot(position_2y)",
        "CREATE INDEX IF NOT EXISTS idx_snapshot_revenue_growth ON latest_stock_snapshot(revenue_growth)",
        "CREATE INDEX IF NOT EXISTS idx_snapshot_eps_growth ON latest_stock_snapshot(eps_growth)",
        "CREATE INDEX IF NOT EXISTS idx_snapshot_gross_margin ON latest_stock_snapshot(gross_margin)",
        "CREATE INDEX IF NOT EXISTS idx_snapshot_vol_spike ON latest_stock_snapshot(vol_spike)",
    ]
    for sql in indexes:
        cursor.execute(sql)

    cursor.execute("SELECT COUNT(*) FROM latest_stock_snapshot")
    snapshot_count = cursor.fetchone()[0]
    conn.commit()

    if should_close:
        conn.close()

    print(f"✅ latest_stock_snapshot 刷新完成，共 {snapshot_count} 檔")
    return snapshot_count


def update_precomputed_metrics():
    """
    更新所有股票的預先計算指標
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    print("🚀 開始預先計算指標更新...")
    
    # 取得所有股票
    cursor.execute("SELECT stock_id FROM stocks")
    stocks = [row[0] for row in cursor.fetchall()]
    
    total = len(stocks)
    updated = 0
    
    for i, stock_id in enumerate(stocks):
        if i % 100 == 0:
            print(f"  [{i+1}/{total}] 處理中...", end="\r")
        
        try:
            # 計算各項指標
            refresh_price_summary(conn, stock_id)
            position_1y, position_2y = precompute_position(stock_id, conn)
            bias_20, bias_60 = precompute_bias(stock_id, conn)
            vol_spike = precompute_vol_spike(stock_id, conn)
            consolidation_log = precompute_consolidation_log(stock_id, conn)
            
            # 更新 stocks 表
            cursor.execute('''
                UPDATE stocks SET
                    position_1y = ?,
                    position_2y = ?,
                    bias_20 = ?,
                    bias_60 = ?,
                    vol_spike = ?,
                    consolidation_log = ?,
                    last_updated = ?
                WHERE stock_id = ?
            ''', (
                position_1y, position_2y, bias_20, bias_60, 
                vol_spike, consolidation_log,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                stock_id
            ))
            
            updated += 1
            
        except Exception as e:
            print(f"\n❌ {stock_id} 計算失敗: {e}")
            raise
    
    # 🔄 同步月營收 YOY
    sync_revenue_yoy_to_stocks(conn)
    
    conn.commit()
    conn.close()
    
    print(f"\n🎉 預先計算完成！共更新 {updated}/{total} 檔股票")


def update_weekly_ma():
    """
    更新週線均線 (weekly_ma_5, weekly_ma_20)
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    print("\n🚀 開始更新週線均線...")
    
    cursor.execute("SELECT stock_id FROM stocks")
    stocks = [row[0] for row in cursor.fetchall()]
    
    total = len(stocks)
    updated = 0
    
    for i, stock_id in enumerate(stocks):
        if i % 100 == 0:
            print(f"  [{i+1}/{total}] 處理中...", end="\r")
        
        try:
            # 取得日線資料
            df = pd.read_sql('''
                SELECT date, close, volume FROM daily_prices 
                WHERE stock_id = ? AND close>0 ORDER BY date ASC
            ''', conn, params=(stock_id,))
            
            if df.empty:
                continue
            cursor.execute('UPDATE daily_prices SET weekly_ma_5=NULL, weekly_ma_20=NULL WHERE stock_id=?', (stock_id,))
            from data_correctness import weekly_averages
            close = pd.Series(df['close'].to_numpy(), index=pd.to_datetime(df['date']))
            weekly = weekly_averages(close)
            rows = [
                (None if pd.isna(a) else float(a), None if pd.isna(b) else float(b),
                 stock_id, date.strftime('%Y-%m-%d'))
                for date, a, b in weekly.itertuples(name=None)
            ]
            # Include NULLs to clear incorrect legacy values for short histories.
            cursor.executemany('UPDATE daily_prices SET weekly_ma_5=?, weekly_ma_20=? '
                               'WHERE stock_id=? AND date=?', rows)
            
            updated += 1
            
        except Exception as e:
            print(f"\n❌ {stock_id} 週線計算失敗: {e}")
            raise
    
    conn.commit()
    refresh_latest_stock_snapshot(conn)
    conn.close()
    
    print(f"\n🎉 週線均線更新完成！共更新 {updated}/{total} 檔股票")


if __name__ == "__main__":
    # 執行預先計算
    update_precomputed_metrics()
    update_weekly_ma()
