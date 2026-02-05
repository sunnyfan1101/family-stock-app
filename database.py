# database.py
import sqlite3

DB_NAME = "stock_data.db"

def get_connection():
    return sqlite3.connect(DB_NAME)

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    
    # 1. 股票基本資料表 (新增 consolidation_days_20)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stocks (
        stock_id TEXT PRIMARY KEY,
        name TEXT,
        industry TEXT,
        market_type TEXT,
        yahoo_symbol TEXT,
        
        -- 基本面
        eps REAL, pe_ratio REAL, pb_ratio REAL, yield_rate REAL, 
        beta REAL, market_cap REAL, capital REAL,
        
        -- 成長與獲利
        revenue_growth REAL, revenue_ttm REAL, revenue_streak INTEGER, eps_growth REAL,
        gross_margin REAL, operating_margin REAL, pretax_margin REAL, net_margin REAL,
        
        -- 技術面
        year_high REAL, year_low REAL, 
        year_high_2y REAL, year_low_2y REAL,
        vol_ma_5 REAL, vol_ma_20 REAL, 
        consolidation_days INTEGER,      -- 10% 盤整
        consolidation_days_20 INTEGER,   -- ★ 新增：20% 盤整
        
        last_updated TEXT
    )
    ''')
    
    # 2. 股價歷史數據表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_prices (
        stock_id TEXT, date TEXT,
        open REAL, high REAL, low REAL, close REAL, volume INTEGER,
        change_pct REAL, ma_5 REAL, ma_20 REAL, ma_60 REAL,
        FOREIGN KEY(stock_id) REFERENCES stocks(stock_id),
        PRIMARY KEY (stock_id, date)
    )
    ''')
    
    # 3. ★ 新增：大盤統計數據表 (Market Stats)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS market_stats (
        date TEXT PRIMARY KEY,
        new_low_count INTEGER,  -- 位階=0 (創新低) 的家數
        updated_at TEXT
    )
    ''')

    # 4. 使用者策略與自選股
    cursor.execute('CREATE TABLE IF NOT EXISTS user_presets (name TEXT PRIMARY KEY, settings TEXT)')
    cursor.execute('CREATE TABLE IF NOT EXISTS watchlist (stock_id TEXT PRIMARY KEY, added_date TEXT, note TEXT)')

    conn.commit()
    conn.close()
    print("資料庫結構初始化完成 (含 Market Stats)！")

if __name__ == "__main__":
    init_db()