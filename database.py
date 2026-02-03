# database.py
import sqlite3

DB_NAME = "stock_data.db"

def get_connection():
    return sqlite3.connect(DB_NAME)

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    
    # 1. 股票基本資料表 (完整擴充版)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stocks (
        stock_id TEXT PRIMARY KEY,
        name TEXT,
        industry TEXT,
        market_type TEXT,
        yahoo_symbol TEXT,
        
        -- 基本面數據
        eps REAL,                   -- 每股盈餘
        pe_ratio REAL,              -- 本益比
        pb_ratio REAL,              -- 股價淨值比
        yield_rate REAL,            -- 殖利率 (%)
        beta REAL,                  -- Beta 值 (波動率)
        market_cap REAL,            -- 市值
        capital REAL,               -- 股本 (億)
        
        -- 成長性數據
        revenue_growth REAL,        -- 營收成長率 (YoY)
        revenue_ttm REAL,           -- 近四季營收成長
        revenue_streak INTEGER,     -- 營收連續成長年數
        eps_growth REAL,            -- EPS 成長率 (YoY)
        
        -- 獲利能力 (三率)
        gross_margin REAL,          -- 毛利率
        operating_margin REAL,      -- 營業利益率
        pretax_margin REAL,         -- 稅前純益率
        net_margin REAL,            -- 稅後純益率
        
        -- 技術面摘要
        year_high REAL,             -- 近一年最高價
        year_low REAL,              -- 近一年最低價
        year_high_2y REAL,          -- 近兩年最高價
        year_low_2y REAL,           -- 近兩年最低價
        vol_ma_5 REAL,              -- 最新 5日均量 (股)
        vol_ma_20 REAL,             -- 最新 20日均量 (股)
        consolidation_days INTEGER, -- 盤整天數 (打底)
        
        last_updated TEXT           -- 最後更新日期
    )
    ''')
    
    # 2. 股價歷史數據表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_prices (
        stock_id TEXT,
        date TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        change_pct REAL,            -- 漲跌幅 (%)
        ma_5 REAL,                  -- 5日均線
        ma_20 REAL,                 -- 20日均線 (月線)
        ma_60 REAL,                 -- 60日均線 (季線)
        
        FOREIGN KEY(stock_id) REFERENCES stocks(stock_id),
        PRIMARY KEY (stock_id, date)
    )
    ''')
    
    # 3. 使用者策略儲存表 (之前漏掉這個，這很重要！)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS user_presets (
        name TEXT PRIMARY KEY,
        settings TEXT               -- 儲存 JSON 格式的設定參數
    )
    ''')

    # 4. 自選股清單 (保留)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS watchlist (
        stock_id TEXT PRIMARY KEY,
        added_date TEXT,
        note TEXT,
        FOREIGN KEY(stock_id) REFERENCES stocks(stock_id)
    )
    ''')

    conn.commit()
    conn.close()
    print("資料庫結構初始化完成 (含完整欄位)！")

if __name__ == "__main__":
    init_db()