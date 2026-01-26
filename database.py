# database.py
import sqlite3

DB_NAME = "stock_data.db"

def get_connection():
    return sqlite3.connect(DB_NAME)

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    
    # 1. 股票基本資料表 (擴充 EPS, Beta, 股本, 殖利率等)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stocks (
        stock_id TEXT PRIMARY KEY,
        name TEXT,
        industry TEXT,
        market_type TEXT,
        yahoo_symbol TEXT,
        
        -- 新增基本面欄位
        eps REAL,           -- 每股盈餘
        pe_ratio REAL,      -- 本益比
        pb_ratio REAL,      -- 股價淨值比
        yield_rate REAL,    -- 殖利率 (%)
        beta REAL,          -- Beta 值 (波動率)
        market_cap REAL,    -- 市值 (可推算股本)
        last_updated TEXT   -- 最後更新基本面的日期
    )
    ''')
    
    # 2. 股價歷史數據表 (擴充均線, 漲跌幅)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_prices (
        stock_id TEXT,
        date TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        
        -- 新增技術面欄位
        change_pct REAL,    -- 漲跌幅 (%)
        ma_5 REAL,          -- 5日均線
        ma_20 REAL,         -- 20日均線 (月線)
        ma_60 REAL,         -- 60日均線 (季線)
        
        -- 預留籌碼面欄位 (未來若有爬蟲可填入)
        foreign_buy INTEGER,    -- 外資買賣超
        trust_buy INTEGER,      -- 投信買賣超
        dealer_buy INTEGER,     -- 自營商買賣超
        
        FOREIGN KEY(stock_id) REFERENCES stocks(stock_id),
        PRIMARY KEY (stock_id, date)
    )
    ''')
    
    # 3. 自選股清單 (維持不變)
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
    print("資料庫結構升級完成！")

if __name__ == "__main__":
    init_db()