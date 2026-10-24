# database.py
import lzma
import os
import shutil
import sqlite3
import tempfile
import fcntl
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("STOCK_DB_PATH", str(PROJECT_DIR / "stock_data.db"))).resolve()
DB_XZ_PATH = Path(str(DB_PATH) + ".xz")
DB_NAME = str(DB_PATH)


def ensure_database():
    """Restore the runtime SQLite DB from the GitHub-friendly compressed copy."""
    bundle_dir = os.environ.get("STOCK_BUNDLE_DIR")
    bundle_id = os.environ.get("STOCK_BUNDLE_ID")
    if bundle_dir or bundle_id:
        if not bundle_dir or not bundle_id:
            raise ValueError("STOCK_BUNDLE_DIR and STOCK_BUNDLE_ID must be set together")
        from phase4_bundle import extract_private_shard, install_public_bundle
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        with (DB_PATH.parent / (DB_PATH.name + ".bundle.lock")).open("a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if DB_PATH.exists():
                with sqlite3.connect(DB_PATH.resolve().as_uri() + "?mode=ro", uri=True) as connection:
                    try:
                        current = connection.execute("SELECT bundle_id FROM _bundle_release").fetchone()
                    except sqlite3.OperationalError:
                        current = None
                if current and current[0] == bundle_id:
                    return
            with tempfile.TemporaryDirectory(prefix="private-shard-", dir=DB_PATH.parent) as temporary:
                private = Path(temporary) / "local.db"
                extract_private_shard(DB_PATH, private)
                install_public_bundle(bundle_dir, private, DB_PATH, expected_bundle_id=bundle_id)
        return
    if DB_PATH.exists():
        return

    if not DB_XZ_PATH.exists():
        raise FileNotFoundError(f"找不到資料庫：{DB_PATH} 或 {DB_XZ_PATH}")

    print("正在解壓縮資料庫 (LZMA)...")
    with lzma.open(DB_XZ_PATH, "rb") as f_in:
        with DB_PATH.open("wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    print("解壓縮完成！")

def get_connection():
    ensure_database()
    return sqlite3.connect(DB_NAME, timeout=30)

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
        revenue_growth REAL, revenue_monthly_yoy REAL, revenue_ytd_yoy REAL,
        revenue_period TEXT, revenue_ttm REAL, revenue_streak INTEGER, eps_growth REAL,
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
