# fetch_data.py (全能智慧版 - 自動判斷新舊股與補漏)
import yfinance as yf
import pandas as pd
import sqlite3
import time
import requests
import random
from datetime import datetime, timedelta
from io import StringIO
import database

# --- 1. 取得股票清單 ---
def get_tw_stock_list():
    print("正在抓取最新股票清單...")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    stock_list = []
    try:
        # 1. 抓上市
        url_sii = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
        resp_sii = requests.get(url_sii, headers=headers, timeout=10)
        print(f"上市清單回應碼: {resp_sii.status_code}") 
        
        if resp_sii.status_code == 200:
            res_sii = pd.read_html(StringIO(resp_sii.text))[0]
            # ❌ 刪除這行：res_sii = res_sii.iloc[1:] (不要在這裡刪標題！)
        else:
            print("❌ 上市清單抓取失敗")
            res_sii = pd.DataFrame()

        # 休息一下
        time.sleep(3) 

        # 2. 抓上櫃
        url_otc = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4"
        resp_otc = requests.get(url_otc, headers=headers, timeout=10)
        print(f"上櫃清單回應碼: {resp_otc.status_code}")
        
        if resp_otc.status_code == 200:
            res_otc = pd.read_html(StringIO(resp_otc.text))[0]
            # ❌ 刪除這行：res_otc = res_otc.iloc[1:] (不要在這裡刪標題！)
        else:
            res_otc = pd.DataFrame()
        
        # 3. 解析資料
        for res, market in [(res_sii, "sii"), (res_otc, "otc")]:
            if res.empty: continue
            
            # 設定第一列為標題
            res.columns = res.iloc[0]
            # 設定完標題後，這裡再把標題列(第0列)排除，從內容開始
            res = res.iloc[1:] 
            
            # 防呆：檢查標題對不對
            target_col = '有價證券代號及名稱'
            if target_col not in res.columns:
                print(f"⚠️ 警告：表格格式不符，缺少 '{target_col}' 欄位")
                continue

            for index, row in res.iterrows():
                try:
                    code_name = row[target_col]
                    if not isinstance(code_name, str): continue
                    parts = code_name.split()
                    
                    if len(parts) >= 2:
                        code, name = parts[0], parts[1]
                        industry = row.get('產業別', '其他')
                        if pd.isna(industry) and code.startswith('00'): industry = "ETF"
                        
                        suffix = ".TW" if market == "sii" else ".TWO"
                        
                        if len(code) == 4 or code.startswith('00'):
                            stock_list.append({
                                "id": code, "name": name, "symbol": f"{code}{suffix}",
                                "industry": industry, "market": market
                            })
                except: continue
                
    except Exception as e:
        print(f"⚠️ 網路爬蟲失敗 ({e})，準備切換至離線模式...")
    
    # 4. 斷網自救機制
    if not stock_list:
        print("⚠️ 無法從網路取得清單，改使用資料庫既有名單進行更新。")
        conn = database.get_connection()
        try:
            df = pd.read_sql("SELECT stock_id, name, industry, market_type, yahoo_symbol FROM stocks", conn)
            for _, row in df.iterrows():
                stock_list.append({
                    "id": row['stock_id'], "name": row['name'], "symbol": row['yahoo_symbol'],
                    "industry": row['industry'], "market": row['market_type']
                })
        except Exception as db_e:
            print(f"❌ 資料庫讀取也失敗: {db_e}")
        finally:
            conn.close()
            
    print(f"✅ 取得 {len(stock_list)} 檔股票代號。")
    return stock_list

# --- 2. 取得資料庫日期 ---
def get_db_last_dates():
    conn = database.get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT stock_id, MAX(date) FROM daily_prices GROUP BY stock_id")
        return {row[0]: row[1] for row in cursor.fetchall()}
    except: return {}
    finally: conn.close()

# --- 3. 營收連增計算函數 ---
def calculate_revenue_streak(ticker):
    try:
        fin = ticker.income_stmt 
        if fin.empty: return 0
        rev_row = None
        for idx in fin.index:
            if "Total Revenue" in str(idx) or "TotalRevenue" in str(idx):
                rev_row = idx; break
        if rev_row is None: return 0
        revenues = fin.loc[rev_row].sort_index()
        streak = 0
        for i in range(len(revenues) - 1, 0, -1):
            if revenues.iloc[i] > revenues.iloc[i-1]: streak += 1
            else: break
        return streak
    except: return 0

# --- 4. 取得歷史資料函數 ---
def get_db_history_data(stock_id, days=600):
    conn = database.get_connection()
    try:
        sql = f"SELECT date, close, volume FROM daily_prices WHERE stock_id = ? ORDER BY date DESC LIMIT {days}"
        df = pd.read_sql(sql, conn, params=(stock_id,))
        if df.empty: return pd.DataFrame()
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        return df.set_index('date')
    finally: conn.close()

# --- 5. 主更新邏輯 ---
def update_stock_data(progress_bar=None, status_text=None):
    conn = database.get_connection()
    cursor = conn.cursor()
    
    all_stocks = get_tw_stock_list()
    db_dates = get_db_last_dates()
    today = datetime.now().date()
    total_stocks = len(all_stocks)
    
    print(f"🚀 準備更新 {total_stocks} 檔股票 (全能智慧模式)...")
    
    for i, stock in enumerate(all_stocks):
        stock_id = stock["id"]
        symbol = stock["symbol"]
        
        if progress_bar: progress_bar.progress((i + 1) / total_stocks)
        if status_text: status_text.text(f"處理中 [{i+1}/{total_stocks}]: {stock['name']}")
        if i % 10 == 0: print(f"[{i+1}/{total_stocks}] 處理: {stock['name']}...", end="\r")
        
        # --- 變數初始化 ---
        capital_billion = 0
        revenue_streak = 0
        revenue_growth_pct = 0
        revenue_ttm = 0
        last_vol_ma5 = 0
        last_vol_ma20 = 0
        year_high_2y = 0
        year_low_2y = 0
        
        try: # 開始監控錯誤
            
            # --- ★★★ 核心修改：智慧判斷區間 (User Requested) ★★★ ---
            last_date_str = db_dates.get(stock_id)
            ticker = yf.Ticker(symbol)
            new_hist = pd.DataFrame()

            try:
                if last_date_str:
                    # 【情境 A：老股票】
                    # 邏輯：從「資料庫最後日期 - 5天」開始抓到今天
                    # 好處：1. 保證有 overlap 能算漲跌幅 (修復 0% 問題)
                    #       2. 如果太久沒跑更新，也會自動把中間缺的月份補齊 (修復 Gap)
                    last_dt = datetime.strptime(last_date_str, '%Y-%m-%d')
                    start_dt = last_dt - timedelta(days=5) # 往回推 5 天
                    start_arg = start_dt.strftime('%Y-%m-%d')
                    
                    # 抓取這段期間的資料
                    new_hist = ticker.history(start=start_arg, auto_adjust=False)
                else:
                    # 【情境 B：新股票】
                    # 邏輯：完全沒看過的股票，直接抓 5 年
                    new_hist = ticker.history(period="5y", auto_adjust=False)
            
            except Exception as e:
                print(f"抓取失敗: {e}")
                new_hist = pd.DataFrame()
            
            # --- 資料拼接與防呆 ---
            if last_date_str:
                old_df = get_db_history_data(stock_id, days=600)
                
                if not new_hist.empty:
                    # 有新資料 -> 處理時區並拼接
                    try:
                        if new_hist.index.tz is not None:
                            new_hist.index = new_hist.index.tz_localize(None)
                    except: pass
                    
                    combined_close = pd.concat([old_df['close'] if not old_df.empty else pd.Series(dtype=float), new_hist['Close']])
                    
                    if not old_df.empty and 'volume' in old_df.columns:
                        combined_volume = pd.concat([old_df['volume'], new_hist['Volume']])
                    else:
                        combined_volume = new_hist['Volume']
                else:
                    # ⚠️ 沒有新資料 -> 直接用舊
                    combined_close = old_df['close'] if not old_df.empty else pd.Series(dtype=float)
                    combined_volume = old_df['volume'] if not old_df.empty and 'volume' in old_df.columns else pd.Series(dtype=float)
                
                # 去重
                combined_close = combined_close[~combined_close.index.duplicated(keep='last')]
                combined_volume = combined_volume[~combined_volume.index.duplicated(keep='last')]
                
            else:
                # 新股票
                if new_hist.empty: continue
                combined_close = new_hist['Close']
                combined_volume = new_hist['Volume']

            # --- 計算技術指標 ---
            if combined_close.empty: continue

            full_ma5 = combined_close.rolling(window=5).mean()
            full_ma20 = combined_close.rolling(window=20).mean()
            full_ma60 = combined_close.rolling(window=60).mean()
            
            # ★ 均量
            if not combined_volume.empty:
                vol_ma5 = combined_volume.rolling(window=5).mean()
                vol_ma20 = combined_volume.rolling(window=20).mean()
                last_vol_ma5 = vol_ma5.iloc[-1] if not pd.isna(vol_ma5.iloc[-1]) else 0
                last_vol_ma20 = vol_ma20.iloc[-1] if not pd.isna(vol_ma20.iloc[-1]) else 0

            # ★ 位階
            past_year = combined_close.tail(250)
            year_high = past_year.max() if not past_year.empty else 0
            year_low = past_year.min() if not past_year.empty else 0
            
            past_2year = combined_close.tail(500)
            year_high_2y = past_2year.max() if not past_2year.empty else year_high
            year_low_2y = past_2year.min() if not past_2year.empty else year_low

            # --- 填回 new_hist (只存新資料的指標) ---
            if not new_hist.empty:
                new_hist['MA5'] = full_ma5.loc[new_hist.index]
                new_hist['MA20'] = full_ma20.loc[new_hist.index]
                new_hist['MA60'] = full_ma60.loc[new_hist.index]
                new_hist['Change_Pct'] = new_hist['Close'].pct_change(fill_method=None) * 100

            # --- 抓取基本面 ---
            try: 
                # 強制重新抓取 info，不使用快取
                info = yf.Ticker(symbol).info 
            except: 
                info = {}
            
            # 1. EPS 與 本益比
            eps = info.get('trailingEps')
            if eps is None: eps = 0 # 真的沒資料才補 0
            
            pe = info.get('trailingPE')
            if pe is None: pe = 0

            # 2. 股淨比
            pb = info.get('priceToBook', 0)

            # 3. Beta
            beta = info.get('beta', 0)

            # 4. 市值 (優先用 marketCap，沒有則用 totalAssets)
            market_cap = info.get('marketCap')
            if market_cap is None: market_cap = info.get('totalAssets', 0)

            # 5. 殖利率 (智慧修正版)
            raw_yield = info.get('dividendYield')
            if raw_yield is None:
                raw_yield = info.get('trailingAnnualDividendYield')
            
            # ★★★ 修改這裡：增加防呆判斷 ★★★
            if raw_yield is not None:
                # Yahoo 有時候會給 0.03 (代表 3%)，有時候給 3.0 (代表 3%)
                # 我們假設殖利率不太可能超過 30%，如果大於 1，我們就當作它已經是百分比了，不再乘 100
                if raw_yield > 1: 
                    yield_rate = raw_yield  # 已經是百分比了 (例如 3.5)
                else:
                    yield_rate = raw_yield * 100 # 是小數 (例如 0.035 -> 3.5)
            else:
                yield_rate = 0
            
            # 6. 營收成長
            rev_growth = info.get('revenueGrowth')
            revenue_growth_pct = rev_growth * 100 if rev_growth is not None else 0
            revenue_ttm = revenue_growth_pct 

            # 7. EPS 成長
            earn_growth = info.get('earningsGrowth')
            eps_growth_pct = earn_growth * 100 if earn_growth is not None else 0

            # ★ 營收連增
            revenue_streak = calculate_revenue_streak(ticker)
            
            # ★ 股本
            shares = info.get('sharesOutstanding', 0)
            if shares: capital_billion = shares / 10000000 

            # --- 寫入資料庫 (stocks) ---
            cursor.execute('''
                UPDATE stocks 
                SET eps=?, pe_ratio=?, pb_ratio=?, yield_rate=?, beta=?, market_cap=?, 
                    revenue_growth=?, revenue_ttm=?, revenue_streak=?, eps_growth=?, 
                    year_high=?, year_low=?, capital=?, vol_ma_5=?, vol_ma_20=?, 
                    year_high_2y=?, year_low_2y=?, last_updated=?
                WHERE stock_id=?
            ''', (eps, pe, pb, yield_rate, beta, market_cap, 
                  revenue_growth_pct, revenue_ttm, revenue_streak, eps_growth_pct, 
                  year_high, year_low, capital_billion, last_vol_ma5, last_vol_ma20, 
                  year_high_2y, year_low_2y, 
                  datetime.now().strftime('%Y-%m-%d'), stock_id))
            
            if cursor.rowcount == 0:
                 cursor.execute('''
                    INSERT INTO stocks (stock_id, name, industry, market_type, yahoo_symbol, eps, pe_ratio, pb_ratio, yield_rate, beta, market_cap, 
                    revenue_growth, revenue_ttm, revenue_streak, eps_growth, 
                    year_high, year_low, capital, vol_ma_5, vol_ma_20, year_high_2y, year_low_2y, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (stock_id, stock['name'], stock['industry'], stock['market'], symbol, eps, pe, pb, yield_rate, beta, market_cap, 
                      revenue_growth_pct, revenue_ttm, revenue_streak, eps_growth_pct, 
                      year_high, year_low, capital_billion, last_vol_ma5, last_vol_ma20, 
                      year_high_2y, year_low_2y, datetime.now().strftime('%Y-%m-%d')))

            # --- 寫入資料庫 (daily_prices) ---
            if not new_hist.empty:
                data_to_insert = []
                for date, row in new_hist.iterrows():
                    date_str = date.strftime('%Y-%m-%d')
                    ma5 = row['MA5'] if pd.notna(row['MA5']) else None
                    ma20 = row['MA20'] if pd.notna(row['MA20']) else None
                    ma60 = row['MA60'] if pd.notna(row['MA60']) else None
                    change = row['Change_Pct'] if pd.notna(row['Change_Pct']) else 0
                    
                    data_to_insert.append((stock_id, date_str, row['Open'], row['High'], row['Low'], row['Close'], row['Volume'], change, ma5, ma20, ma60))
                
                cursor.executemany('''
                    INSERT OR REPLACE INTO daily_prices 
                    (stock_id, date, open, high, low, close, volume, change_pct, ma_5, ma_20, ma_60)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', data_to_insert)
            
            conn.commit()

        except Exception as e:
            # 印出錯誤但繼續跑
            print(f"\n❌ {stock_id} 發生錯誤: {e}")
            continue
        
        # 避免被 Yahoo 封鎖
        time.sleep(0.2)

    conn.close()
    print("\n🎉 全部更新完成！請檢查資料庫。")

if __name__ == "__main__":
    update_stock_data()