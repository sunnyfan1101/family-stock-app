# fetch_data.py (Local Version - 含盤整、三率強力計算、大盤統計)
import yfinance as yf
import pandas as pd
import sqlite3
import time
import requests
import random
from datetime import datetime, timedelta
from io import StringIO
import database
import numpy as np # 記得引入 numpy

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
        
        if resp_sii.status_code == 200:
            res_sii = pd.read_html(StringIO(resp_sii.text))[0]
        else:
            res_sii = pd.DataFrame()

        time.sleep(3) 

        # 2. 抓上櫃
        url_otc = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4"
        resp_otc = requests.get(url_otc, headers=headers, timeout=10)
        
        if resp_otc.status_code == 200:
            res_otc = pd.read_html(StringIO(resp_otc.text))[0]
        else:
            res_otc = pd.DataFrame()
        
        # 3. 解析資料
        for res, market in [(res_sii, "sii"), (res_otc, "otc")]:
            if res.empty: continue
            
            res.columns = res.iloc[0]
            res = res.iloc[1:] 
            
            target_col = '有價證券代號及名稱'
            if target_col not in res.columns: continue

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
        except: pass
        finally: conn.close()
            
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

# --- 4. 盤整天數計算函式 (支援 threshold) ---
def calculate_consolidation_days(hist_data, threshold=0.10):
    if hist_data.empty or len(hist_data) < 5:
        return 0
    try:
        current_price = hist_data.iloc[-1]
        upper_bound = current_price * (1 + threshold)
        lower_bound = current_price * (1 - threshold)
        
        days = 0
        recent_data = hist_data.iloc[:-1].tail(400).iloc[::-1]
        
        for price in recent_data:
            if lower_bound <= price <= upper_bound:
                days += 1
            else:
                break
        return days
    except:
        return 0

# --- 5. 取得歷史資料函數 ---
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

# --- 6. 主更新邏輯 ---
def update_stock_data(progress_bar=None, status_text=None):
    conn = database.get_connection()
    cursor = conn.cursor()
    
    # 0. 自動修補新欄位 (consolidation_days_20)
    try:
        cursor.execute("SELECT consolidation_days_20 FROM stocks LIMIT 1")
    except:
        print("⚠️ 新增 consolidation_days_20 欄位...")
        try:
            cursor.execute("ALTER TABLE stocks ADD COLUMN consolidation_days_20 REAL")
            conn.commit()
        except: pass

    all_stocks = get_tw_stock_list()
    db_dates = get_db_last_dates()
    total_stocks = len(all_stocks)
    
    print(f"🚀 準備更新 {total_stocks} 檔股票...")
    
    # --- Part A: 逐檔更新股票 ---
    for i, stock in enumerate(all_stocks):
        stock_id = stock["id"]
        symbol = stock["symbol"]
        
        if progress_bar: progress_bar.progress((i + 1) / total_stocks)
        if status_text: status_text.text(f"處理中 [{i+1}/{total_stocks}]: {stock['name']}")
        if i % 10 == 0: print(f"[{i+1}/{total_stocks}] {stock['name']}...", end="\r")

        capital_billion = 0
        revenue_streak = 0
        revenue_growth_pct = 0
        revenue_ttm = 0
        last_vol_ma5 = 0
        last_vol_ma20 = 0
        year_high_2y = 0
        year_low_2y = 0
        
        try: 
            # 1. 抓股價
            last_date_str = db_dates.get(stock_id)
            ticker = yf.Ticker(symbol)
            new_hist = pd.DataFrame()

            try:
                if last_date_str:
                    last_dt = datetime.strptime(last_date_str, '%Y-%m-%d')
                    start_dt = last_dt - timedelta(days=5) 
                    start_arg = start_dt.strftime('%Y-%m-%d')
                    new_hist = ticker.history(start=start_arg, auto_adjust=False)
                else:
                    new_hist = ticker.history(period="10y", auto_adjust=False)
            except:
                new_hist = pd.DataFrame()
            
            # --- 資料拼接 ---
            if last_date_str:
                old_df = get_db_history_data(stock_id, days=600)
                if not new_hist.empty:
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
                    combined_close = old_df['close'] if not old_df.empty else pd.Series(dtype=float)
                    combined_volume = old_df['volume'] if not old_df.empty and 'volume' in old_df.columns else pd.Series(dtype=float)
                
                combined_close = combined_close[~combined_close.index.duplicated(keep='last')]
                combined_volume = combined_volume[~combined_volume.index.duplicated(keep='last')]
            else:
                if new_hist.empty: continue
                combined_close = new_hist['Close']
                combined_volume = new_hist['Volume']

            # --- 計算指標 ---
            if combined_close.empty: continue
            
            # 均量
            if not combined_volume.empty:
                vol_ma5 = combined_volume.rolling(window=5).mean()
                vol_ma20 = combined_volume.rolling(window=20).mean()
                last_vol_ma5 = vol_ma5.iloc[-1] if not pd.isna(vol_ma5.iloc[-1]) else 0
                last_vol_ma20 = vol_ma20.iloc[-1] if not pd.isna(vol_ma20.iloc[-1]) else 0

            # 位階
            past_2year = combined_close.tail(500)
            year_high = combined_close.tail(250).max() if not combined_close.empty else 0
            year_low = combined_close.tail(250).min() if not combined_close.empty else 0
            year_high_2y = past_2year.max() if not past_2year.empty else year_high
            year_low_2y = past_2year.min() if not past_2year.empty else year_low

            # ★ 計算盤整天數
            consolidation_days = 0
            consolidation_days_20 = 0
            if not combined_close.empty:
                consolidation_days = calculate_consolidation_days(combined_close, threshold=0.10)
                consolidation_days_20 = calculate_consolidation_days(combined_close, threshold=0.20)

            # 填回 new_hist
            if not new_hist.empty:
                full_ma5 = combined_close.rolling(window=5).mean()
                full_ma20 = combined_close.rolling(window=20).mean()
                full_ma60 = combined_close.rolling(window=60).mean()
                new_hist['MA5'] = full_ma5.loc[new_hist.index]
                new_hist['MA20'] = full_ma20.loc[new_hist.index]
                new_hist['MA60'] = full_ma60.loc[new_hist.index]
                new_hist['Change_Pct'] = new_hist['Close'].pct_change(fill_method=None) * 100

            # --- 抓取基本面 (含三率強力修復版) ---
            try: 
                info = yf.Ticker(symbol).info 
            except: 
                info = {}
            
            eps = info.get('trailingEps', 0) or 0
            pe = info.get('trailingPE', 0) or 0
            pb = info.get('priceToBook', 0) or 0
            beta = info.get('beta', 0) or 0
            market_cap = info.get('marketCap')
            if market_cap is None: market_cap = info.get('totalAssets', 0)

            raw_yield = info.get('dividendYield')
            if raw_yield is None: raw_yield = info.get('trailingAnnualDividendYield')
            if raw_yield is not None:
                yield_rate = raw_yield if raw_yield > 1 else raw_yield * 100
            else:
                yield_rate = 0
            
            rev_growth = info.get('revenueGrowth')
            revenue_growth_pct = rev_growth * 100 if rev_growth is not None else 0
            revenue_ttm = revenue_growth_pct 
            earn_growth = info.get('earningsGrowth')
            eps_growth_pct = earn_growth * 100 if earn_growth is not None else 0

            # ★★★ 三率強力計算區 ★★★
            raw_gross = info.get('grossMargins')
            gross_margin_pct = raw_gross * 100 if raw_gross is not None else 0
            
            raw_op = info.get('operatingMargins')
            operating_margin_pct = raw_op * 100 if raw_op is not None else 0
            
            raw_net = info.get('profitMargins')
            net_margin_pct = raw_net * 100 if raw_net is not None else 0
            
            # 稅前純益率 (強力計算)
            pretax_margin_pct = 0 
            if info.get('pretaxMargins') is not None:
                pretax_margin_pct = info['pretaxMargins'] * 100
            
            # 如果 Info 沒有，去爬損益表
            if pretax_margin_pct == 0:
                try:
                    fin = ticker.income_stmt
                    if not fin.empty:
                        p_income = None
                        t_revenue = None
                        for idx in fin.index:
                            label = str(idx)
                            if "Pretax Income" in label and p_income is None:
                                p_income = fin.loc[idx].iloc[0]
                            if ("Total Revenue" in label or "TotalRevenue" in label) and t_revenue is None:
                                t_revenue = fin.loc[idx].iloc[0]
                        
                        if p_income is not None and t_revenue is not None and t_revenue != 0:
                            pretax_margin_pct = (p_income / t_revenue) * 100
                except: pass

            revenue_streak = calculate_revenue_streak(ticker)
            shares = info.get('sharesOutstanding', 0)
            if shares: capital_billion = shares / 10000000 

            # --- 寫入資料庫 (stocks) ---
            cursor.execute('''
                UPDATE stocks 
                SET eps=?, pe_ratio=?, pb_ratio=?, yield_rate=?, beta=?, market_cap=?, 
                    revenue_growth=?, revenue_ttm=?, revenue_streak=?, eps_growth=?, 
                    year_high=?, year_low=?, capital=?, vol_ma_5=?, vol_ma_20=?, 
                    year_high_2y=?, year_low_2y=?, gross_margin=?, 
                    operating_margin=?, pretax_margin=?, net_margin=?, 
                    consolidation_days=?, consolidation_days_20=?,
                    last_updated=?
                WHERE stock_id=?
            ''', (eps, pe, pb, yield_rate, beta, market_cap, 
                  revenue_growth_pct, revenue_ttm, revenue_streak, eps_growth_pct, 
                  year_high, year_low, capital_billion, last_vol_ma5, last_vol_ma20, 
                  year_high_2y, year_low_2y, gross_margin_pct,
                  operating_margin_pct, pretax_margin_pct, net_margin_pct,
                  consolidation_days, consolidation_days_20,
                  datetime.now().strftime('%Y-%m-%d'), stock_id))
            
            if cursor.rowcount == 0:
                 cursor.execute('''
                    INSERT INTO stocks (stock_id, name, industry, market_type, yahoo_symbol, 
                    eps, pe_ratio, pb_ratio, yield_rate, beta, market_cap, 
                    revenue_growth, revenue_ttm, revenue_streak, eps_growth, 
                    year_high, year_low, capital, vol_ma_5, vol_ma_20, 
                    year_high_2y, year_low_2y, gross_margin, 
                    operating_margin, pretax_margin, net_margin, consolidation_days, consolidation_days_20, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (stock_id, stock['name'], stock['industry'], stock['market'], symbol, 
                      eps, pe, pb, yield_rate, beta, market_cap, 
                      revenue_growth_pct, revenue_ttm, revenue_streak, eps_growth_pct, 
                      year_high, year_low, capital_billion, last_vol_ma5, last_vol_ma20, 
                      year_high_2y, year_low_2y, gross_margin_pct, 
                      operating_margin_pct, pretax_margin_pct, net_margin_pct, consolidation_days, consolidation_days_20,
                      datetime.now().strftime('%Y-%m-%d')))

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
            print(f"\n❌ {stock_id} 發生錯誤: {e}")
            continue
        
        time.sleep(0.2)
        
    # --- Part B: 新增每日市場統計 ---
    print("\n📊 正在計算今日大盤創新低家數...")
    try:
        cursor.execute("SELECT MAX(date) FROM daily_prices")
        latest_date = cursor.fetchone()[0]
        
        if latest_date:
            sql_stat_position = """
            SELECT COUNT(*) FROM stocks 
            WHERE (year_high - year_low) > 0 
            AND ( (SELECT close FROM daily_prices WHERE stock_id = stocks.stock_id ORDER BY date DESC LIMIT 1) - year_low ) / (year_high - year_low) <= 0.01
            """
            cursor.execute(sql_stat_position)
            low_count = cursor.fetchone()[0]
            print(f"📅 日期: {latest_date} | 📉 創新低(位階=0)家數: {low_count}")
            
            cursor.execute('''
                INSERT OR REPLACE INTO market_stats (date, new_low_count, updated_at)
                VALUES (?, ?, ?)
            ''', (latest_date, low_count, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            conn.commit()
    except Exception as e:
        print(f"❌ 大盤統計失敗: {e}")

    conn.close()

    # ==========================================
    # ★★★ GitHub 版本專屬：自動瘦身與壓縮 (.xz) ★★★
    # ==========================================
    print("\n🧹 [GitHub Mode] 執行資料庫瘦身 (保留近 5 年)...")
    try:
        import lzma
        import shutil
        import os

        # 1. 重新連線進行 VACUUM
        clean_conn = sqlite3.connect("stock_data.db", isolation_level=None)
        clean_cursor = clean_conn.cursor()
        
        # 刪除 5 年前資料
        clean_cursor.execute("DELETE FROM daily_prices WHERE date < date('now', '-5 years')")
        print(f"   已清除 {clean_cursor.rowcount} 筆過期資料。")
        
        # 重組資料庫 (VACUUM)
        print("   正在執行資料庫重組 (VACUUM)...")
        clean_cursor.execute("VACUUM")
        clean_conn.close()
        
        # 2. 執行 LZMA 強力壓縮
        print("📦 正在執行 LZMA 強力壓縮...")
        if os.path.exists("stock_data.db"):
            with open('stock_data.db', 'rb') as f_in:
                with lzma.open('stock_data.db.xz', 'wb', preset=9) as f_out:
                    shutil.copyfileobj(f_in, f_out)
            print("✅ 壓縮完成：產生 stock_data.db.xz")
        else:
            print("❌ 找不到 stock_data.db，無法壓縮")

    except Exception as e:
        print(f"⚠️ 瘦身或壓縮失敗: {e}")

if __name__ == "__main__":
    update_stock_data()