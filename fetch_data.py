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

# 新增盤整天數函式
def calculate_consolidation_days(hist_data, threshold=0.10):
    """
    計算盤整天數：
    從最新一天往回數，看有多少天股價維持在 (最新價 +/- 10%) 的區間內。
    """
    if hist_data.empty or len(hist_data) < 5:
        return 0
    
    # 取得最新收盤價
    current_price = hist_data['Close'].iloc[-1]
    
    # 定義箱型上下緣
    upper_bound = current_price * (1 + threshold)
    lower_bound = current_price * (1 - threshold)
    
    days = 0
    # 從倒數第二天開始往回數 (因為最新一天一定在範圍內)
    # 我們取最近 250 天(約一年)來檢查即可
    recent_data = hist_data.iloc[:-1].tail(250).iloc[::-1] # 反轉順序，從昨天開始往回
    
    for price in recent_data['Close']:
        if lower_bound <= price <= upper_bound:
            days += 1
        else:
            # 一旦超出區間，盤整計數結束
            break
            
    return days

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
# [fetch_data.py] 的 update_stock_data 函式 (修正版)

def update_stock_data(progress_bar=None, status_text=None):
    conn = database.get_connection()
    cursor = conn.cursor()
    
    # ★★★ 0. 檢查並自動新增三率欄位 (資料庫遷移) ★★★
    new_cols = ['operating_margin', 'pretax_margin', 'net_margin', 'consolidation_days']
    for col in new_cols:
        try:
            cursor.execute(f"SELECT {col} FROM stocks LIMIT 1")
        except:
            print(f"⚠️ 偵測到舊版資料庫，正在新增 '{col}' 欄位...")
            try:
                cursor.execute(f"ALTER TABLE stocks ADD COLUMN {col} REAL")
                conn.commit()
            except Exception as e:
                print(f"❌ 欄位 {col} 新增失敗: {e}")

    all_stocks = get_tw_stock_list()
    db_dates = get_db_last_dates()
    total_stocks = len(all_stocks)
    
    print(f"🚀 準備更新 {total_stocks} 檔股票 (含三率)...")
    
    for i, stock in enumerate(all_stocks):
        stock_id = stock["id"]
        symbol = stock["symbol"]
        
        if progress_bar: progress_bar.progress((i + 1) / total_stocks)
        if status_text: status_text.text(f"處理中 [{i+1}/{total_stocks}]: {stock['name']}")
        if i % 10 == 0: print(f"[{i+1}/{total_stocks}] 處理: {stock['name']}...", end="\r")
        
        # 變數初始化
        capital_billion = 0
        revenue_streak = 0
        revenue_growth_pct = 0
        revenue_ttm = 0
        last_vol_ma5 = 0
        last_vol_ma20 = 0
        year_high_2y = 0
        year_low_2y = 0
        
        try: 
            # 1. 抓股價 (維持 5 年邏輯)
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
                    new_hist = ticker.history(period="5y", auto_adjust=False)
            except Exception as e:
                print(f"抓取失敗: {e}")
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
            
            # 均量 (存股數)
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

            # ★★★ 2. 計算盤整天數 ★★★
            consolidation_days = 0
            if not combined_close.empty:
                # 傳入歷史收盤價 Series
                # 這裡我們用新的 new_hist 還是 combined 都可以，建議用 combined_close (含舊資料) 比較準
                # 但 combined_close 是 Series，我們要轉成 DataFrame 或是稍微修改函式
                # 為了方便，我們直接傳 combined_close (Series) 給它，稍微改一下上面的函式參數接收方式即可
                # 修正上面的 calculate_consolidation_days 讓他接收 Series
                
                # 為了程式碼乾淨，直接在這裡用 combined_close 計算：
                try:
                    curr_p = combined_close.iloc[-1]
                    up_b = curr_p * 1.15 # 上下 15% 寬度
                    lo_b = curr_p * 0.85
                    
                    # 取最近 300 天，反轉
                    check_series = combined_close.iloc[:-1].tail(300).iloc[::-1]
                    for p in check_series:
                        if lo_b <= p <= up_b:
                            consolidation_days += 1
                        else:
                            break
                except:
                    consolidation_days = 0

                # 填回 new_hist
                if not new_hist.empty:
                    full_ma5 = combined_close.rolling(window=5).mean()
                    full_ma20 = combined_close.rolling(window=20).mean()
                    full_ma60 = combined_close.rolling(window=60).mean()
                    new_hist['MA5'] = full_ma5.loc[new_hist.index]
                    new_hist['MA20'] = full_ma20.loc[new_hist.index]
                    new_hist['MA60'] = full_ma60.loc[new_hist.index]
                    new_hist['Change_Pct'] = new_hist['Close'].pct_change(fill_method=None) * 100

            # --- 抓取基本面 (含三率) ---
            try: 
                info = yf.Ticker(symbol).info 
            except: 
                info = {}
            
            # 1. 基礎數據
            eps = info.get('trailingEps', 0) or 0
            pe = info.get('trailingPE', 0) or 0
            pb = info.get('priceToBook', 0) or 0
            beta = info.get('beta', 0) or 0
            market_cap = info.get('marketCap')
            if market_cap is None: market_cap = info.get('totalAssets', 0)

            # 2. 殖利率
            raw_yield = info.get('dividendYield')
            if raw_yield is None: raw_yield = info.get('trailingAnnualDividendYield')
            if raw_yield is not None:
                yield_rate = raw_yield if raw_yield > 1 else raw_yield * 100
            else:
                yield_rate = 0
            
            # 3. 成長率
            rev_growth = info.get('revenueGrowth')
            revenue_growth_pct = rev_growth * 100 if rev_growth is not None else 0
            revenue_ttm = revenue_growth_pct 
            earn_growth = info.get('earningsGrowth')
            eps_growth_pct = earn_growth * 100 if earn_growth is not None else 0

            # 4. ★★★ 抓取三率 (Info 優先，財報補強) ★★★
            # A. 毛利率 (Gross)
            raw_gross = info.get('grossMargins')
            gross_margin_pct = raw_gross * 100 if raw_gross is not None else 0
            
            # B. 營業利益率 (Operating)
            raw_op = info.get('operatingMargins')
            operating_margin_pct = raw_op * 100 if raw_op is not None else 0
            
            # C. 稅後純益率 (Net)
            raw_net = info.get('profitMargins')
            net_margin_pct = raw_net * 100 if raw_net is not None else 0
            
            # D. 稅前純益率 (Pretax) - ★ 強力計算區 ★
            pretax_margin_pct = 0 
            
            # (1) 先試試看 Info 有沒有
            if info.get('pretaxMargins') is not None:
                pretax_margin_pct = info['pretaxMargins'] * 100
            
            # (2) 如果 Info 沒有 (通常台股都沒有)，就去爬損益表
            if pretax_margin_pct == 0:
                try:
                    # 這行代碼會去下載財報，可能會稍微增加一點點更新時間，但值得
                    fin = ticker.income_stmt
                    if not fin.empty:
                        p_income = None
                        t_revenue = None
                        
                        # 模糊搜尋欄位名稱 (因為 Yahoo 有時叫 Pretax Income 有時叫 Pretax Income (Loss))
                        for idx in fin.index:
                            label = str(idx)
                            # 找稅前淨利
                            if "Pretax Income" in label and p_income is None:
                                p_income = fin.loc[idx].iloc[0] # 取最近一期
                            # 找總營收
                            if ("Total Revenue" in label or "TotalRevenue" in label) and t_revenue is None:
                                t_revenue = fin.loc[idx].iloc[0] # 取最近一期
                        
                        # 開始計算
                        if p_income is not None and t_revenue is not None and t_revenue != 0:
                            pretax_margin_pct = (p_income / t_revenue) * 100
                            # print(f"   [補救成功] {stock['name']} 稅前: {pretax_margin_pct:.2f}%") # 測試用
                except Exception as e:
                    # 如果真的算不出來，就保持 0
                    pass

            revenue_streak = calculate_revenue_streak(ticker)
            shares = info.get('sharesOutstanding', 0)
            if shares: capital_billion = shares / 10000000 

            # --- 寫入資料庫 (stocks) ---
            # ★ 更新 SQL，加入 operating_margin, pretax_margin, net_margin
            cursor.execute('''
                UPDATE stocks 
                SET eps=?, pe_ratio=?, pb_ratio=?, yield_rate=?, beta=?, market_cap=?, 
                    revenue_growth=?, revenue_ttm=?, revenue_streak=?, eps_growth=?, 
                    year_high=?, year_low=?, capital=?, vol_ma_5=?, vol_ma_20=?, 
                    year_high_2y=?, year_low_2y=?, gross_margin=?, 
                    operating_margin=?, pretax_margin=?, net_margin=?, 
                    consolidation_days=?,
                    last_updated=?
                WHERE stock_id=?
            ''', (eps, pe, pb, yield_rate, beta, market_cap, 
                  revenue_growth_pct, revenue_ttm, revenue_streak, eps_growth_pct, 
                  year_high, year_low, capital_billion, last_vol_ma5, last_vol_ma20, 
                  year_high_2y, year_low_2y, gross_margin_pct,
                  operating_margin_pct, pretax_margin_pct, net_margin_pct,
                  consolidation_days,   
                  datetime.now().strftime('%Y-%m-%d'), stock_id))
            
            if cursor.rowcount == 0:
                 cursor.execute('''
                    INSERT INTO stocks (stock_id, name, industry, market_type, yahoo_symbol, 
                    eps, pe_ratio, pb_ratio, yield_rate, beta, market_cap, 
                    revenue_growth, revenue_ttm, revenue_streak, eps_growth, 
                    year_high, year_low, capital, vol_ma_5, vol_ma_20, 
                    year_high_2y, year_low_2y, gross_margin, 
                    operating_margin, pretax_margin, net_margin, consolidation_days, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (stock_id, stock['name'], stock['industry'], stock['market'], symbol, 
                      eps, pe, pb, yield_rate, beta, market_cap, 
                      revenue_growth_pct, revenue_ttm, revenue_streak, eps_growth_pct, 
                      year_high, year_low, capital_billion, last_vol_ma5, last_vol_ma20, 
                      year_high_2y, year_low_2y, gross_margin_pct, 
                      operating_margin_pct, pretax_margin_pct, net_margin_pct, consolidation_days, 
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

    conn.close()

    print("\n🎉 全部流程結束！")
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