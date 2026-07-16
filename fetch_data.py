# fetch_data.py (Local Version - 含盤整、三率強力計算、大盤統計、預先計算)
import yfinance as yf
import pandas as pd
import sqlite3
import time
import requests
import random
from datetime import datetime, timedelta
from io import StringIO
import database
import numpy as np

# ★★★ 匯入預先計算模組 ★★★
try:
    from fetch_precompute import (
        get_connection as pc_get_connection,
        precompute_position,
        precompute_bias,
        precompute_vol_spike,
        precompute_consolidation_log,
        update_precomputed_metrics,
        update_weekly_ma
    )
    PRECOMPUTE_AVAILABLE = True
except ImportError:
    PRECOMPUTE_AVAILABLE = False
    print("⚠️  fetch_precompute.py 未找到,預先計算功能未啟用")

# ★★★ 匯入月營收抓取模組 ★★★
try:
    from fetch_revenue import update_all_stocks as update_monthly_revenue
    REVENUE_AVAILABLE = True
except ImportError:
    REVENUE_AVAILABLE = False
    print("⚠️  fetch_revenue.py 未找到,月營收增量更新未啟用")

# ★★★ 匯入 FinMind 基本面抓取模組 ★★★
try:
    from fetch_fundamentals_finmind import fetch_fundamentals_finmind
    FINMIND_FUNDAMENTAL_AVAILABLE = True
except ImportError:
    FINMIND_FUNDAMENTAL_AVAILABLE = False
    print("⚠️  fetch_fundamentals_finmind.py 未找到,基本面抓取 fallback 至 yfinance")

# ★★★ 單一股票預先計算函數 (供每日更新時呼叫) ★★★
def calculate_precompute_for_stock(cursor, stock_id):
    """
    為單一股票執行預先計算指標更新
    """
    try:
        # 使用外部連線，不建立新連線（避免 SQLite 鎖衝突）
        conn = cursor.connection

        # 計算位階
        position_1y, position_2y = precompute_position(stock_id, conn)

        # 計算乖離率
        bias_20, bias_60 = precompute_bias(stock_id, conn)

        # 計算爆量倍數
        vol_spike = precompute_vol_spike(stock_id, conn)

        # 計算盤整對數
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
            position_1y if position_1y is not None else 0,
            position_2y if position_2y is not None else 0,
            bias_20 if bias_20 is not None else 0,
            bias_60 if bias_60 is not None else 0,
            vol_spike if vol_spike is not None else 0,
            consolidation_log if consolidation_log is not None else 0,
            datetime.now().strftime('%Y-%m-%d'),
            stock_id
        ))

        return True

    except Exception as e:
        print(f"⚠️ {stock_id} 預先計算失敗: {e}")
        return False


# ★★★ 批次預先計算 (供每日更新結束後呼叫) ★★★
def run_batch_precompute(stock_ids=None):
    """
    批次更新預先計算指標
    參數:
        stock_ids: 指定更新哪些股票，None 則更新全部
    """
    if PRECOMPUTE_AVAILABLE:
        print("\n🚀 開始批次預先計算...")
        if stock_ids:
            print(f"   指定更新 {len(stock_ids)} 檔股票")
            for sid in stock_ids:
                try:
                    # 使用現有的 calculate_precompute_for_stock 函數
                    # 但需要資料庫連線，這裡簡化處理
                    pass
                except Exception as e:
                    print(f"   ⚠️ {sid} 預先計算失敗: {e}")
            update_weekly_ma()
        else:
            update_precomputed_metrics()
            update_weekly_ma()
        print("✅ 批次預先計算完成!")
    else:
        print("⚠️ 預先計算模組未載入,跳過批次計算")


# --- 1. 取得股票清單 ---

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
        print(f"⚠️ 網路爬蟲失敗 ({e}),準備切換至離線模式...")

    # 4. 斷網自救機制
    if not stock_list:
        print("⚠️ 無法從網路取得清單,改使用資料庫既有名單進行更新。")
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
    # 使用獨立連線，避免關閉主連線
    conn = database.get_connection()
    try:
        sql = f"SELECT date, close, volume FROM daily_prices WHERE stock_id = ? ORDER BY date DESC LIMIT {days}"
        df = pd.read_sql(sql, conn, params=(stock_id,))
        if df.empty: return pd.DataFrame()
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        return df.set_index('date')
    finally:
        conn.close()

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

    # 💡 [新增] 讀取啟動參數，判斷是否為財報季強制更新
    import sys
    force_financials = '--force-financials' in sys.argv

    # ★ 智慧篩選與快取預載：一次性撈取現有財報，避免被洗成 0
    cursor.execute('''
        SELECT stock_id, eps, pe_ratio, yield_rate, gross_margin,
               operating_margin, pretax_margin, net_margin, eps_growth, revenue_growth
        FROM stocks
    ''')
    existing_funds = {}
    for r in cursor.fetchall():
        existing_funds[r[0]] = {
            'eps': r[1] or 0, 'pe_ratio': r[2] or 0, 'yield_rate': r[3] or 0,
            'gross_margin': r[4] or 0, 'operating_margin': r[5] or 0,
            'pretax_margin': r[6] or 0, 'net_margin': r[7] or 0,
            'eps_growth': r[8] or 0, 'revenue_growth': r[9] or 0
        }

    total_to_update = total_stocks
    finmind_api_calls = 0  # 追蹤 API 使用量 (每檔股票用掉 2 次請求)

    print(f"🚀 開始全面更新 {total_to_update} 檔股票每日股價...")
    if force_financials:
        print("⚠️ [財報季加班模式] 已啟用 --force-financials，將強制更新所有基本面！")
    else:
        print("💡 [日常模式] 基本面快取已啟動，僅更新新股票(EPS=0)。")

    # ==========================================
    # 💡 [搬移並優化] 發車前先讀取記憶卡 (只要讀一次！)
    # ==========================================
    import os, time
    done_file = "finmind_done.txt"
    forced_done = set()

    if force_financials:
        # 如果記憶卡存在，檢查是不是上個季度的舊檔案 (超過 30 天)
        if os.path.exists(done_file):
            file_age_days = (time.time() - os.path.getmtime(done_file)) / (24 * 3600)
            if file_age_days > 30:
                os.remove(done_file) # 太舊了，撕掉重來
            else:
                with open(done_file, 'r') as f:
                    forced_done = set(f.read().splitlines())

    # --- Part A: 逐檔更新股票 ---
    for i, stock in enumerate(all_stocks):
        stock_id = stock['id']

        # ==========================================
        # 🛡️ 終極防護網：先猜上市，抓不到秒切上櫃
        # ==========================================
        symbol = f"{stock_id}.TW"  # 預設上市車牌

        try:
            # 去 Yahoo 敲個門，看有沒有資料
            test_df = yf.download(symbol, period="1d", progress=False)

            # 如果回傳是空的，代表這檔是上櫃股票！
            if test_df.empty:
                symbol = f"{stock_id}.TWO"  # 秒切上櫃車牌
        except Exception:
            # 💡 終極修正：如果 Yahoo 直接噴 404/400 錯誤，代表 .TW 找不到
            # 這時候絕對要把車牌換成 .TWO！不能寫 pass！
            symbol = f"{stock_id}.TWO"
        # ==========================================

        # ==========================================
        # 🚀 財報加班車「極速通關」機制
        # ==========================================
        if force_financials:
            is_etf = str(stock_id).startswith('00')
            has_been_forced = str(stock_id) in forced_done

            # 判斷這檔需不需要呼叫 FinMind
            need_finmind_now = (not is_etf) and (not has_been_forced) and (finmind_api_calls <= 580)

            # 如果這班車是來補財報的，但這檔股票根本不用補 (或 API 額度已滿)
            if not need_finmind_now:
                continue

        if progress_bar: progress_bar.progress((i + 1) / total_to_update)
        if status_text: status_text.text(f"處理中 [{i+1}/{total_to_update}]: {stock['name']}")
        if i % 10 == 0: print(f"[{i+1}/{total_to_update}] {stock['name']}...", end="\r")

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

            # --- 抓取基本面 (FinMind 核心替換版) ---
            # 💡 防護網:取得最新收盤價,若無則從資料庫撈取舊價或設為 0
            if not new_hist.empty:
                close_price = new_hist['Close'].iloc[-1]
            else:
                try:
                    cursor.execute("SELECT close FROM daily_prices WHERE stock_id = ? ORDER BY date DESC LIMIT 1", (stock_id,))
                    last_price_row = cursor.fetchone()
                    close_price = last_price_row[0] if last_price_row else 0
                except:
                    close_price = 0

            # 🚀 智慧快取機制 + 流量控管
            curr_existing = existing_funds.get(stock_id, {})
            db_eps = curr_existing.get('eps', 0)
            is_etf = str(stock_id).startswith('00')

            # ==========================================
            # 💡 [新增] 財報季記憶卡機制 (防止強制模式無限重複)
            # ==========================================
            import os, time
            done_file = "finmind_done.txt"
            forced_done = set()

            if force_financials:
                # 如果記憶卡存在，檢查是不是上個季度的舊檔案 (超過 30 天)
                if os.path.exists(done_file):
                    file_age_days = (time.time() - os.path.getmtime(done_file)) / (24 * 3600)
                    if file_age_days > 30:
                        os.remove(done_file) # 太舊了，撕掉重來
                    else:
                        with open(done_file, 'r') as f:
                            forced_done = set(f.read().splitlines())

            has_been_forced = str(stock_id) in forced_done

            # 判斷是否需要 Call FinMind:
            if force_financials:
                needs_update = not (str(stock_id) in forced_done)
            else:
                needs_update = (db_eps == 0)       # 日常模式：EPS 是空值才抓！

            # 綜合判斷：不是 ETF + 需要更新 + 額度還夠 (安全值 580)
            need_finmind = (not is_etf) and needs_update and (finmind_api_calls <= 580)

            if need_finmind:
                finmind_data = fetch_fundamentals_finmind(stock_id, close_price)
                finmind_api_calls += 2 # 每個 stock 呼叫兩次 API (財報+股利)

                # 📝 把抓過的股票寫進記憶卡，下個小時的排程就會自動跳過它！
                if force_financials:
                    with open(done_file, 'a') as f:
                        f.write(f"{stock_id}\n")
            else:
                # 🛡️ 防護網：沿用資料庫舊數據，絕對不洗白！
                finmind_data = {
                    'eps': db_eps,
                    'eps_growth': curr_existing.get('eps_growth', 0),
                    'gross_margin': curr_existing.get('gross_margin', 0),
                    'operating_margin': curr_existing.get('operating_margin', 0),
                    'pretax_margin': curr_existing.get('pretax_margin', 0),
                    'net_margin': curr_existing.get('net_margin', 0),
                    'revenue_growth': curr_existing.get('revenue_growth', 0),
                    'yield_rate': curr_existing.get('yield_rate', 0),
                    'pe_ratio': (close_price / db_eps) if db_eps > 0 and close_price > 0 else 0
                }

            eps = finmind_data.get('eps', 0)
            pe = finmind_data.get('pe_ratio', 0)
            yield_rate = finmind_data.get('yield_rate', 0)
            gross_margin_pct = finmind_data.get('gross_margin', 0)
            operating_margin_pct = finmind_data.get('operating_margin', 0)
            pretax_margin_pct = finmind_data.get('pretax_margin', 0)
            net_margin_pct = finmind_data.get('net_margin', 0)
            eps_growth_pct = finmind_data.get('eps_growth', 0)
            revenue_growth_pct = finmind_data.get('revenue_growth', 0)

            # 📝 以下次要欄位保留 yfinance 抓取
            try:
                info = yf.Ticker(symbol).info
            except:
                info = {}
            pb = info.get('priceToBook', 0) or 0
            beta = info.get('beta', 0) or 0
            market_cap = info.get('marketCap', 0) or 0
            revenue_ttm = revenue_growth_pct
            revenue_streak = calculate_revenue_streak(ticker)

            capital_billion = 0
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
                  datetime.now().strftime('%Y-%m-%d %H:%M:%S'), stock_id))

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
                      datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

            # --- ★★★ 預先計算指標 (每次更新後立即計算) ★★★
            calculate_precompute_for_stock(cursor, stock_id)

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

    # --- Part B: 新增與補齊每日市場統計 ---
    print("\n📊 正在檢查並補齊大盤創新低家數...")
    try:
        # 0. 確保表格存在
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS market_stats (
                date TEXT PRIMARY KEY,
                new_low_count INTEGER,
                updated_at TEXT
            )
        ''')

        # 1. 找出股價表有,但統計表沒有的「漏洞日期」(從2026開始查)
        cursor.execute("""
            SELECT DISTINCT date
            FROM daily_prices
            WHERE date >= '2026-01-01'
            AND date NOT IN (SELECT date FROM market_stats)
            ORDER BY date ASC
        """)
        missing_dates = [row[0] for row in cursor.fetchall()]

        if not missing_dates:
            print("✅ 大盤統計已是最新,無破洞需補齊。")
        else:
            print(f"🔍 發現 {len(missing_dates)} 天缺失的統計資料,開始自動補齊...")

            # 2. 迴圈逐日計算並補齊
            for target_date in missing_dates:
                # 這裡使用當天收盤價與 stocks 表的年低點做計算
                sql_stat_position = """
                SELECT COUNT(*)
                FROM stocks s
                JOIN daily_prices d ON s.stock_id = d.stock_id
                WHERE d.date = ?
                AND (s.year_high - s.year_low) > 0
                AND (d.close - s.year_low) / (s.year_high - s.year_low) <= 0.01
                """
                cursor.execute(sql_stat_position, (target_date,))
                low_count = cursor.fetchone()[0]

                # 寫入資料庫
                cursor.execute('''
                    INSERT OR REPLACE INTO market_stats (date, new_low_count, updated_at)
                    VALUES (?, ?, ?)
                ''', (target_date, low_count, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

                print(f"📅 補齊日期: {target_date} | 📉 創新低家數: {low_count}")

            conn.commit()
            print("✅ 大盤統計資料補齊完成!")

    except Exception as e:
        print(f"❌ 大盤統計失敗: {e}")


    # ★★★ Part C: 月營收智能增量更新 ★★★
    # 只在第一次執行時更新月營收（避免每次重複）
    if REVENUE_AVAILABLE and len(all_stocks) > 100:
        print("\n📊 開始月營收智能增量更新...")
        try:
            update_monthly_revenue(start_date="2024-01-01", batch_size=50)
            print("✅ 月營收增量更新完成!")
        except Exception as e:
            print(f"⚠️ 月營收更新失敗: {e}")
    else:
        print("\n⏭️ 月營收更新跳過（已是最新或批次過小）")

    # ★★★ Part D: 批次預先計算 ★★★
    # 只預先計算本次更新的股票
    update_ids_list = [s['id'] for s in all_stocks]
    run_batch_precompute(stock_ids=update_ids_list)

    conn.close()

    # ==========================================
    # ★★★ GitHub 版本專屬:自動瘦身與壓縮 (.xz) ★★★
    # ==========================================
    print("\n🧹 [GitHub Mode] 執行資料庫瘦身 (保留近 5 年)...")
    try:
        import lzma
        import shutil
        # 1. 重新連線進行 VACUUM
        clean_conn = sqlite3.connect(database.DB_NAME, isolation_level=None)
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
        if database.DB_PATH.exists():
            with database.DB_PATH.open('rb') as f_in:
                with lzma.open(database.DB_XZ_PATH, 'wb', preset=9) as f_out:
                    shutil.copyfileobj(f_in, f_out)
            print("✅ 壓縮完成:產生 stock_data.db.xz")
        else:
            print("❌ 找不到 stock_data.db,無法壓縮")

    except Exception as e:
        print(f"⚠️ 瘦身或壓縮失敗: {e}")

if __name__ == "__main__":
    update_stock_data()
