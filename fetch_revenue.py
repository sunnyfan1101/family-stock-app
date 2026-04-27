# fetch_revenue.py - 台股月營收抓取與 YOY 計算
# 來源：FinMind API (https://api.finmindtrade.com/)
# 優點：穩定、免費、JSON 格式，無需 token

import requests
import pandas as pd
import sqlite3
import time
from datetime import datetime

FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"
DATASET = "TaiwanStockMonthRevenue"


def get_connection():
    return sqlite3.connect("stock_data.db")


def fetch_stock_revenue(stock_id, start_date="2024-01-01"):
    """
    從 FinMind API 抓取單一股票月營收
    """
    params = {
        "dataset": DATASET,
        "data_id": stock_id,
        "start_date": start_date,
    }
    
    try:
        response = requests.get(FINMIND_API_URL, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data.get("data"):
                df = pd.DataFrame(data["data"])
                # FinMind 的 revenue 單位是「千元」，但數值已經 *1000
                # 例如顯示 103277000 實際是 103,277 千元
                # 需要除以 1000 還原正確千元單位
                df["revenue"] = df["revenue"] / 1000
                return df
            else:
                print(f"⚠️ {stock_id} 無資料回傳")
                return pd.DataFrame()
        elif response.status_code == 403:
            # 🛑 403 阻擋休眠機制
            print(f"\n🛑 API 403 Forbidden，休眠 30 分鐘...")
            time.sleep(1800)
            print(f"⏰ 休眠結束，重試 {stock_id}...")
            # 重試一次
            response2 = requests.get(FINMIND_API_URL, params=params, timeout=30)
            if response2.status_code == 200:
                data = response2.json()
                if data.get("data"):
                    df = pd.DataFrame(data["data"])
                    df["revenue"] = df["revenue"] / 1000
                    return df
            print(f"❌ {stock_id} 重試後仍失敗 (狀態碼 {response2.status_code})")
            return pd.DataFrame()
        elif response.status_code == 402:
            # 🛑 402 額度耗盡休眠機制
            print(f"\n🛑 API 402 Payment Required，休眠 60 分鐘...")
            time.sleep(3600)
            print(f"⏰ 休眠結束，重試 {stock_id}...")
            response2 = requests.get(FINMIND_API_URL, params=params, timeout=30)
            if response2.status_code == 200:
                data = response2.json()
                if data.get("data"):
                    df = pd.DataFrame(data["data"])
                    df["revenue"] = df["revenue"] / 1000
                    return df
            print(f"❌ {stock_id} 重試後仍失敗 (狀態碼 {response2.status_code})")
            return pd.DataFrame()
        else:
            print(f"❌ {stock_id} API 錯誤 (狀態碼 {response.status_code})")
            return pd.DataFrame()
    except Exception as e:
        print(f"❌ {stock_id} 抓取失敗: {e}")
        return pd.DataFrame()


def calculate_yoy(df):
    """
    計算單月 YOY 與累積 YOY
    """
    if df.empty:
        return df
    
    # 確保欄位型別正確
    df["revenue_year"] = df["revenue_year"].astype(int)
    df["revenue_month"] = df["revenue_month"].astype(int)
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
    
    # 排序
    df = df.sort_values(["revenue_year", "revenue_month"]).reset_index(drop=True)
    
    # 計算單月 YOY
    df["last_year_revenue"] = 0.0
    df["yoy_growth"] = 0.0
    
    for idx, row in df.iterrows():
        year = row["revenue_year"]
        month = row["revenue_month"]
        
        # 查找去年同期
        last_year_data = df[(df["revenue_year"] == year - 1) & (df["revenue_month"] == month)]
        
        if len(last_year_data) > 0:
            last_rev = last_year_data["revenue"].values[0]
            df.at[idx, "last_year_revenue"] = last_rev
            
            if last_rev > 0:
                yoy = ((row["revenue"] - last_rev) / last_rev) * 100
                df.at[idx, "yoy_growth"] = yoy
    
    # 計算累積營收與累積 YOY
    df["cumulative_revenue"] = 0.0
    df["last_year_cumulative"] = 0.0
    df["cumulative_yoy"] = 0.0
    
    for (year, stock_id), group in df.groupby(["revenue_year", "stock_id"]):
        # 計算當年累積（逐月累加）
        cumsum = group["revenue"].cumsum()
        for idx in group.index:
            df.at[idx, "cumulative_revenue"] = cumsum.loc[idx]
        
        # 計算去年同期累積
        for idx, row in group.iterrows():
            month = row["revenue_month"]
            last_year = year - 1
            
            last_year_data = df[(df["revenue_year"] == last_year) & (df["revenue_month"] <= month)]
            
            if len(last_year_data) > 0:
                last_cumulative = last_year_data["revenue"].sum()
                df.at[idx, "last_year_cumulative"] = last_cumulative
                
                current_cumulative = df.at[idx, "cumulative_revenue"]
                if last_cumulative > 0:
                    cumulative_yoy = ((current_cumulative - last_cumulative) / last_cumulative) * 100
                    df.at[idx, "cumulative_yoy"] = cumulative_yoy
    
    return df


def save_to_database(df):
    """
    寫入 monthly_revenue 表
    """
    if df.empty:
        print("⚠️ 沒有資料可寫入")
        return 0
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # 準備寫入資料
    data_to_insert = []
    for _, row in df.iterrows():
        data_to_insert.append((
            str(row["stock_id"]),
            int(row["revenue_year"]),
            int(row["revenue_month"]),
            float(row["revenue"]),
            float(row.get("cumulative_revenue", 0)),
            float(row.get("yoy_growth", 0)),
            float(row.get("cumulative_yoy", 0)),
            float(row.get("last_year_revenue", 0)),
            float(row.get("last_year_cumulative", 0)),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ))
    
    # 批量寫入
    cursor.executemany("""
        INSERT OR REPLACE INTO monthly_revenue 
        (stock_id, year, month, revenue, cumulative_revenue, yoy_growth, cumulative_yoy, 
         last_year_revenue, last_year_cumulative, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, data_to_insert)
    
    conn.commit()
    inserted = cursor.rowcount
    conn.close()
    
    print(f"✅ 成功寫入 {len(data_to_insert)} 筆營收資料")
    return len(data_to_insert)


def update_monthly_revenue_for_stock(stock_id, start_date="2024-01-01"):
    """
    更新單一股票月營收
    """
    print(f"📊 抓取 {stock_id} 月營收...")
    
    df = fetch_stock_revenue(stock_id, start_date)
    
    if df.empty:
        print(f"⚠️ {stock_id} 無資料")
        return 0
    
    print(f"  抓取到 {len(df)} 筆原始資料")
    
    # 計算 YOY
    df = calculate_yoy(df)
    
    # 寫入資料庫
    count = save_to_database(df)
    
    # 顯示最新資料
    latest = df[df["revenue_year"] == df["revenue_year"].max()]
    latest = latest[latest["revenue_month"] == latest["revenue_month"].max()].iloc[0]
    
    print(f"  最新: {int(latest['revenue_year'])}年{int(latest['revenue_month'])}月")
    print(f"    單月營收: {int(latest['revenue']):,} 千元")
    print(f"    單月YOY: {latest['yoy_growth']:.2f}%")
    print(f"    累積營收: {int(latest['cumulative_revenue']):,} 千元")
    print(f"    累積YOY: {latest['cumulative_yoy']:.2f}%")
    
    return count


def get_expected_latest_month():
    """
    計算市場預期最新營收月份
    台股每月 10 號公佈上月營收
    今天 >= 10 號 → 最新月是「上個月」
    今天 < 10 號 → 最新月是「上上個月」
    """
    now = datetime.now()
    today = now.day
    
    if today >= 10:
        # 10 號以後，最新月是「上個月」
        if now.month == 1:
            expected_year = now.year - 1
            expected_month = 12
        else:
            expected_year = now.year
            expected_month = now.month - 1
    else:
        # 10 號以前，最新月是「上上個月」
        if now.month == 1:
            expected_year = now.year - 1
            expected_month = 11
        elif now.month == 2:
            expected_year = now.year - 1
            expected_month = 12
        else:
            expected_year = now.year
            expected_month = now.month - 2
    
    return expected_year, expected_month


def update_all_stocks(start_date="2024-01-01", batch_size=50):
    """
    更新所有股票月營收
    智能過濾：排除 ETF、跳過已有資料、402/403 休眠機制
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # 計算預期最新月份
    expected_year, expected_month = get_expected_latest_month()
    expected_ym = expected_year * 100 + expected_month  # 例如 202603
    print(f"📅 預期市場最新營收月份: {expected_year}年{expected_month:02d}月")
    
    # 取得所有股票代號
    cursor.execute("SELECT stock_id FROM stocks")
    all_stocks = [row[0] for row in cursor.fetchall()]
    
    # 🧹 嚴格過濾無效標的：只保留代號長度=4且不是00開頭
    stocks = [
        s for s in all_stocks 
        if len(s) == 4 and not s.startswith('00')
    ]
    
    filtered_count = len(all_stocks) - len(stocks)
    print(f"🚀 開始更新月營收...")
    print(f"   原始股票數: {len(all_stocks)}")
    print(f"   過濾後: {len(stocks)}（排除 {filtered_count} 檔 ETF/無效標的）")
    
    total = len(stocks)
    total_inserted = 0
    skipped_count = 0
    
    for i, stock_id in enumerate(stocks):
        print(f"\n[{i+1}/{total}] {stock_id} ", end="")
        
        # 🧠 基準月比對法：查詢該股票資料庫最新月份
        try:
            cursor.execute('''
                SELECT MAX(year * 100 + month) as latest_ym
                FROM monthly_revenue 
                WHERE stock_id = ?
            ''', (stock_id,))
            row = cursor.fetchone()
            
            if row and row[0] and row[0] >= expected_ym:
                # ✅ 資料庫最新月 >= 預期最新月，真正跳過
                db_year = row[0] // 100
                db_month = row[0] % 100
                print(f"⏭️ 已是最新資料 ({db_year}年{db_month:02d}月)，跳過")
                skipped_count += 1
                continue
                
        except Exception as e:
            print(f"(跳過檢查失敗: {e}) ", end="")
        
        try:
            count = update_monthly_revenue_for_stock(stock_id, start_date)
            total_inserted += count
        except Exception as e:
            # 🛑 402 / 403 斷路休眠機制
            error_str = str(e)
            if "402" in error_str or "Payment Required" in error_str:
                print(f"\n🛑 API 402 額度耗盡，休眠 60 分鐘...")
                time.sleep(3600)
                print(f"⏰ 休眠結束，繼續處理 {stock_id}...")
                # 重試一次
                try:
                    count = update_monthly_revenue_for_stock(stock_id, start_date)
                    total_inserted += count
                except Exception as e2:
                    print(f"❌ 重試失敗: {e2}")
            elif "403" in error_str or "Forbidden" in error_str:
                print(f"\n🛑 API 403 Forbidden，休眠 30 分鐘...")
                time.sleep(1800)
                print(f"⏰ 休眠結束，繼續處理 {stock_id}...")
                # 重試一次
                try:
                    count = update_monthly_revenue_for_stock(stock_id, start_date)
                    total_inserted += count
                except Exception as e2:
                    print(f"❌ 重試失敗: {e2}")
            else:
                print(f"❌ {stock_id} 處理失敗: {e}")
        
        # 避免請求過快
        if (i + 1) % batch_size == 0:
            print(f"\n⏸️ 已處理 {i+1} 檔，暫停 5 秒...")
            time.sleep(5)
        else:
            time.sleep(0.5)
    
    conn.close()
    
    print(f"\n🎉 月營收更新完成！")
    print(f"   總股票數: {total}")
    print(f"   跳過筆數: {skipped_count}")
    print(f"   總寫入筆數: {total_inserted}")
    
    return total_inserted


if __name__ == "__main__":
    # 測試模式：只更新特定股票
    import sys
    
    if len(sys.argv) > 1:
        # 命令列參數指定股票代號
        stock_id = sys.argv[1]
        update_monthly_revenue_for_stock(stock_id)
    else:
        # 預設更新所有股票
        print("使用方式: python3 fetch_revenue.py [stock_id]")
        print("範例: python3 fetch_revenue.py 4588")
        print("\n或執行 update_all_stocks() 更新全部")
