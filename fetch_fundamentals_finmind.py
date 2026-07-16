#!/usr/bin/env python3
"""
fetch_fundamentals_finmind.py - FinMind 基本面資料抓取模組
用途：替換 fetch_data.py 中 yfinance 的基本面抓取邏輯
作者：DK 🦍
日期：2026-05-28

📝 安裝需求：
    pip install requests pandas

🔑 API Token（選填）：
    FinMind 免費版每日 600 次請求，不需要 token 也能用。
    如需大量請求，請到 https://finmindtrade.com/ 註冊獲取 token。
"""

import requests
import pandas as pd
from datetime import datetime
import os

FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"

# 讀取 API Token
def load_token():
    token_path = os.path.join(os.path.dirname(__file__), "finmind_token.txt")
    if os.path.exists(token_path):
        with open(token_path, "r") as f:
            return f.read().strip()
    return ""

FINMIND_TOKEN = load_token()
if FINMIND_TOKEN:
    print("🔑 FinMind API Token 已載入")
else:
    print("⚠️ 未找到 FinMind API Token，使用免費版限制")

# ============================================================
# 【核心函數】抓取單一股票基本面（EPS、PE、殖利率、三率）
# ============================================================

def fetch_fundamentals_finmind(stock_id, close_price=0):
    """
    從 FinMind API 抓取台股基本面資料

    參數:
        stock_id (str): 4位數台股代號，例如 "2330"
        close_price (float): 最新收盤價（用於計算 PE 和殖利率）

    回傳:
        dict: 包含以下欄位
            - eps (float): 近四季 EPS 總和
            - eps_growth (float): EPS 年增率 %
            - pe_ratio (float): 本益比（收盤價 / 近四季 EPS）
            - yield_rate (float): 殖利率 %
            - gross_margin (float): 毛利率 %
            - operating_margin (float): 營業利益率 %
            - pretax_margin (float): 稅前純益率 %
            - net_margin (float): 淨利率 %
            - revenue_growth (float): 營收成長率 %
            - pb_ratio (float): 市淨比（暫無，回傳 0）
            - beta (float): Beta（暫無，回傳 0）
            - market_cap (float): 市值（暫無，回傳 0）
    """
    result = {
        'eps': 0,
        'eps_growth': 0,
        'pe_ratio': 0,
        'yield_rate': 0,
        'gross_margin': 0,
        'operating_margin': 0,
        'pretax_margin': 0,
        'net_margin': 0,
        'revenue_growth': 0,
        'pb_ratio': 0,
        'beta': 0,
        'market_cap': 0
    }

    try:
        # ========== 1. 抓取財務報表 ==========
        # 檢查是否為 ETF（00 開頭的股票代號）
        if stock_id.startswith('00'):
            return result  # ETF 不需要財報資料，直接返回預設值

        params_fs = {
            'dataset': 'TaiwanStockFinancialStatements',
            'data_id': stock_id,
            'start_date': '2023-01-01'
        }
        if FINMIND_TOKEN:
            params_fs['token'] = FINMIND_TOKEN

        resp_fs = requests.get(FINMIND_API_URL, params=params_fs, timeout=15)

        if resp_fs.status_code == 200:
            data_fs = resp_fs.json().get('data', [])

            if data_fs:
                df_fs = pd.DataFrame(data_fs)

                # --- EPS 計算 ---
                eps_data = df_fs[df_fs['type'] == 'EPS'][['date', 'value']].sort_values('date')
                if len(eps_data) >= 4:
                    # 近四季 EPS 總和
                    recent_4q = eps_data.tail(4)['value'].sum()
                    result['eps'] = recent_4q

                    # EPS 成長率（近四季 vs 去年同期四季）
                    if len(eps_data) >= 8:
                        last_year_4q = eps_data.iloc[-8:-4]['value'].sum()
                        if last_year_4q != 0:
                            result['eps_growth'] = ((recent_4q - last_year_4q) / last_year_4q) * 100

                # --- EPS 為負數時也保留（區分「沒資料」和「虧損」）---
                # 如果 FinMind 有資料但 EPS 總和為負數，保留負數值
                # 這樣可以區分「eps = 0（沒資料）」和「eps < 0（虧損）」
                elif len(eps_data) > 0:
                    # 有資料但不足 4 季，用現有資料總和
                    result['eps'] = eps_data['value'].sum()

                # --- 三率計算（最新季）---
                latest_date = df_fs['date'].max()
                latest_df = df_fs[df_fs['date'] == latest_date]

                # 建立 type -> value 對照
                type_values = {}
                for _, row in latest_df.iterrows():
                    type_values[row['type']] = row['value']

                revenue = type_values.get('Revenue', 0)
                gross_profit = type_values.get('GrossProfit', 0)
                operating_income = type_values.get('OperatingIncome', 0)
                pretax_income = type_values.get('PreTaxIncome', 0)
                net_income = type_values.get('IncomeAfterTaxes', 0)

                if revenue != 0:
                    result['gross_margin'] = (gross_profit / revenue) * 100
                    result['operating_margin'] = (operating_income / revenue) * 100
                    result['pretax_margin'] = (pretax_income / revenue) * 100
                    result['net_margin'] = (net_income / revenue) * 100

                # --- 營收成長率（最新季 vs 去年同期）---
                if revenue != 0:
                    # 抓去年同季營收
                    year_ago_date = str(int(latest_date[:4]) - 1) + latest_date[4:]
                    year_ago_df = df_fs[df_fs['date'] == year_ago_date]
                    year_ago_revenue = 0
                    for _, row in year_ago_df.iterrows():
                        if row['type'] == 'Revenue':
                            year_ago_revenue = row['value']
                            break

                    if year_ago_revenue != 0:
                        result['revenue_growth'] = ((revenue - year_ago_revenue) / year_ago_revenue) * 100

        # ========== 2. 抓取股利政策（計算殖利率）==========
        params_div = {
            'dataset': 'TaiwanStockDividend',
            'data_id': stock_id,
            'start_date': '2023-01-01'
        }
        if FINMIND_TOKEN:
            params_div['token'] = FINMIND_TOKEN

        resp_div = requests.get(FINMIND_API_URL, params=params_div, timeout=15)

        if resp_div.status_code == 200:
            data_div = resp_div.json().get('data', [])

            if data_div and close_price > 0:
                df_div = pd.DataFrame(data_div)
                # 抓最新一次現金股利
                latest_div = df_div.sort_values('date').tail(1)
                if not latest_div.empty:
                    cash_div = latest_div['CashEarningsDistribution'].values[0]
                    # 殖利率 = 現金股利 / 收盤價 * 100
                    result['yield_rate'] = (cash_div / close_price) * 100

        # ========== 3. 計算本益比 ==========
        if close_price > 0 and result['eps'] > 0:
            result['pe_ratio'] = close_price / result['eps']

    except Exception as e:
        print(f"⚠️ {stock_id} FinMind 基本面抓取失敗: {e}")

    return result


# ============================================================
# 【快速測試】
# ============================================================
if __name__ == "__main__":
    # 測試台積電 2330
    test_result = fetch_fundamentals_finmind("2330", close_price=850)
    print("\n🧪 測試結果（台積電 2330, 假設收盤價 850）:")
    for key, value in test_result.items():
        print(f"  {key}: {value:.2f}" if isinstance(value, float) else f"  {key}: {value}")
