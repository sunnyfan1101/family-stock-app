import streamlit as st
import pandas as pd
import sqlite3
import database 
from groq import Groq
import os

# 1. 設定 Groq Client
def get_groq_client():
    try:
        # 優先讀取 secrets，如果沒有則讀取環境變數 (本地測試用)
        api_key = st.secrets.get("GROQ_API_KEY") or os.environ.get("GROQ_API_KEY")
        if not api_key:
            st.error("❌ 找不到 Groq API Key，請確認 .streamlit/secrets.toml 設定。")
            return None
        client = Groq(api_key=api_key)
        return client
    except Exception as e:
        st.error(f"❌ Groq 連線錯誤: {e}")
        return None

# ★★★ 補回這個函式來相容 app.py 的啟動檢查 ★★★
def configure_genai():
    """
    相容函式：檢查 Groq API Key 是否存在
    """
    if "GROQ_API_KEY" in st.secrets:
        return True
    return False

# 2. 建立股票快取表
@st.cache_data
def get_stock_map():
    conn = database.get_connection()
    try:
        df = pd.read_sql("SELECT stock_id, name FROM stocks", conn)
        stock_map = {}
        for _, row in df.iterrows():
            stock_map[str(row['stock_id'])] = str(row['stock_id']) 
            stock_map[row['name']] = str(row['stock_id'])     
        return stock_map
    finally:
        conn.close()

# 3. 抓股票代號
def extract_mentioned_stocks(query, stock_map):
    found_ids = set()
    for key, stock_id in stock_map.items():
        if key in query:
            found_ids.add(stock_id)
    return list(found_ids)

# 4. 撈資料
def fetch_stocks_data(stock_ids):
    if not stock_ids:
        return pd.DataFrame()
    
    conn = database.get_connection()
    try:
        placeholders = ','.join(['?'] * len(stock_ids))
        
        # ★★★ 修改重點：加入所有你指定的欄位 (Beta, 盤整天數, 稅前...) ★★★
        sql = f"""
        SELECT 
            s.stock_id, s.name, s.industry, 
            d.close, d.change_pct, d.volume,
            s.pe_ratio, s.yield_rate, s.pb_ratio,
            s.revenue_growth, s.revenue_streak, s.eps_growth,
            s.gross_margin, s.operating_margin, s.net_margin, s.pretax_margin,
            s.eps, s.capital,
            s.year_high, s.year_low, s.vol_ma_20,
            s.beta, s.consolidation_days
        FROM stocks s
        JOIN daily_prices d ON s.stock_id = d.stock_id
        WHERE s.stock_id IN ({placeholders})
          AND d.date = (SELECT MAX(date) FROM daily_prices WHERE stock_id = s.stock_id)
        """
        df = pd.read_sql(sql, conn, params=stock_ids)
        return df
    finally:
        conn.close()

# 5. 產生 Context (已修復小數點問題)
def generate_context(data_df):
    if data_df.empty:
        return "無數據"
    
    context = "【系統提供的詳細股票數據】\n"
    
    for _, row in data_df.iterrows():
        # --- 1. 預計算指標 & 防呆 ---
        def safe_float(val):
            return float(val) if val is not None else 0.0
            
        try:
            high = float(row['year_high'])
            low = float(row['year_low'])
            close = float(row['close'])
            position = (close - low) / (high - low) if (high - low) != 0 else 0
        except: position = 0
            
        try:
            vol = float(row['volume'])
            ma20 = float(row['vol_ma_20'])
            vol_spike = vol / ma20 if ma20 else 0
        except: vol_spike = 0

        vol_k = int(row['volume']/1000) if row['volume'] else 0
        
        # --- 2. 數據組裝 (包含你指定的所有欄位) ---
        context += f"""
        ● 股票：{row['name']} ({row['stock_id']}) | 產業：{row['industry']}
          [交易數據]
          - 股價：{safe_float(row['close']):.2f} | 漲跌幅：{safe_float(row['change_pct']):.2f}%
          - 成交量：{vol_k}張 | 爆量倍數：{vol_spike:.1f}倍 | Beta波動：{safe_float(row.get('beta', 0)):.2f}
          
          [技術型態]
          - 位階：{position:.2f} (0=地板, 1=天花板)
          - 盤整天數：{row.get('consolidation_days', 0)} 天
          
          [獲利能力]
          - EPS：{row['eps']} | EPS成長：{safe_float(row.get('eps_growth', 0)):.2f}%
          - 毛利率：{safe_float(row['gross_margin']):.2f}% | 營益率：{safe_float(row['operating_margin']):.2f}%
          - 稅前淨利：{safe_float(row.get('pretax_margin', 0)):.2f}% | 稅後淨利：{safe_float(row['net_margin']):.2f}%
          
          [成長與評價]
          - 營收YoY：{safe_float(row['revenue_growth']):.2f}% | 連增年數：{row['revenue_streak']}年
          - 本益比(PE)：{safe_float(row['pe_ratio']):.2f} | 股淨比(PB)：{safe_float(row['pb_ratio']):.2f}
          - 殖利率：{safe_float(row['yield_rate']):.2f}% | 股本：{safe_float(row['capital']):.2f}億
        ----------------------------------
        """
    return context


# 6. Groq 個股分析 AI (使用 Llama 3.3)
# [ai_agent.py] 修改 get_ai_response 函式內的 system_message

def get_ai_response(user_query, stock_context, chat_history):
    client = get_groq_client()
    if not client: return

    system_message = {
        "role": "system",
        "content": f"""
        你是一位在台股打滾 20 年、幽默犀利、說話一針見血的「老練操盤手」。
        你討厭給出冷冰冰的機器人罐頭回覆，你會像在熱炒店跟朋友聊股票一樣，直接點出兩檔股票的致命差異，或是為什麼某檔股票現在能不能買。

        【系統提供的詳細股票數據】
        {stock_context}

        【你的回答框架 - 請務必包含這三個部分，並展現你的個性】

        1. 🗣️ **老手開講 (破題回答)**
           - 認真傾聽使用者的問題並「直接回答」。
           - 例如使用者問「大成跟卜蜂差在哪？」你要直接講明（像是：一個在天上飛，一個在地上爬...等）。
           - 用詞要有人性、帶點幽默或犀利感 (可使用如：韭菜、大戶、洗盤、本夢比、護城河 等詞彙)。

        2. 📊 **數據對決 (Markdown表格)**
           - ⚠️ 嚴格要求：你必須使用標準的 Markdown 表格，確保每個指標會**換行**顯示，絕對不可以擠在同一行！
           - 範例格式：
             | 觀察指標 | 股票A | 股票B |
             | :--- | :--- | :--- |
             | **股價與位階** | 50.9 (位階 0.02 破底) | 147.0 (位階 0.86 高檔) |
             | **EPS與成長** | 3.92 (YoY -4.6%) | 10.2 (YoY +1.1%) |
             | **三率表現** | 毛利低、營益差 | 三率維持高檔 |
             | **估值(PE/殖利率)** | PE 12.9 (5.5%) | PE 14.4 (3.0%) |

        3. 💡 **操盤手真心話 (實戰結論)**
           - 結合上面的數據，給出你最真實的建議。
           - 明確表態：現在是可以抄底？還是要觀望？還是快逃？
           - 🚫 嚴禁說出「這不是投資建議，投資有風險」這種免責罐頭廢話。請用「老哥建議你...」或「我的實戰經驗是...」來結尾。

        【隱藏規則 - 絕對遵守】
        - 絕對使用「繁體中文」與「台灣股市慣用語」。
        - 如果使用者只問一檔股票，表格就只列一檔的資訊。
        """
    }

    messages = [system_message]
    # (後面的程式碼維持不變...)
    for msg in chat_history:
        role = "user" if msg["role"] == "user" else "assistant"
        messages.append({"role": role, "content": msg["content"]})
    
    messages.append({"role": "user", "content": user_query})

    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=messages,
            temperature=0.7,
            max_tokens=2000,
            top_p=1,
            stream=True,
            stop=None,
        )

        for chunk in completion:
            content = chunk.choices[0].delta.content
            if content:
                yield content

    except Exception as e:
        yield f"🤯 AI 斷線了：{str(e)}"
        

# 7. Groq 通用顧問 AI (不報明牌)
def get_general_response(user_query, chat_history):
    client = get_groq_client()
    if not client: return
    
    system_message = {
        "role": "system",
        "content": """
        你是一位專業、幽默的「台股投資教練」。
        
        【情境】使用者目前沒有選中任何股票。
        
        【任務】
        1. 回答投資觀念問題 (如：什麼是本益比？)。
        2. 若使用者問「推薦股票」，請**委婉拒絕報明牌**。
           - 引導他使用右側的「條件篩選 (Screener)」功能。
           - 範例：「大師不報明牌，想找飆股？去左邊把『營收成長』設 20% 以上，你就看到了！」
        
        【風格】幽默、老手口吻、繁體中文。
        """
    }
    
    messages = [system_message]
    for msg in chat_history:
        role = "user" if msg["role"] == "user" else "assistant"
        messages.append({"role": role, "content": msg["content"]})
        
    messages.append({"role": "user", "content": user_query})
    
    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile", # ★★★ 最新模型
            messages=messages,
            temperature=0.7,
            max_tokens=1024,
            stream=True,
        )
        
        for chunk in completion:
            content = chunk.choices[0].delta.content
            if content:
                yield content
                
    except Exception as e:
        yield f"🤯 AI 斷線了：{str(e)}"