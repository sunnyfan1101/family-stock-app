import streamlit as st
import pandas as pd
import sqlite3
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import analysis
from streamlit_option_menu import option_menu # 務必確認已安裝此套件
import plotly.express as px # ★ 新增這一行
import json
import time
# --- ★★★ GitHub 版本專用：解壓縮資料庫 (LZMA版) ★★★ ---
import lzma # 改用 lzma
import shutil
import os

# 偵測 .xz 檔案
if not os.path.exists("stock_data.db") and os.path.exists("stock_data.db.xz"):
    print("正在解壓縮資料庫 (LZMA)...")
    with lzma.open("stock_data.db.xz", "rb") as f_in:
        with open("stock_data.db", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    print("解壓縮完成！")

# ==========================================
# 0. 頁面設定與 CSS 美化
# ==========================================
st.set_page_config(page_title="StockAI 投資助理", layout="wide", page_icon="📈")

# 自定義 CSS 讓介面更乾淨
st.markdown("""
<style>
    /* 隱藏預設的 Streamlit 選單和 Footer */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* 調整指標卡片的樣式 */
    div[data-testid="stMetricValue"] {
        font-size: 24px;
        color: #FFD700; /* 金色字體 */
    }
    
    /* 讓表格標頭明顯一點 */
    thead tr th:first-child {display:none}
    tbody th {display:none}
    
    /* 按鈕樣式 */
    .stButton>button {
        width: 100%;
        border-radius: 5px;
        height: 3em;
    }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 1. 資料庫與繪圖函數
# ==========================================

def get_connection():
    return sqlite3.connect("stock_data.db")

def load_data(filters):
    conn = get_connection()
    
    # 1. 決定位階使用的欄位 (1年 vs 2年)
    # 根據傳入的設定，決定要用哪個欄位來計算 Position
    if filters.get('period') == '2y':
        col_h = 's.year_high_2y'
        col_l = 's.year_low_2y'
    else:
        col_h = 's.year_high'
        col_l = 's.year_low'

    # 2. SQL 查詢指令 (加入新欄位 capital, vol_ma, streak)
    base_sql = f"""
    SELECT 
        s.stock_id, s.name, s.industry, s.market_type,
        s.pe_ratio, s.yield_rate, s.pb_ratio, s.eps, s.beta, s.market_cap,
        s.revenue_growth, s.revenue_streak, s.capital, s.vol_ma_5, s.vol_ma_20,
        s.eps_growth,
        {col_h} as year_high, {col_l} as year_low,
        d.date, d.close, d.change_pct, d.volume, d.ma_5, d.ma_20, d.ma_60
    FROM stocks s
    JOIN daily_prices d ON s.stock_id = d.stock_id
    WHERE d.date = (SELECT MAX(date) FROM daily_prices dp WHERE dp.stock_id = s.stock_id)
    """
    conditions = []
    params = []

    # 產業篩選
    if filters.get('industry') and "全部" not in filters['industry']:
        placeholders = ','.join(['?'] * len(filters['industry']))
        conditions.append(f"s.industry IN ({placeholders})")
        params.extend(filters['industry'])

    # 3. 數值篩選 (加入 Capital, Vol MA, Streak)
    numeric_filters = [
        ('s.pe_ratio', filters.get('pe_min'), filters.get('pe_max')),
        ('s.yield_rate', filters.get('yield_min'), filters.get('yield_max')),
        ('s.pb_ratio', filters.get('pb_min'), filters.get('pb_max')),
        ('s.eps', filters.get('eps_min'), filters.get('eps_max')),
        ('s.beta', filters.get('beta_min'), filters.get('beta_max')),
        ('s.revenue_growth', filters.get('rev_min'), filters.get('rev_max')),
        ('s.capital', filters.get('cap_min'), filters.get('cap_max')),     # 股本
        ('s.vol_ma_5', filters.get('vol_ma_min'), filters.get('vol_ma_max')), # 均量
        ('d.close', filters.get('price_min'), filters.get('price_max')),
        ('d.change_pct', filters.get('change_min'), filters.get('change_max')),
        ('d.volume', filters.get('vol_min'), filters.get('vol_max')),
        ('s.vol_ma_5', filters.get('vol_ma_min'), filters.get('vol_ma_max')),
        ('s.vol_ma_20', filters.get('vol_ma20_min'), filters.get('vol_ma20_max')), # ★ 補上這行！
        ('s.eps_growth', filters.get('eps_growth_min'), filters.get('eps_growth_max')),
    ]

    for col, min_val, max_val in numeric_filters:
        if min_val is not None:
            conditions.append(f"{col} >= ?")
            params.append(min_val)
        if max_val is not None:
            conditions.append(f"{col} <= ?")
            params.append(max_val)

    # 營收連增 (大於等於 N 年)
    if filters.get('streak_min') is not None:
        conditions.append("s.revenue_streak >= ?")
        params.append(filters.get('streak_min'))

    # 位階篩選 (使用動態欄位 col_h, col_l)
    if filters.get('pos_min') is not None or filters.get('pos_max') is not None:
        # 公式：(收盤 - 低) / (高 - 低)
        pos_sql = f"(d.close - {col_l}) / NULLIF({col_h} - {col_l}, 0)"
        if filters.get('pos_min') is not None:
            conditions.append(f"{pos_sql} >= ?")
            params.append(filters.get('pos_min'))
        if filters.get('pos_max') is not None:
            conditions.append(f"{pos_sql} <= ?")
            params.append(filters.get('pos_max'))

    if conditions:
        final_sql = base_sql + " AND " + " AND ".join(conditions)
    else:
        final_sql = base_sql

    try:
        df = pd.read_sql(final_sql, conn, params=params)
        # 計算位階 (前端顯示用)
        df['position'] = (df['close'] - df['year_low']) / (df['year_high'] - df['year_low'])

        # [修改點 C] 計算爆量倍數 (Python 端計算)
        # 邏輯：今日成交量 / 20日均量 (避免除以0)
        df['vol_spike'] = df.apply(lambda x: x['volume'] / x['vol_ma_20'] if x['vol_ma_20'] > 0 else 0, axis=1)
        
        # [修改點 D] 執行爆量篩選
        if filters.get('vol_spike_min'):
            df = df[df['vol_spike'] >= filters['vol_spike_min']]
    except Exception as e:
        st.error(f"資料庫讀取錯誤: {e}")
        df = pd.DataFrame()
    finally:
        conn.close()
    return df

def load_stock_history(stock_id, days=1800): # 改成 1800 (約5年)
    conn = get_connection()
    sql = """
    SELECT date, open, high, low, close, volume, ma_5, ma_20, ma_60
    FROM daily_prices
    WHERE stock_id = ?
    ORDER BY date ASC
    """
    df = pd.read_sql(sql, conn, params=(stock_id,))
    conn.close()
    
    # 這裡原本是 df.tail(days)，現在 days 變大，就能回傳完整資料
    return df.tail(days)


def resample_to_weekly(df):
    df['date'] = pd.to_datetime(df['date'])
    # 定義轉換邏輯：開盤取第一天，收盤取最後一天，高取最高，低取最低，量取總和
    logic = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }
    # 'W-FRI' 代表每週五結算一根 K 棒
    df_weekly = df.resample('W-FRI', on='date').agg(logic).dropna().reset_index()
    
    # 重算週均線
    df_weekly['ma_5'] = df_weekly['close'].rolling(5).mean()
    df_weekly['ma_20'] = df_weekly['close'].rolling(20).mean()
    
    return df_weekly

def get_all_stocks_list():
    conn = get_connection()
    try:
        df = pd.read_sql("SELECT stock_id, name FROM stocks", conn)
        stock_options = [f"{row['stock_id']} {row['name']}" for index, row in df.iterrows()]
    except:
        stock_options = []
    conn.close()
    return stock_options

def plot_candlestick(df, stock_id, name, period_type="日線"):

    # [修改點] 標題使用傳進來的 period_type
    title_text = f'{stock_id} {name} - {period_type}走勢'

    # 1. 資料處理：確保日期格式正確
    df['date'] = pd.to_datetime(df['date'])
    df['date_str'] = df['date'].dt.strftime('%Y-%m-%d')
    
    # 2. 建立子圖 (開啟 shared_xaxes 來同步縮放)
    fig = make_subplots(
        rows=2, cols=1, 
        shared_xaxes=True,  # 關鍵：這會讓上下圖表的 X 軸連動
        vertical_spacing=0.02, # 縮小圖表間距
        subplot_titles=(title_text, '成交量'),
        row_width=[0.25, 0.75] # 調整比例：成交量佔 25%
    )
    
    # 3. K線圖 (上圖)
    fig.add_trace(go.Candlestick(
        x=df['date_str'], 
        open=df['open'], high=df['high'], low=df['low'], close=df['close'], 
        name='K線',
        increasing_line_color='#FF4B4B', decreasing_line_color='#00FF7F',
        showlegend=False # 隱藏圖例避免擋住畫面
    ), row=1, col=1)
    
    # 4. 均線 (上圖)
    fig.add_trace(go.Scatter(x=df['date_str'], y=df['ma_5'], mode='lines', name='MA5', line=dict(color='orange', width=1)), row=1, col=1)
    fig.add_trace(go.Scatter(x=df['date_str'], y=df['ma_20'], mode='lines', name='MA20', line=dict(color='#BA55D3', width=1)), row=1, col=1)
    
    # 5. 成交量 (下圖) - 顏色優化
    vol_colors = ['#FF4B4B' if c >= o else '#00FF7F' for c, o in zip(df['close'], df['open'])]
    fig.add_trace(go.Bar(
        x=df['date_str'], 
        y=df['volume'], 
        marker_color=vol_colors, 
        name='成交量',
        showlegend=False
    ), row=2, col=1)
    
    # --- 關鍵修正區：計算縮放範圍與成交量高度 ---
    
    # A. 計算預設顯示範圍 (最近 120 根 K 棒)
    # 對於 Category 軸，我們最好給它 "索引 (Index)" 或是 "精確的字串範圍"
    if len(df) > 120:
        start_date = df['date_str'].iloc[-120]
        end_date = df['date_str'].iloc[-1]
        initial_range = [start_date, end_date]
    else:
        initial_range = None # 資料太少就全顯示

    # B. 計算成交量 Y 軸上限 (讓成交量看起來高一點)
    # 我們取最近 120 天的最大量來設定，而不是 5 年的最大量，這樣近期才看得清楚
    if len(df) > 120:
        recent_vol = df['volume'].tail(120)
        vol_max = recent_vol.max() * 1.1 # 留 10% 頭部空間
    else:
        vol_max = df['volume'].max() * 1.1 if not df.empty else 1000

    # --- Layout 設定 ---
    fig.update_layout(
        height=600,
        template="plotly_dark",
        margin=dict(l=50, r=20, t=50, b=50),
        xaxis_rangeslider_visible=False, # 關閉原本的 slider，因為它會破壞 category 軸的同步
        dragmode='pan',
        
        # 設定 X 軸 (共用軸)
        xaxis=dict(
            type='category',     # 移除假日空洞
            categoryorder='category ascending', 
            range=initial_range, # ★ 這裡強制設定初始範圍
            nticks=8,            # 減少刻度密度
            tickangle=0          # 日期轉正比較好讀
        ),
        
        # 設定 X 軸 (下圖的 X 軸，通常被隱藏但需要確保屬性一致)
        xaxis2=dict(
            type='category',
            categoryorder='category ascending',
            matches='x' # ★ 強制下圖 X 軸跟隨上圖
        ),

        # 設定 Y 軸 (成交量)
        yaxis2=dict(
            range=[0, vol_max], # ★ 固定高度
            showgrid=False
        )
    )
    
    return fig

# ==========================================
# 2. UI 輔助函數 (下拉選單邏輯)
# ==========================================


def get_pe_range(option):
    mapping = {"不拘": (None, None), "10 倍以下 (低估)": (None, 10), "15 倍以下 (合理)": (None, 15), "20 倍以下 (正常)": (None, 20), "25 倍以上 (成長)": (25, None)}
    return mapping.get(option, (None, None))

def get_yield_range(option):
    mapping = {"不拘": (None, None), "3% 以上 (及格)": (3, None), "5% 以上 (高股息)": (5, None), "7% 以上 (超高配)": (7, None), "1% 以下 (成長)": (0, 1)}
    return mapping.get(option, (None, None))

def get_eps_range(option):
    mapping = {"不拘": (None, None), "0 元以上 (賺錢)": (0, None), "3 元以上 (穩健)": (3, None), "5 元以上 (高獲利)": (5, None), "10 元以上 (股王)": (10, None)}
    return mapping.get(option, (None, None))

def get_price_range(option):
    mapping = {"不拘": (None, None), "100 元以上 (高價)": (100, None), "50 ~ 100 元 (中價)": (50, 100), "10 ~ 50 元 (銅板)": (10, 50), "10 元以下 (低價)": (0, 10)}
    return mapping.get(option, (None, None))

def get_change_range(option):
    mapping = {"不拘": (None, None), "上漲 (> 0%)": (0, None), "強勢 (> 3%)": (3, None), "漲停 (> 9%)": (9, None), "下跌 (< 0%)": (None, 0), "跌深 (<-3%)": (None, -3)}
    return mapping.get(option, (None, None))

def get_volume_range(option):
    mapping = {"不拘": (None, None), "500 張以上": (500, None), "1000 張以上": (1000, None), "5000 張以上": (5000, None), "10000 張以上": (10000, None)}
    return mapping.get(option, (None, None))

def get_beta_range(option):
    mapping = {"不拘": (None, None), "大於 1 (活潑)": (1, None), "大於 1.5 (攻擊)": (1.5, None), "小於 1 (穩健)": (None, 1), "小於 0.5 (牛皮)": (None, 0.5)}
    return mapping.get(option, (None, None))

# --- 新增：營收與位階的選項邏輯 ---
def get_revenue_range(option):
    # 營收成長率 YoY (%)
    return {"成長 (> 0%)": (0, None), "高成長 (> 20%)": (20, None), "爆發 (> 50%)": (50, None), "衰退 (< 0%)": (None, 0)}.get(option, (None, None))

def get_position_range(option):
    # 位階 (0.0 ~ 1.0)
    mapping = {
        "底部 (0 ~ 0.2)": (0, 0.2), 
        "低檔 (0.2 ~ 0.4)": (0.2, 0.4), 
        "中階 (0.4 ~ 0.6)": (0.4, 0.6), 
        "高檔 (0.6 ~ 0.8)": (0.6, 0.8), 
        "頭部 (0.8 ~ 1.0)": (0.8, 1.0)
    }
    return mapping.get(option, (None, None))

# --- 新增：股本與營收連增的選項邏輯 ---
def get_capital_range(option):
    # 股本 (億)
    mapping = {
        "不拘": (None, None),
        "小型股 (< 10億)": (0, 10),
        "中型股 (10億 ~ 50億)": (10, 50),
        "大型股 (> 50億)": (50, None),
        "超大型權值股 (> 200億)": (200, None)
    }
    return mapping.get(option, (None, None))

def get_streak_range(option):
    # 營收連續成長 (年/季)
    mapping = {
        "不拘": None,
        "連增 1 年以上": 1,
        "連增 2 年以上": 2,
        "連增 3 年以上": 3,
        "連增 5 年以上": 5
    }
    return mapping.get(option, None)

# --- 策略管理函數 ---
def save_user_preset(name, settings):
    conn = get_connection()
    try:
        # 將設定字典轉成 JSON 字串存入
        settings_json = json.dumps(settings, ensure_ascii=False)
        conn.execute("INSERT OR REPLACE INTO user_presets (name, settings) VALUES (?, ?)", (name, settings_json))
        conn.commit()
        return True
    except Exception as e:
        st.error(f"儲存失敗: {e}")
        return False
    finally:
        conn.close()

def get_user_presets():
    conn = get_connection()
    try:
        df = pd.read_sql("SELECT name, settings FROM user_presets", conn)
        return df.set_index('name')['settings'].to_dict()
    except:
        return {}
    finally:
        conn.close()

def delete_user_preset(name):
    conn = get_connection()
    conn.execute("DELETE FROM user_presets WHERE name=?", (name,))
    conn.commit()
    conn.close()

# ==========================================
# 3. 主程式
# ==========================================

def main():
    
    # --- 左側導航欄 (使用 streamlit-option-menu 美化) ---
    with st.sidebar:
        st.image("https://cdn-icons-png.flaticon.com/512/3314/3314323.png", width=50) # 可換成自己的 Logo
        

        selected_page = option_menu(
        menu_title="股市尋寶",
        options=["條件篩選 (Screener)", "AI 相似股 (Similarity)"], # ★ 拿掉系統設定
        icons=["filter-circle", "robot"], # icons 也可以少一個，不過沒差
        menu_icon="graph-up-arrow",
        default_index=0,
        styles={
                "container": {"padding": "5px", "background-color": "#262730"},
                "icon": {"color": "orange", "font-size": "20px"}, 
                "nav-link": {"font-size": "16px", "text-align": "left", "margin":"0px", "--hover-color": "#444"},
                "nav-link-selected": {"background-color": "#FF4B4B"},
            }
        )
        
    # ==========================================
    # 頁面 1: 條件篩選 (Screener)
    # ==========================================
    # --- 條件篩選 (Screener) ---
    if selected_page == "條件篩選 (Screener)":
        st.title("🎯 智慧選股儀表板")
        
        conn = get_connection()
        try:
            df_all = pd.read_sql("SELECT DISTINCT industry FROM stocks", conn)
            all_industries = ["全部"] + df_all['industry'].dropna().tolist()
        except: all_industries = ["全部"]
        conn.close()

        # --- 定義內建策略 (對應選單的文字) ---
        default_strategies = {
            "巴菲特護城河 (穩健)": {
                "capital": "大型股 (> 50億)", "beta": "小於 1 (穩健)", "yield": "3% 以上 (及格)", "eps": "0 元以上 (賺錢)",
                "pe": "不拘", "revenue": "不拘", "streak": "連增 1 年以上", "position": "不拘"
            },
            "彼得林區成長 (爆發)": {
                "revenue": "高成長 (> 20%)", "pe": "20 倍以下 (正常)", "capital": "中型股 (10億 ~ 50億)",
                "yield": "不拘", "beta": "不拘", "streak": "不拘", "position": "不拘"
            },
            "低檔轉機股 (抄底)": {
                "position": "底部 (0 ~ 0.2)", "revenue": "成長 (> 0%)", "change": "不拘",
                "pe": "不拘", "capital": "不拘", "streak": "不拘"
            }
        }

        # --- 初始化 Session State (確保每個選單都有預設值) ---
        # 這是為了讓按鈕可以控制選單
        filter_keys = ['sel_industry', 'sel_price', 'sel_capital', 'sel_pos', 'sel_vol5', 'sel_vol20', 'sel_change', 
                       'sel_rev', 'sel_streak', 'sel_pe', 'sel_yield', 'sel_beta', 'sel_eps']
        
        for k in filter_keys:
            if k not in st.session_state:
                # 設定預設值
                if k == 'sel_industry': st.session_state[k] = ["全部"] # multiselect 預設是 list
                else: st.session_state[k] = "不拘"

        # --- 策略按鈕區 ---
        st.markdown("##### ⚡ 策略快選")
        
        # 1. 讀取自訂策略
        saved_presets = get_user_presets()
        all_strategies = default_strategies.copy()
        # 解析 JSON 並加入選單
        for name, json_str in saved_presets.items():
            try: all_strategies[f"👤 {name}"] = json.loads(json_str)
            except: pass

        # 2. 顯示按鈕 (用 columns 排列)
        strat_cols = st.columns(len(all_strategies) + 1)
        
        # 產生策略按鈕
        for i, (strat_name, strat_params) in enumerate(all_strategies.items()):
            # 這裡我們只顯示前 4 個按鈕，避免爆版，或者你可以用 selectbox 來選策略
            if i < 4: 
                with strat_cols[i]:
                    if st.button(strat_name, width='stretch'):
                        # ★ 當按鈕按下，更新 Session State
                        # 先重置所有為不拘
                        for k in filter_keys:
                            if k == 'sel_industry': st.session_state[k] = ["全部"]
                            else: st.session_state[k] = "不拘"
                        
                        # 套用策略參數 (對應下方的 key)
                        if "capital" in strat_params: st.session_state['sel_capital'] = strat_params["capital"]
                        if "beta" in strat_params: st.session_state['sel_beta'] = strat_params["beta"]
                        if "yield" in strat_params: st.session_state['sel_yield'] = strat_params["yield"]
                        if "eps" in strat_params: st.session_state['sel_eps'] = strat_params["eps"]
                        if "revenue" in strat_params: st.session_state['sel_rev'] = strat_params["revenue"]
                        if "pe" in strat_params: st.session_state['sel_pe'] = strat_params["pe"]
                        if "streak" in strat_params: st.session_state['sel_streak'] = strat_params["streak"]
                        if "position" in strat_params: st.session_state['sel_pos'] = strat_params["position"]
                        st.rerun() # 強制重整頁面以套用新數值

        # --- 位階與熱力圖開關 ---
        period_col, save_col = st.columns([2, 2])
        with period_col:
            period_mode = st.radio("位階計算基準", ["近 1 年 (標準)", "近 2 年 (長線)"], horizontal=True)
            period_val = '2y' if "2" in period_mode else '1y'
        
        with save_col:
            # --- 儲存目前設定的功能 ---
            with st.popover("💾 儲存目前條件為策略"):
                new_preset_name = st.text_input("策略名稱", placeholder="例如：我的存股名單")
                if st.button("確認儲存"):
                    if new_preset_name:
                        # 收集目前的所有設定
                        current_settings = {
                            "industry": st.session_state.sel_industry,
                            "price": st.session_state.sel_price,
                            "capital": st.session_state.sel_capital,
                            "position": st.session_state.sel_pos,
                            "vol5": st.session_state.sel_vol5,
                            "vol20": st.session_state.sel_vol20,
                            "change": st.session_state.sel_change,
                            "revenue": st.session_state.sel_rev,
                            "streak": st.session_state.sel_streak,
                            "pe": st.session_state.sel_pe,
                            "yield": st.session_state.sel_yield,
                            "beta": st.session_state.sel_beta,
                            "eps": st.session_state.sel_eps
                        }
                        if save_user_preset(new_preset_name, current_settings):
                            st.success(f"策略 '{new_preset_name}' 已儲存！")
                            time.sleep(1)
                            st.rerun()
                    else:
                        st.warning("請輸入名稱")
                
                # 刪除功能
                if saved_presets:
                    st.divider()
                    del_name = st.selectbox("刪除自訂策略", list(saved_presets.keys()))
                    if st.button("🗑️ 刪除"):
                        delete_user_preset(del_name)
                        st.rerun()

        with st.expander("🛠️ 設定篩選條件 (含股本、均量、營收連增)", expanded=True):
            col1, col2, col3, col4 = st.columns(4)
            
            # ★ 重點：這裡的每個元件都必須加上 key=...，且不可以有 value=... (因為預設值已經由 session_state 控制)
            
            with col1:
                st.markdown("##### 🏢 基本條件")
                # Multiselect 比較特別，它的預設值由 default 參數控制，我們傳入 session_state
                search_txt = st.text_input("🔍 搜尋股票", placeholder="例如：2330 或 台積電", key="search_input")
                selected_industry = st.multiselect("產業分類", all_industries, key='sel_industry', help="資料來源：證交所產業分類")
                price_opt = st.selectbox("股價範圍", ["不拘", "100 元以上 (高價)", "50 ~ 100 元 (中價)", "10 ~ 50 元 (銅板)", "10 元以下 (低價)"], key='sel_price', help="篩選目前的最新收盤價 (Close)")
                capital_opt = st.selectbox("股本規模", ["不拘", "小型股 (< 10億)", "中型股 (10億 ~ 50億)", "大型股 (> 50億)", "超大型權值股 (> 200億)"], key='sel_capital', help="計算公式：發行股數 × 10元面額 / 1億 (單位：億元)")
            
            with col2:
                st.markdown("##### 📈 技術面")
                position_opt = st.selectbox(f"位階高低 ({period_val.upper()})", ["不拘", "底部 (0 ~ 0.2)", "低檔 (0.2 ~ 0.4)", "中階 (0.4 ~ 0.6)", "高檔 (0.6 ~ 0.8)", "頭部 (0.8 ~ 1.0)"], key='sel_pos', help="計算公式：(目前股價 - N年最低價) / (N年最高價 - N年最低價)")
                vol_ma5_opt = st.selectbox("5日均量 (週量)", ["不拘", "500 張以上", "1000 張以上", "5000 張以上", "10000 張以上"], key='sel_vol5', help="計算公式：最近 5 個交易日的成交量總和 / 5")
                vol_ma20_opt = st.selectbox("20日均量 (月量)", ["不拘", "500 張以上", "1000 張以上", "5000 張以上", "10000 張以上"], key='sel_vol20', help="計算公式：最近 20 個交易日的成交量總和 / 20")
                change_opt = st.selectbox("今日漲跌", ["不拘", "上漲 (> 0%)", "強勢 (> 3%)", "漲停 (> 9%)", "下跌 (< 0%)", "跌深 (<-3%)"], key='sel_change', help="計算公式：(今日收盤 - 昨日收盤) / 昨日收盤 × 100%")
                vol_spike_opt = st.selectbox("爆量偵測 (vs 20日均量)", ["不拘", "大於 1.5 倍", "大於 2 倍 (倍增)", "大於 3 倍 (爆量)", "大於 5 倍 (天量)"], key='sel_vol_spike')

            with col3:
                st.markdown("##### 💰 獲利能力")
                revenue_opt = st.selectbox("營收成長 (YoY)", ["不拘", "成長 (> 0%)", "高成長 (> 20%)", "爆發 (> 50%)", "衰退 (< 0%)"], key='sel_rev', help="計算公式：(本季營收 - 去年同季營收) / 去年同季營收 × 100% (來源：Yahoo Finance quarterlyRevenueGrowth)")
                streak_opt = st.selectbox("營收連增 (Streak)", ["不拘", "連增 1 年以上", "連增 2 年以上", "連增 3 年以上"], key='sel_streak', help="計算方式：回溯年度財報，計算「本年度營收 > 上年度營收」的連續年數")
                pe_opt = st.selectbox("本益比 (PE)", ["不拘", "10 倍以下 (低估)", "15 倍以下 (合理)", "20 倍以下 (正常)", "25 倍以上 (成長)"], key='sel_pe', help="計算公式：目前股價 / 近四季每股盈餘 (Trailing EPS)")
                eps_growth_opt = st.selectbox("EPS 成長 (YoY)", ["不拘", "成長 (> 0%)", "高成長 (> 20%)", "翻倍 (> 100%)", "衰退 (< 0%)"], key='sel_eps_growth')

            with col4:
                st.markdown("##### 💎 股利與籌碼")
                yield_opt = st.selectbox("殖利率 (%)", ["不拘", "3% 以上 (及格)", "5% 以上 (高股息)", "7% 以上 (超高配)"], key='sel_yield', help="計算公式：最近一年發放現金股利 / 目前股價 × 100%")
                beta_opt = st.selectbox("Beta (波動)", ["不拘", "大於 1 (活潑)", "大於 1.5 (攻擊)", "小於 1 (穩健)"], key='sel_beta', help="定義：個股報酬率與大盤報酬率的協方差 / 大盤變異數 (來源：Yahoo Finance Beta)")
                eps_opt = st.selectbox("EPS", ["不拘", "0 元以上 (賺錢)", "3 元以上 (穩健)", "5 元以上 (高獲利)"], key='sel_eps', help="每股盈餘 = (稅後淨利 - 特別股股利) / 加權平均股數 (Trailing 12M)")

            
            vol_map = {"不拘": None, "大於 1.5 倍": 1.5, "大於 2 倍 (倍增)": 2.0, "大於 3 倍 (爆量)": 3.0, "大於 5 倍 (天量)": 5.0}
            vol_spike_min = vol_map.get(vol_spike_opt)
            
            eps_map = {"成長 (> 0%)": (0, None), "高成長 (> 20%)": (20, None), "翻倍 (> 100%)": (100, None), "衰退 (< 0%)": (None, 0)}
            eps_growth_min, eps_growth_max = eps_map.get(eps_growth_opt, (None, None))
            # 轉換選單邏輯
            pe_min, pe_max = get_pe_range(pe_opt)
            price_min, price_max = get_price_range(price_opt)
            yield_min, yield_max = get_yield_range(yield_opt)
            eps_min, eps_max = get_eps_range(eps_opt)
            change_min, change_max = get_change_range(change_opt)
            beta_min, beta_max = get_beta_range(beta_opt)
            
            rev_min, rev_max = get_revenue_range(revenue_opt)
            pos_min, pos_max = get_position_range(position_opt)
            cap_min, cap_max = get_capital_range(capital_opt) # 新增
            streak_min = get_streak_range(streak_opt)         # 新增
            vol_ma5_min, vol_ma5_max = get_volume_range(vol_ma5_opt)
            vol_ma20_min, vol_ma20_max = get_volume_range(vol_ma20_opt) # 新增

            filters = {
                'industry': selected_industry if "全部" not in selected_industry else None,
                'period': period_val, # 傳入 1y 或 2y
                'pe_min': pe_min, 'pe_max': pe_max, 'price_min': price_min, 'price_max': price_max,
                'yield_min': yield_min, 'yield_max': yield_max, 'eps_min': eps_min, 'eps_max': eps_max,
                'change_min': change_min, 'change_max': change_max, 
                'beta_min': beta_min, 'beta_max': beta_max,
                'rev_min': rev_min, 'rev_max': rev_max,
                'streak_min': streak_min, # 傳入連增
                'cap_min': cap_min, 'cap_max': cap_max, # 傳入股本
                'pos_min': pos_min, 'pos_max': pos_max,
                'pb_min': None, 'pb_max': None,
                'vol_ma_min': vol_ma5_min, 'vol_ma_max': vol_ma5_max, # 對應 load_data 的 vol_ma_5
                'vol_ma20_min': vol_ma20_min, 'vol_ma20_max': vol_ma20_max, # 新增，需要去 load_data 加個對應
                'vol_spike_min': vol_spike_min,
                'eps_growth_min': eps_growth_min, 'eps_growth_max': eps_growth_max,
            }

        # --- 執行篩選 ---
        df_result = load_data(filters)
        
        
        # 如果有輸入搜尋字串，就過濾 df_result
        if search_txt:
            # 支援代號或名稱模糊搜尋
            df_result = df_result[
                df_result['stock_id'].astype(str).str.contains(search_txt) | 
                df_result['name'].str.contains(search_txt)
            ]

        st.markdown("---")
        
        if not df_result.empty:
            with st.expander("🗺️ 產業資金流向 (熱力圖) - 點擊展開", expanded=True):
                # 1. 資料處理：按產業分組，計算平均漲跌與總市值
                # 為了避免 nan 報錯，先填補 0
                df_treemap = df_result.copy()
                df_treemap['change_pct'] = pd.to_numeric(df_treemap['change_pct'], errors='coerce').fillna(0)
                df_treemap['market_cap'] = df_treemap['market_cap'].fillna(0)

                # 2. 畫圖
                fig_map = px.treemap(
                    df_treemap, 
                    path=['industry', 'name'], # 層級：先看產業，再看個股
                    values='market_cap',       # 方塊大小：市值 (越大代表該股越重要)
                    color='change_pct',        # 顏色深淺：漲跌幅
                    color_continuous_scale=['#00FF00', '#FFFFFF', '#FF0000'], # 綠跌、白平、紅漲 (台股配色)
                    range_color=[-5, 5],       # 顏色範圍鎖定在 -5% ~ +5% 之間，對比更強烈
                    title=f"🔥 篩選結果產業熱力圖 (共 {len(df_result)} 檔，方塊大小=市值)"
                )
                fig_map.update_layout(margin=dict(t=30, l=10, r=10, b=10), height=400)
                st.plotly_chart(fig_map, width='stretch')

        # --- 版面分割：左邊列表，右邊詳情 ---
        col_list, col_detail = st.columns([1, 2])
        
        with col_list:
            st.subheader(f"📋 篩選清單 ({len(df_result)})")
            if not df_result.empty:
                df_show = df_result.copy()
                
                # 1. 定義所有要顯示的欄位 (加入週/月均量)
                all_cols = [
                    'stock_id', 'name', 'industry', 
                    'close', 'change_pct', 'vol_spike', 'position', 'beta',
                    'revenue_growth', 'eps_growth', 'revenue_streak',
                    'pe_ratio', 'pb_ratio', 'yield_rate', 'eps',
                    'capital', 'vol_ma_5', 'vol_ma_20' # ★ 新增這兩個
                ]
                
                # 防呆補 0
                for c in all_cols:
                    if c not in df_show.columns: df_show[c] = 0
                
                df_show = df_show[all_cols] # 確保順序與濾除多餘欄位

                # 2. 強制轉數字
                numeric_cols = [
                    'close', 'change_pct', 'vol_spike', 'position', 'beta',
                    'revenue_growth', 'eps_growth', 'revenue_streak',
                    'pe_ratio', 'pb_ratio', 'yield_rate', 'eps', 'capital',
                    'vol_ma_5', 'vol_ma_20' # ★ 記得轉數字
                ]
                for c in numeric_cols:
                    df_show[c] = pd.to_numeric(df_show[c], errors='coerce').fillna(0)

                # 3. 極簡配色風格設定
                # 只针对：爆量(紅)、營收/EPS成長(綠)、位階(藍)、連增(紫)
                event = st.dataframe(
                    df_show.style.format({
                        'close': '{:.2f}', 
                        'change_pct': '{:+.2f}%',
                        'vol_spike': '{:.1f}倍', 
                        'position': '{:.2f}',
                        'beta': '{:.2f}',
                        'revenue_growth': '{:+.2f}%',
                        'eps_growth': '{:+.2f}%',
                        'revenue_streak': '{:.0f}年',
                        'pe_ratio': '{:.1f}', 
                        'pb_ratio': '{:.2f}',
                        'yield_rate': '{:.2f}%', 
                        'eps': '{:.2f}',
                        'capital': '{:.1f}億',
                        'vol_ma_5': '{:,.0f}張',  # ★ 加逗號比較好讀
                        'vol_ma_20': '{:,.0f}張'
                    })
                    # ★★★ 配色區 (只留你指定的重點) ★★★
                    # 1. 爆量：越紅越誇張
                    .background_gradient(subset=['vol_spike'], cmap='Reds', vmin=1, vmax=5)
                    # 2. 成長雙引擎：綠色代表生機
                    .background_gradient(subset=['revenue_growth', 'eps_growth'], cmap='Greens', vmin=0, vmax=50)
                    # 3. 位階：藍色 (深藍代表高檔，淺藍低檔)
                    .background_gradient(subset=['position'], cmap='Blues', vmin=0, vmax=1)
                    # 4. 連增年數：紫色 (代表累積)
                    .background_gradient(subset=['revenue_streak'], cmap='Purples', vmin=0, vmax=5)
                    # (漲跌幅 change_pct 我先拿掉背景色，讓畫面乾淨，只保留數值)
                    ,
                    
                    column_config={
                        "stock_id": "代號", "name": "名稱", "industry": "產業",
                        "close": "股價", "change_pct": "漲跌", 
                        "vol_spike": "爆量倍數", "position": "位階", "beta": "波動",
                        "revenue_growth": "營收成長", "eps_growth": "EPS成長", "revenue_streak": "連增年數",
                        "pe_ratio": "本益比", "pb_ratio": "股淨比", "yield_rate": "殖利率", 
                        "capital": "股本",
                        "vol_ma_5": "5日均量", 
                        "vol_ma_20": "20日均量",
                        "eps": "EPS"  # 確保 EPS 有顯示名稱
                    },
                    # ★★★ 修改這裡：去掉 change_pct，把 eps 加在最後面 ★★★
                    column_order=[
                        "stock_id", "name", "industry", 
                        "close", "vol_spike",        # 籌碼 (去掉 change_pct)
                        "position", "revenue_growth", "eps_growth", "revenue_streak", # 成長/技術
                        "vol_ma_5", "vol_ma_20",
                        "pe_ratio", "yield_rate", "capital",                 # 基本面
                        "eps"                                                # ★ EPS 放最後
                    ],
                    width="stretch", height=600, on_select="rerun", selection_mode="single-row", hide_index=True
                )

                # 取得使用者選了哪一行
                if len(event.selection.rows) > 0:
                    selected_row_index = event.selection.rows[0]
                    selected_stock_id = df_result.iloc[selected_row_index]['stock_id']
                else:
                    # 預設選第一筆，避免右邊空白
                    selected_stock_id = df_result.iloc[0]['stock_id']
                    
            else:
                st.warning("無符合條件股票")
                selected_stock_id = None

        with col_detail:
            if selected_stock_id:
                # 取得該股票詳細資料
                row = df_result[df_result['stock_id'] == selected_stock_id].iloc[0]
                
                st.subheader(f"📊 {row['stock_id']} {row['name']} 個股儀表板")
                
                # 1. 關鍵指標卡片 (Metrics)
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("股價", f"{row['close']:.2f}", f"{row['change_pct']:+.2f}%")
                
                # 顯示營收連增與成長率
                streak_text = f"連增{row['revenue_streak']}年" if row['revenue_streak'] > 0 else "無連增"
                m2.metric("營收表現", f"{row['revenue_growth']:+.1f}%", streak_text)
                
                # 顯示位階 (根據目前選擇的週期)
                pos_label = f"位階 ({period_val.upper()})"
                m3.metric(pos_label, f"{row['position']:.2f}", help="0=低點, 1=高點")
                
                # 顯示股本
                m4.metric("股本", f"{row['capital']:.1f} 億" if pd.notna(row['capital']) else "N/A")

                # 2. 基本面資訊區
                with st.container():
                    st.info(f"📌 **基本面概況**：產業別 [{row['industry']}] | EPS [{row['eps']}] | Beta [{row['beta']}]")
                
                # 3. K 線圖                
                hist = load_stock_history(selected_stock_id)
                
                if not hist.empty:
                    # 轉數字
                    for c in ['open', 'high', 'low', 'close', 'ma_5', 'ma_20', 'volume']:
                        hist[c] = pd.to_numeric(hist[c], errors='coerce')
                    
                    chart_type = st.radio("K 線週期", ["日線", "週線"], horizontal=True, key='chart_period')

                    # [修改點] 根據按鈕決定資料
                    if chart_type == "週線":
                        plot_data = resample_to_weekly(hist)
                    else:
                        plot_data = hist
                        
                    # 傳入 chart_type 給標題用
                    fig = plot_candlestick(plot_data, selected_stock_id, row['name'], chart_type)
                    st.plotly_chart(
                        fig, 
                        width="stretch", 
                        config={
                            'scrollZoom': True,        # 開啟滑鼠滾輪縮放
                            'displayModeBar': True,    # 顯示右上角工具列
                            'displaylogo': False       # 隱藏 plotly logo
                        }
                    )
                else:
                    st.warning("無歷史股價資料")
            else:
                st.info("👈 請從左側清單選擇一檔股票查看詳情")

    # ==========================================
    # 頁面 2: AI 相似股搜尋 (Similarity)
    # ==========================================
    elif selected_page == "AI 相似股 (Similarity)":
        st.title("🧬 AI 潛力股 DNA 比對")
        
        col_left, col_right = st.columns([1, 2])
        
        with col_left:
            st.info("輸入一檔目標股票，AI 將根據您設定的因子權重，找出全台股中最像的標的。")
            
            all_stocks_list = get_all_stocks_list()
            default_idx = 0
            for i, s in enumerate(all_stocks_list):
                if "2330" in s: default_idx = i; break
            def reset_ai_state():
                st.session_state.ai_triggered = False

            selected_stock_str = st.selectbox("🔍 DNA 來源 (目標股票)", all_stocks_list, index=default_idx, on_change = reset_ai_state) # ★ 當換股票時，重置分析狀態
            target_id = selected_stock_str.split()[0] if selected_stock_str else "2330"

            col_opt1, col_opt2 = st.columns(2)
            with col_opt1:
                period_mode = st.radio("位階基準", ["近 1 年", "近 2 年"], horizontal=True)
                period_val = '2y' if "2" in period_mode else '1y'
            with col_opt2:
                st.write("") # 排版用空格
                st.write("") 
                lock_industry = st.checkbox("🔒 僅限同產業", value=False, help="勾選後，只會從相同產業中尋找相似股")

            st.markdown("---")
        
            
            st.write("⚖️ **因子權重設定 (0=不考慮, 5=最重要)**")
            
            with st.expander("1️⃣ 基本面 (體質)", expanded=True):
                w_pe = st.slider("本益比 (PE)", 0, 5, 3, help="公式：股價 / EPS")
                w_yield = st.slider("殖利率 (Yield)", 0, 5, 3, help="公式：現金股利 / 股價")
                w_revenue = st.slider("營收成長 (YoY)", 0, 5, 3, help="公式：(本季營收 - 去年同季) / 去年同季")
                w_streak = st.slider("營收連增 (Streak)", 0, 5, 3, help="定義：年度營收連續成長年數")
                w_eps = st.slider("每股盈餘 (EPS)", 0, 5, 3, help="定義：Trailing 12-Month EPS")
                w_pb = st.slider("股價淨值比 (PB)", 0, 5, 3, help="公式：股價 / 每股淨值")
                w_capital = st.slider("股本規模 (Capital)", 0, 5, 3, help="公式：股數 × 10 / 1億 (單位：億)")
            
            with st.expander("2️⃣ 技術與籌碼 (趨勢)", expanded=True):
                w_trend = st.slider("K線走勢相似度 (Correlation)", 0, 5, 3, help="比較過去 60 天的股價走勢圖形狀。權重越高，找出來的股票線型會越像目標股")
                w_position = st.slider(f"位階高低 ({period_val.upper()})", 0, 5, 3, help="公式：(股價 - 期間低點) / (期間高點 - 期間低點)")
                w_vol5 = st.slider("5日均量 (週量)", 0, 5, 3, help="定義：過去 5 日成交量平均")
                w_vol20 = st.slider("20日均量 (月量)", 0, 5, 3, help="定義：過去 20 日成交量平均")
                w_bias20 = st.slider("月線乖離 (Bias 20)", 0, 5, 3, help="公式：(股價 - 20MA) / 20MA")
                w_bias60 = st.slider("季線乖離 (Bias 60)", 0, 5, 3, help="公式：(股價 - 60MA) / 60MA")
                w_beta = st.slider("波動度 (Beta)", 0, 5, 3, help="定義：相對於大盤的波動係數")
                w_change = st.slider("今日漲跌", 0, 5, 3, help="公式：(今收 - 昨收) / 昨收")  

            # 1. 確保 Session State 有這個變數 (用來記憶是否按過分析)
            if 'ai_triggered' not in st.session_state:
                st.session_state.ai_triggered = False

            # 2. 按鈕被按下時，將狀態設為 True
            if st.button("🚀 開始 AI 分析", type="primary", width="stretch"):
                st.session_state.ai_triggered = True
            
        with col_right:
            # 3. 改成檢查 Session State，而不是只檢查按鈕那一瞬間的狀態
            if st.session_state.ai_triggered:
                with st.spinner(f"正在分析... (基準: {period_val})"):
                    try:
                    # 1. 執行分析
                        weights = {
                            'pe': w_pe, 'yield': w_yield, 'pb': w_pb, 'eps': w_eps, 
                            'revenue': w_revenue, 'streak': w_streak, 'capital': w_capital,
                            'bias20': w_bias20, 'bias60': w_bias60, 'beta': w_beta, 'change': w_change, 
                            'position': w_position, 'vol5': w_vol5, 'vol20': w_vol20, 'trend': w_trend
                        }

                        similar_stocks, error = analysis.find_similar_stocks(
                            target_id, weights, period=period_val, industry_only=lock_industry
                        )
                        
                        if error:
                            st.error(error)
                        else:
                            st.success(f"✅ 找到與 {target_id} 最像的股票！")
                            
                            sim_show = similar_stocks.copy()
                            
                            # 1. 補算「爆量倍數」
                            sim_show['vol_spike'] = sim_show.apply(
                                lambda x: x['volume'] / x['vol_ma_20'] if pd.notna(x['vol_ma_20']) and x['vol_ma_20'] > 0 else 0, 
                                axis=1
                            )

                            # 2. 補齊欄位 (加入週/月均量)
                            all_cols = [
                                'stock_id', 'name', 'industry', 'similarity',
                                'close', 'change_pct', 'vol_spike', 'position', 'beta',
                                'revenue_growth', 'eps_growth', 'revenue_streak',
                                'pe_ratio', 'pb_ratio', 'yield_rate', 'eps', 'capital',
                                'vol_ma_5', 'vol_ma_20' # ★ 新增
                            ]
                            for c in all_cols:
                                if c not in sim_show.columns: sim_show[c] = 0

                            # 3. 強制轉數字
                            numeric_cols = [
                                'similarity', 'close', 'change_pct', 'vol_spike', 'position', 'beta',
                                'revenue_growth', 'eps_growth', 'revenue_streak',
                                'pe_ratio', 'pb_ratio', 'yield_rate', 'eps', 'capital',
                                'vol_ma_5', 'vol_ma_20'
                            ]
                            for c in numeric_cols:
                                sim_show[c] = pd.to_numeric(sim_show[c], errors='coerce').fillna(0)

                            # 4. 極簡配色風格 (與篩選頁面一致)
                            # ★★★ 修改 1：加入 on_select 與 selection_mode，讓表格可以點選 ★★★
                            event = st.dataframe(
                                sim_show.style.format({
                                    'similarity': '{:.1f}%',
                                    'close': '{:.2f}', 'change_pct': '{:+.2f}%',
                                    'vol_spike': '{:.1f}倍', 
                                    'position': '{:.2f}', 'beta': '{:.2f}',
                                    'revenue_growth': '{:+.2f}%', 'eps_growth': '{:+.2f}%', 
                                    'revenue_streak': '{:.0f}年',
                                    'pe_ratio': '{:.1f}', 'pb_ratio': '{:.2f}', 'yield_rate': '{:.2f}%', 
                                    'capital': '{:.1f}億',
                                    'vol_ma_5': '{:,.0f}張', 'vol_ma_20': '{:,.0f}張',
                                    'eps': '{:.2f}'
                                })
                                .background_gradient(subset=['similarity'], cmap='Greens')
                                .background_gradient(subset=['vol_spike'], cmap='Reds', vmin=1, vmax=5)
                                .background_gradient(subset=['revenue_growth', 'eps_growth'], cmap='Greens', vmin=0, vmax=50)
                                .background_gradient(subset=['position'], cmap='Blues', vmin=0, vmax=1)
                                .background_gradient(subset=['revenue_streak'], cmap='Purples', vmin=0, vmax=5),
                                
                                column_config={
                                    "stock_id": "代號", "name": "名稱", "industry": "產業", "similarity": "相似度",
                                    "close": "股價", "change_pct": "漲跌", 
                                    "vol_spike": "爆量倍數", "position": "位階", "beta": "波動",
                                    "revenue_growth": "營收成長", "eps_growth": "EPS成長", "revenue_streak": "連增年數",
                                    "pe_ratio": "本益比", "pb_ratio": "股淨比", "yield_rate": "殖利率", 
                                    "capital": "股本",
                                    "vol_ma_5": "5日均量", "vol_ma_20": "20日均量", "eps": "EPS"
                                },
                                column_order=[
                                    "stock_id", "name", "similarity", "industry",
                                    "close", "vol_spike",
                                    "position", "revenue_growth", "eps_growth", "revenue_streak",
                                    "vol_ma_5", "vol_ma_20","pe_ratio", "yield_rate", "capital",
                                    "eps"
                                ],
                                width='stretch',
                                hide_index=True,
                                on_select="rerun",       # ★ 開啟點選功能
                                selection_mode="single-row" # ★ 單選模式
                            )
                            
                            st.markdown("---")
                            
                            # ★★★ 修改 2：決定要顯示哪一檔股票 (預設第二名，或使用者點選的那檔) ★★★
                            target_stock = None
                            
                            # 情況 A: 使用者有點選表格
                            if len(event.selection.rows) > 0:
                                selected_idx = event.selection.rows[0]
                                target_stock = similar_stocks.iloc[selected_idx]
                            
                            # 情況 B: 使用者沒點選，預設顯示「最像的那檔 (排除自己)」
                            # 邏輯：原本的第一名 (iloc[0]) 是本尊，所以我們抓第二名 (iloc[1])
                            elif len(similar_stocks) > 1:
                                target_stock = similar_stocks.iloc[1] 
                            
                            # 情況 C: 如果真的只找到自己 (例如產業內只有一檔)，那就只好顯示自己
                            else:
                                target_stock = similar_stocks.iloc[0]

                            # --- 顯示詳細資料與 K 線圖 ---
                            if target_stock is not None:
                                st.subheader(f"📊 {target_stock['name']} ({target_stock['stock_id']}) - 相似度 {target_stock['similarity']:.1f}%")
                                
                                m1, m2, m3, m4, m5 = st.columns(5)
                                m1.metric("收盤價", f"{target_stock['close']:.2f}")
                                m2.metric(f"位階 ({period_val})", f"{target_stock['position']:.2f}")
                                
                                streak_txt = f"連增{target_stock['revenue_streak']}年" if target_stock['revenue_streak'] > 0 else "無"
                                m3.metric("營收表現", f"{target_stock['revenue_growth']:+.1f}%", streak_txt)
                                
                                m4.metric("股本", f"{target_stock['capital']:.1f} 億")
                                m5.metric("月均量", f"{int(target_stock['vol_ma_20'])} 張" if pd.notna(target_stock['vol_ma_20']) else "N/A")
                                
                                # ★★★ 修改 3：加上 K 線週期切換按鈕 (跟條件篩選頁面一樣) ★★★
                                chart_type_ai = st.radio("K 線週期", ["日線", "週線"], horizontal=True, key='chart_period_ai')

                                # 載入歷史資料
                                hist = load_stock_history(target_stock['stock_id'])
                                
                                if not hist.empty:
                                    for c in ['open', 'high', 'low', 'close', 'ma_5', 'ma_20', 'volume']:
                                        hist[c] = pd.to_numeric(hist[c], errors='coerce')
                                    
                                    # 根據按鈕決定資料
                                    if chart_type_ai == "週線":
                                        plot_data = resample_to_weekly(hist)
                                    else:
                                        plot_data = hist
                                    
                                    # 繪圖
                                    fig = plot_candlestick(plot_data, target_stock['stock_id'], target_stock['name'], chart_type_ai)
                                    st.plotly_chart(fig, width='stretch', config={'scrollZoom': True})
                                else:
                                    st.warning(f"⚠️ 找不到 {target_stock['stock_id']} 的歷史股價資料")
                    except Exception as e:
                        st.error(f"分析錯誤: {e}")

    # ==========================================
    # 頁面 3: 系統設定 (UI 更新版)
    # ==========================================
    elif selected_page == "系統設定":
        st.title("⚙️ 系統維護")
        
        st.info("💡 智慧增量更新：系統會自動檢查每檔股票的最後日期，只抓取缺漏的資料。若資料已是最新，會自動跳過。")

        # 這裡不使用 subprocess，改用直接呼叫 python 函數
        if st.button("🔄 立即更新 (Smart Update)", type="primary"):
            
            # 1. 建立 UI 元件
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # 2. 執行更新 (傳入 UI 元件讓 fetch_data 控制)
            try:
                # 這裡要引用 fetch_data 模組
                import fetch_data 
                
                # 開始跑回圈
                fetch_data.update_stock_data(progress_bar, status_text)
                
                # 3. 完成
                progress_bar.progress(100)
                status_text.success("✅ 所有資料更新完成！請重新整理頁面以載入最新數據。")
                st.balloons() # 放個氣球慶祝一下
                
            except Exception as e:
                st.error(f"更新發生錯誤: {e}")

if __name__ == "__main__":
    main()