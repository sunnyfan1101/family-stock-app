import pandas as pd
import numpy as np
import sqlite3
from sklearn.preprocessing import StandardScaler

def get_connection():
    return sqlite3.connect("stock_data.db")

# --- 1. 計算所有股票與目標股票的 K 線相關係數 ---
def get_price_correlation(target_id, days=60):
    conn = get_connection()
    try:
        sql = f"""
        SELECT stock_id, date, close 
        FROM daily_prices 
        WHERE date >= date('now', '-120 days')
        """
        df = pd.read_sql(sql, conn)
        df['date'] = pd.to_datetime(df['date'])
        
        price_matrix = df.pivot(index='date', columns='stock_id', values='close')
        
        if target_id not in price_matrix.columns:
            return None
            
        recent_matrix = price_matrix.tail(days)
        corr_series = recent_matrix.corrwith(recent_matrix[target_id])
        
        corr_df = corr_series.to_frame(name='trend_corr').reset_index()
        corr_df['trend_corr'] = corr_df['trend_corr'].fillna(0)
        
        return corr_df
        
    except Exception as e:
        print(f"Correlation Error: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

# --- 2. 抓取所有特徵資料 (含三率 + 盤整天數) ---
def get_all_stock_features():
    conn = get_connection()
    try:
        # ★★★ 修改重點：SELECT 加入 s.consolidation_days ★★★
        sql = """
        SELECT 
            s.stock_id, s.name, s.industry,
            s.pe_ratio, s.yield_rate, s.pb_ratio, s.eps, s.beta, 
            s.revenue_growth, s.revenue_streak, 
            s.capital, s.eps_growth,
            s.vol_ma_5, s.vol_ma_20,
            s.year_high, s.year_low, s.year_high_2y, s.year_low_2y, 
            s.gross_margin, s.operating_margin, s.pretax_margin, s.net_margin,
            s.consolidation_days,  -- ★ 新增這行
            d.close, d.volume, d.change_pct, d.ma_5, d.ma_20, d.ma_60
        FROM stocks s
        JOIN daily_prices d ON s.stock_id = d.stock_id
        WHERE d.date = (SELECT MAX(date) FROM daily_prices dp WHERE dp.stock_id = s.stock_id)
        """
        df = pd.read_sql(sql, conn)
    
    except Exception as e:
        print(f"SQL Error: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

    # 確保數值格式正確
    cols = ['pe_ratio', 'yield_rate', 'pb_ratio', 'eps', 'beta', 'change_pct', 'close', 
            'revenue_growth', 'capital', 'eps_growth', 'revenue_streak', 'vol_ma_5', 'vol_ma_20',
            'year_high', 'year_low', 'year_high_2y', 'year_low_2y', 'ma_20', 'ma_60', 
            'gross_margin', 'operating_margin', 'pretax_margin', 'net_margin', 
            'consolidation_days'] # ★ 加入列表
    
    for col in cols: 
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

# --- 3. 核心演算法：尋找相似股 ---
def find_similar_stocks(target_id, weights, period='1y', industry_only=False):
    df = get_all_stock_features()
    
    if df.empty or target_id not in df['stock_id'].values:
        return None, f"找不到代號 {target_id}，請確認資料庫。"

    # K 線相關係數
    corr_df = get_price_correlation(target_id, days=60)
    if corr_df is not None and not corr_df.empty:
        df = df.merge(corr_df, on='stock_id', how='left')
        df['trend_corr'] = df['trend_corr'].fillna(0)
    else:
        df['trend_corr'] = 0

    if industry_only:
        target_inds = df.loc[df['stock_id'] == target_id, 'industry'].values
        if len(target_inds) > 0:
            target_industry = target_inds[0]
            df = df[df['industry'] == target_industry].copy()
            df.reset_index(drop=True, inplace=True) 
        
        if len(df) < 2:
            return None, f"該產業只有一檔股票，無法比對。"

    df_display = df.copy() 

    # --- 4. 計算衍生特徵 ---
    df['bias_20'] = (df['close'] - df['ma_20']) / df['ma_20'].replace(0, np.nan)
    df['bias_60'] = (df['close'] - df['ma_60']) / df['ma_60'].replace(0, np.nan)
    
    df['capital_log'] = np.log1p(df['capital'].fillna(0))
    df['vol_ma5_log'] = np.log1p(df['vol_ma_5'].fillna(0))
    df['vol_ma20_log'] = np.log1p(df['vol_ma_20'].fillna(0))
    
    # 盤整天數取 log (避免 200 天跟 1 天差距過大拉壞權重)
    df['consolidation_log'] = np.log1p(df['consolidation_days'].fillna(0)) # ★ 處理

    if period == '2y':
        high_col, low_col = 'year_high_2y', 'year_low_2y'
    else:
        high_col, low_col = 'year_high', 'year_low'
    
    df['position'] = (df['close'] - df[low_col]) / (df[high_col] - df[low_col]).replace(0, np.nan)
    
    # --- 5. 定義特徵向量 (加入 consolidation_log) ---
    features = [
        'pe_ratio', 'yield_rate', 'pb_ratio', 'eps', 
        'gross_margin', 'operating_margin', 'net_margin',
        'revenue_growth', 'revenue_streak',
        'bias_20', 'bias_60', 'beta', 'change_pct', 'position',
        'capital_log', 'vol_ma5_log', 'vol_ma20_log',
        'consolidation_log', # ★ 加入這個特徵
        'trend_corr' 
    ]
    
    scaler = StandardScaler()
    
    for col in features:
        if col not in df.columns: df[col] = 0
        df[col] = df[col].replace([np.inf, -np.inf], np.nan)
        median_val = df[col].median()
        df[col] = df[col].fillna(median_val)
        
        upper = df[col].quantile(0.99)
        lower = df[col].quantile(0.01)
        df[col] = df[col].clip(lower, upper)

    scaled_data = scaler.fit_transform(df[features])

    # --- 6. 加權計算 (加入 consolidation 權重) ---
    w_vec = np.array([
        weights.get('pe', 3), weights.get('yield', 3), weights.get('pb', 3), weights.get('eps', 3), 
        weights.get('gross', 3), weights.get('operating', 3), weights.get('net', 3),
        weights.get('revenue', 3), weights.get('streak', 3),
        weights.get('bias20', 3), weights.get('bias60', 3), weights.get('beta', 3), weights.get('change', 3), 
        weights.get('position', 3),
        weights.get('capital', 3), weights.get('vol5', 3), weights.get('vol20', 3),
        weights.get('consolidation', 3), # ★ 加入權重 (對應 features 順序)
        weights.get('trend', 3)
    ])
    
    weighted_data = scaled_data * w_vec
    
    target_idx = df.index[df['stock_id'] == target_id][0]
    target_vec = weighted_data[target_idx]
    
    distances = np.linalg.norm(weighted_data - target_vec, axis=1)
    
    max_dist = np.max(distances)
    if max_dist == 0: max_dist = 1
    similarity_scores = (1 - (distances / max_dist)) * 100
    
    df_display['similarity'] = similarity_scores
    df_display['position'] = df['position']
    
    # --- 7. 回傳結果 ---
    result_cols = [
        'stock_id', 'name', 'industry', 'close', 'similarity', 
        'change_pct', 'trend_corr', 'position', 
        'volume', 'vol_ma_20', 'vol_ma_5',
        'revenue_growth', 'eps_growth', 'revenue_streak',
        'pe_ratio', 'pb_ratio', 'yield_rate', 'eps', 
        'gross_margin', 'operating_margin', 'pretax_margin', 'net_margin', 
        'consolidation_days', # ★ 回傳
        'capital', 'beta'
    ]
    
    available_cols = [c for c in result_cols if c in df_display.columns]
    result = df_display[available_cols].sort_values('similarity', ascending=False).head(11)
    
    return result, None