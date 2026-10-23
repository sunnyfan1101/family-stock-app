"""Deterministic calculations. No network, credentials, or implicit database access."""
import pandas as pd


def close_returns(close):
    """Close-to-previous-observed-close %, NOT exchange reference-price return."""
    close = close[~close.index.duplicated(keep='last')].sort_index()
    previous = close.where(close > 0).ffill().shift(1)
    return ((close.where(close > 0) / previous) - 1) * 100


def weekly_averages(close):
    """Live week + preceding trading-week closes, point-in-time, no future data.

    A week with no observations is excluded. Require 5/20 observed weeks.
    Never backfill a Friday final close into earlier days of that week.
    """
    close = close.sort_index()
    weeks = close.index.to_period('W-FRI')
    final = close.groupby(weeks).last()
    result = pd.DataFrame(index=close.index)
    for n in (5, 20):
        prior_sum = final.shift(1).rolling(n - 1, min_periods=n - 1).sum()
        result[f'weekly_ma_{n}'] = (prior_sum.reindex(weeks).to_numpy() + close.to_numpy()) / n
    return result


def ensure_revenue_columns(conn):
    fields = {'revenue_monthly_yoy': 'REAL', 'revenue_ytd_yoy': 'REAL',
              'revenue_period': 'TEXT', 'revenue_ttm': 'REAL'}
    existing = {r[1] for r in conn.execute('PRAGMA table_info(stocks)')}
    for name, kind in fields.items():
        if name not in existing:
            conn.execute(f'ALTER TABLE stocks ADD COLUMN {name} {kind}')


def sync_revenue(conn):
    """Legacy revenue_growth = YTD YoY (%); TTM uses monthly_revenue units.

    Recompute from amounts, not cached zero-filled growth columns. Unknown is
    NULL. Do not relabel legacy mixed quarterly/monthly figures as quarterly.
    """
    ensure_revenue_columns(conn)
    conn.execute('UPDATE stocks SET revenue_growth=NULL, revenue_ttm=NULL, '
                 'revenue_monthly_yoy=NULL, revenue_ytd_yoy=NULL, revenue_period=NULL')
    groups = {}
    for sid, year, month, amount in conn.execute(
            'SELECT stock_id, year, month, revenue FROM monthly_revenue'):
        groups.setdefault(sid, {})[int(year)*12+int(month)-1] = amount
    def total(values, keys):
        amounts = [values.get(k) for k in keys]
        return sum(amounts) if all(v is not None and pd.notna(v) for v in amounts) else None
    def growth(current, previous):
        return (current / previous - 1)*100 if current is not None and previous is not None and previous > 0 else None
    for sid, values in groups.items():
        latest = max(values)
        year, month0 = divmod(latest, 12)
        monthly = growth(values.get(latest), values.get(latest-12))
        ytd = growth(total(values, range(year*12, latest+1)),
                     total(values, range((year-1)*12, latest-11)))
        ttm = total(values, range(latest-11, latest+1))
        conn.execute('UPDATE stocks SET revenue_growth=?, revenue_ytd_yoy=?, '
                     'revenue_monthly_yoy=?, revenue_ttm=?, revenue_period=? WHERE stock_id=?',
                     (ytd, ytd, monthly, ttm, f'{year:04d}-{month0+1:02d}', sid))


def refresh_price_summary(conn, sid):
    """Use exactly the last observed trading day; never the wall-clock date."""
    rows = conn.execute('SELECT close,volume FROM daily_prices WHERE stock_id=? AND close>0 ORDER BY date DESC LIMIT 500', (sid,)).fetchall()
    if not rows:
        return
    close = pd.Series([r[0] for r in rows], dtype=float)
    volume = pd.Series([r[1] for r in rows], dtype=float)
    def average(n):
        subset=volume.iloc[:n]
        return float(subset.mean()) if len(subset)==n and subset.notna().all() else None
    conn.execute('UPDATE stocks SET vol_ma_5=?,vol_ma_20=?,year_high=?,year_low=?,year_high_2y=?,year_low_2y=? WHERE stock_id=?',
                 (average(5),average(20),float(close.head(250).max()),float(close.head(250).min()),float(close.max()),float(close.min()),sid))
