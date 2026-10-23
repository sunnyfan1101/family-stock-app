"""Offline rehearsal only: explicit read-only source, exclusive new destination.

No fetch, compression, publication or cron. Rebuilt returns are close-to-close,
not exchange reference-price returns. Preserves OHLCV and institution columns.
"""
import argparse
import json
from pathlib import Path
import sqlite3
import pandas as pd
from data_correctness import close_returns, weekly_averages, sync_revenue
from fetch_precompute import refresh_latest_stock_snapshot


def rebuild(conn):
    stocks = [r[0] for r in conn.execute('SELECT stock_id FROM stocks')]
    for i, sid in enumerate(stocks):
        df = pd.read_sql_query('SELECT date,close,volume FROM daily_prices WHERE stock_id=? ORDER BY date', conn, params=(sid,))
        if df.empty:
            continue
        clean = df[pd.to_numeric(df.close, errors='coerce')>0]
        close = pd.Series(clean.close.to_numpy(), index=pd.to_datetime(clean.date))
        vol = pd.Series(clean.volume.to_numpy(), index=close.index)
        if close.empty:
            continue
        values = pd.DataFrame({'change': close_returns(close)})
        for n in (5,20,60):
            values[f'ma{n}'] = close.rolling(n).mean()
        values = values.join(weekly_averages(close))
        def val(x):
            return None if pd.isna(x) else float(x)
        conn.executemany('UPDATE daily_prices SET change_pct=?, ma_5=?, ma_20=?, ma_60=?, weekly_ma_5=?, weekly_ma_20=? WHERE stock_id=? AND date=?',
                         [tuple(val(x) for x in row[1:])+(sid,row[0].strftime('%Y-%m-%d')) for row in values.itertuples(name=None)])
        conn.execute('UPDATE daily_prices SET change_pct=NULL,ma_5=NULL,ma_20=NULL,ma_60=NULL,weekly_ma_5=NULL,weekly_ma_20=NULL WHERE stock_id=? AND (close IS NULL OR close<=0)',(sid,))
        latest = close.iloc[-1]
        ma20, ma60 = values.ma20.iloc[-1], values.ma60.iloc[-1]
        v5, v20 = vol.rolling(5).mean().iloc[-1], vol.rolling(20).mean().iloc[-1]
        def position(n):
            lo, hi = close.tail(n).min(),close.tail(n).max()
            return (latest-lo)/(hi-lo) if hi>lo else .5
        conn.execute('UPDATE stocks SET vol_ma_5=?,vol_ma_20=?,bias_20=?,bias_60=?,vol_spike=?,position_1y=?,position_2y=?,year_high=?,year_low=?,year_high_2y=?,year_low_2y=? WHERE stock_id=?',
                     (val(v5),val(v20),val(latest/ma20-1) if ma20>0 else None,
                      val(latest/ma60-1) if ma60>0 else None,
                      val(vol.iloc[-1]/v20) if v20>0 else None,position(250),position(500),
                      val(close.tail(250).max()),val(close.tail(250).min()),val(close.tail(500).max()),val(close.tail(500).min()),sid))
        if i % 250 == 0:
            print(f'rebuilt {i+1}/{len(stocks)}', flush=True)
    sync_revenue(conn)
    refresh_latest_stock_snapshot(conn)
    conn.commit()


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--source',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args()
    source=a.source.resolve(strict=True)
    target=a.output.resolve()
    if source==target:
        p.error('source and output must differ')
    # Atomic exclusive creation prevents overwriting either existing data or a symlink.
    with target.open('xb'):
        pass
    with sqlite3.connect(source.as_uri()+'?mode=ro',uri=True) as original:
        with sqlite3.connect(target) as copy:
            original.backup(copy)
    with sqlite3.connect(target) as c:
        before=c.execute('SELECT count(*),sum(close),sum(volume) FROM daily_prices').fetchone()
        rebuild(c)
        after=c.execute('SELECT count(*),sum(close),sum(volume) FROM daily_prices').fetchone()
        assert before==after, 'source observations changed'
        checks={
          'integrity':c.execute('PRAGMA integrity_check').fetchone()[0],
          'rows':after[0],
          'bias_mismatch':c.execute('SELECT count(*) FROM latest_stock_snapshot WHERE ma_20>0 AND abs(bias_20-(close/ma_20-1))>0.00000001').fetchone()[0],
          'volume_ratio_mismatch':c.execute('SELECT count(*) FROM latest_stock_snapshot WHERE vol_ma_20>0 AND abs(vol_spike-volume/vol_ma_20)>0.00000001').fetchone()[0],
          'returns_mismatch':c.execute('WITH x AS (SELECT close,change_pct,lag(close) OVER(PARTITION BY stock_id ORDER BY date) prev FROM daily_prices WHERE close>0) SELECT count(*) FROM x WHERE prev>0 AND (change_pct IS NULL OR abs(change_pct-(close/prev-1)*100)>0.00000001)').fetchone()[0],
          'missing_close':c.execute('SELECT count(*) FROM daily_prices WHERE close IS NULL').fetchone()[0],
        }
        assert checks['integrity']=='ok' and all(checks[k]==0 for k in ('bias_mismatch','volume_ratio_mismatch','returns_mismatch'))
        print(json.dumps(checks),flush=True)

if __name__=='__main__':
    main()
