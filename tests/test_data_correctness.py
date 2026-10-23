import unittest
import sqlite3
import pandas as pd
import ast
from pathlib import Path
from unittest.mock import patch, Mock
from data_correctness import close_returns, weekly_averages, sync_revenue


class CorrectnessTests(unittest.TestCase):
    def test_revenue_yoy_gap_is_unknown_in_source_table(self):
        from fetch_revenue import calculate_yoy
        data=pd.DataFrame([
            {'stock_id':'X','revenue_year':2025,'revenue_month':m,'revenue':100.}
            for m in (1,3)
        ]+[
            {'stock_id':'X','revenue_year':2026,'revenue_month':m,'revenue':200.}
            for m in (1,2,3)
        ])
        actual=calculate_yoy(data)
        march=actual[(actual.revenue_year==2026)&(actual.revenue_month==3)].iloc[0]
        self.assertEqual(march.yoy_growth,100.)
        self.assertTrue(pd.isna(march.cumulative_yoy))

    def test_finmind_http_error_is_unknown_not_zero(self):
        from fetch_fundamentals_finmind import fetch_fundamentals_finmind
        with patch('fetch_fundamentals_finmind.requests.get', return_value=Mock(status_code=503)):
            self.assertIsNone(fetch_fundamentals_finmind('2330',100))

    def test_finmind_missing_fields_are_unknown(self):
        from fetch_fundamentals_finmind import fetch_fundamentals_finmind
        statement=Mock(status_code=200)
        statement.json.return_value={'data':[{'date':'2026-06-30','type':'EPS','value':2.0}]}
        dividend=Mock(status_code=200)
        dividend.json.return_value={'data':[]}
        with patch('fetch_fundamentals_finmind.requests.get', side_effect=[statement,dividend]):
            data=fetch_fundamentals_finmind('2330',100)
        self.assertEqual(data['eps'],2.)
        self.assertIsNone(data['gross_margin'])
        self.assertIsNone(data['yield_rate'])

    def test_market_stats_uses_historical_window(self):
        tree=ast.parse(Path('fetch_data.py').read_text())
        sql=next(n.value for n in ast.walk(tree) if isinstance(n,ast.Constant) and isinstance(n.value,str) and 'WITH point_in_time AS' in n.value)
        c=sqlite3.connect(':memory:')
        c.execute('CREATE TABLE daily_prices(stock_id TEXT,date TEXT,close REAL)')
        c.executemany('INSERT INTO daily_prices VALUES (?,?,?)',[('X','2026-01-01',100),('X','2026-01-02',80),('X','2026-01-03',1)])
        self.assertEqual(c.execute(sql,('2026-01-02','2026-01-02')).fetchone()[0],1)

    def test_production_upsert_preserves_institution_fields(self):
        tree=ast.parse(Path('fetch_data.py').read_text())
        sql=next(n.value for n in ast.walk(tree) if isinstance(n,ast.Constant) and isinstance(n.value,str) and 'ON CONFLICT(stock_id, date)' in n.value)
        c=sqlite3.connect(':memory:')
        c.execute('CREATE TABLE daily_prices(stock_id TEXT,date TEXT,open REAL,high REAL,low REAL,close REAL,volume REAL,change_pct REAL,ma_5 REAL,ma_20 REAL,ma_60 REAL,foreign_buy REAL,PRIMARY KEY(stock_id,date))')
        c.execute('INSERT INTO daily_prices(stock_id,date,foreign_buy) VALUES ("X","2026-01-01",123)')
        c.execute(sql,('X','2026-01-01',1,1,1,1,100,None,None,None,None))
        self.assertEqual(c.execute('SELECT foreign_buy,change_pct FROM daily_prices').fetchone(),(123,None))

    def test_write_precedes_metrics(self):
        source=Path('fetch_data.py').read_text()
        updater=source[source.index('def update_stock_data'):]
        self.assertLess(updater.index('ON CONFLICT(stock_id, date)'),updater.index('calculate_precompute_for_stock(cursor, stock_id)'))

    def test_summary_uses_observed_rows_not_stale_stocks(self):
        from data_correctness import refresh_price_summary
        c=sqlite3.connect(':memory:')
        c.executescript('CREATE TABLE stocks(stock_id TEXT,vol_ma_5 REAL,vol_ma_20 REAL,year_high REAL,year_low REAL,year_high_2y REAL,year_low_2y REAL); INSERT INTO stocks(stock_id,vol_ma_20) VALUES ("X",999); CREATE TABLE daily_prices(stock_id TEXT,date TEXT,close REAL,volume REAL);')
        c.executemany('INSERT INTO daily_prices VALUES (?,?,?,?)',[('X',f'2020-01-{i:02d}',i,i*10) for i in range(1,21)])
        refresh_price_summary(c,'X')
        self.assertEqual(c.execute('SELECT vol_ma_20,year_high,year_low FROM stocks').fetchone(),(105.,20.,1.))

    def test_overlap_boundary_uses_previous_close(self):
        s = pd.Series([100., 110., 99.], index=pd.date_range('2026-01-01', periods=3))
        result = close_returns(s).iloc[1:]
        self.assertAlmostEqual(result.iloc[0], 10.)
        self.assertAlmostEqual(result.iloc[1], -10.)

    def test_missing_and_zero_previous_are_unknown(self):
        r = close_returns(pd.Series([0., 10., 10.]))
        self.assertTrue(pd.isna(r.iloc[0]))
        self.assertTrue(pd.isna(r.iloc[1]))
        self.assertEqual(r.iloc[2], 0)

    def test_gap_uses_previous_valid_close(self):
        r = close_returns(pd.Series([100., None, 110.]))
        self.assertTrue(pd.isna(r.iloc[1]))
        self.assertAlmostEqual(r.iloc[2],10.)

    def test_weekly_not_daily_and_no_lookahead(self):
        s = pd.Series(range(1, 111), index=pd.bdate_range('2026-01-05', periods=110), dtype=float)
        r = weekly_averages(s)
        self.assertAlmostEqual(r.iloc[20, 0], (5+10+15+20+21)/5)
        self.assertTrue(pd.isna(r.iloc[19, 0]))
        self.assertAlmostEqual(r.iloc[95, 1], (sum(range(5, 96, 5))+96)/20)
        for length in (21, 28, 96, 103):
            pd.testing.assert_frame_equal(r.iloc[:length], weekly_averages(s.iloc[:length]))

    def test_holiday_week_uses_last_observed_close(self):
        s = pd.Series([1.,2.,3.,4.,5.], index=pd.to_datetime(['2026-01-02','2026-01-08','2026-01-16','2026-01-23','2026-02-02']))
        self.assertEqual(weekly_averages(s).iloc[-1, 0], 3)

    def revenue_db(self):
        c = sqlite3.connect(':memory:')
        c.executescript('CREATE TABLE stocks(stock_id TEXT, revenue_growth REAL); INSERT INTO stocks VALUES ("X",999); CREATE TABLE monthly_revenue(stock_id TEXT, year INTEGER, month INTEGER, revenue REAL);')
        c.executemany('INSERT INTO monthly_revenue VALUES (?,?,?,?)', [('X',y,m,100 if y==2025 else 200) for y in (2025,2026) for m in range(1,13)])
        return c

    def test_revenue_separate_amount_and_percent(self):
        c=self.revenue_db(); sync_revenue(c)
        self.assertEqual(c.execute('SELECT revenue_growth,revenue_monthly_yoy,revenue_ttm,revenue_period FROM stocks').fetchone(), (100.,100.,2400.,'2026-12'))
        sync_revenue(c)
        self.assertEqual(c.execute('SELECT revenue_ttm FROM stocks').fetchone()[0],2400)

    def test_revenue_gap_is_not_zero(self):
        c=self.revenue_db(); c.execute('DELETE FROM monthly_revenue WHERE year=2026 AND month=5'); sync_revenue(c)
        self.assertEqual(c.execute('SELECT revenue_growth,revenue_ttm,revenue_monthly_yoy FROM stocks').fetchone(),(None,None,100.))

    def test_bias_and_zero_volume(self):
        from fetch_precompute import precompute_bias, precompute_vol_spike
        c=sqlite3.connect(':memory:')
        c.executescript('CREATE TABLE stocks(stock_id TEXT,vol_ma_20 REAL); INSERT INTO stocks VALUES ("X",100); CREATE TABLE daily_prices(stock_id TEXT,date TEXT,close REAL,ma_20 REAL,ma_60 REAL,volume REAL); INSERT INTO daily_prices VALUES ("X","2026-01-02",110,100,100,0);')
        self.assertAlmostEqual(precompute_bias('X',c)[0],.1)
        self.assertEqual(precompute_vol_spike('X',c),0)

if __name__ == '__main__':
    unittest.main()
