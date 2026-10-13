"""Run with: venv/bin/python -m unittest discover -s tests -v

Pure functions use fixtures. AppTest reads the existing local market database;
it never invokes reports, AI services, updaters, or preset writes.
"""
import unittest
from pathlib import Path

import pandas as pd
from streamlit.testing.v1 import AppTest

from web_ui import filter_stocks, number


class DisplayTests(unittest.TestCase):
    def test_missing_zero_and_sign(self):
        self.assertEqual(number(None), '未取得')
        self.assertEqual(number(float('nan')), '未取得')
        self.assertEqual(number(0), '0.00')
        self.assertEqual(number(-2.1, '%', True), '-2.10%')
        self.assertEqual(number(999), '999.00')

    def test_filter_keeps_raw_values_and_excludes_missing(self):
        df = pd.DataFrame([
            dict(stock_id='1', industry='科技', date='2026-10-12', close=100, revenue_growth=999),
            dict(stock_id='2', industry='科技', date='2026-10-08', close=20, revenue_growth=30),
            dict(stock_id='3', industry='金融', date='2026-10-12', close=10, revenue_growth=None),
        ])
        result = filter_stocks(df, dict(preset='營收成長', latest_only=True))
        self.assertEqual(result.stock_id.tolist(), ['1'])
        self.assertEqual(result.iloc[0].revenue_growth, 999)
        self.assertTrue(filter_stocks(df, dict(preset='營收成長', latest_only=True, max_price=1)).empty)


class NavigationTests(unittest.TestCase):
    def setUp(self):
        self.at = AppTest.from_file(str(Path(__file__).resolve().parents[1] / 'app.py'), default_timeout=45).run()
        self.assert_clean()

    def assert_clean(self):
        self.assertEqual([e.message for e in self.at.exception], [])

    def test_search_detail_and_recent(self):
        self.at.selectbox(key='web_search').select('2330').run()
        self.at.button[0].click().run()
        self.assert_clean()
        self.assertIn('2330', self.at.title[0].value)
        self.at.radio(key='web_span').set_value('近 3 年').run()
        self.assert_clean()
        self.at.radio(key='web_page').set_value('首頁').run()
        self.assertEqual(self.at.button(key='recent_2330').label, '2330 台積電')

    def test_filters_reset_cards_and_open(self):
        self.at.radio(key='web_page').set_value('探索選股').run()
        self.at.selectbox(key='web_preset').select('營收成長')
        self.at.number_input(key='web_max_price').set_value(0.01)
        self.at.button(key='FormSubmitter:web_filters-套用篩選').click().run()
        self.assert_clean()
        self.assertTrue(any('符合 0 檔' == s.value for s in self.at.subheader))
        self.at.button(key='FormSubmitter:web_filters-清除篩選').click().run()
        self.assert_clean()
        self.assertEqual(self.at.selectbox(key='web_preset').value, '不限條件')
        self.at.toggle(key='web_compact').set_value(True).run()
        self.assert_clean()
        card = next(b for b in self.at.button if b.key and b.key.startswith('card_'))
        stock_id = card.key.removeprefix('card_')
        card.click().run()
        self.assert_clean()
        self.assertIn(stock_id, self.at.title[0].value)

    def test_legacy_entry_and_return(self):
        self.at.toggle(key='web_legacy').set_value(True).run()
        self.assert_clean()
        self.at.toggle(key='web_legacy').set_value(False).run()
        self.assert_clean()
        self.assertEqual(self.at.title[0].value, '投資研究工作台')


if __name__ == '__main__':
    unittest.main()
