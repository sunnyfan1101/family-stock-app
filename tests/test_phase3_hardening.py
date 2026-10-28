"""Offline-only quarterly and market-session regression tests."""

import json
import lzma
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import financials_only
import requests
from market_session import session_status
from schedule_policy import monthly_revenue_due


class MarketSessionTests(unittest.TestCase):
    def test_weekend_and_unknown_weekday(self):
        self.assertEqual(session_status("2026-10-24"), "closed_weekend")
        self.assertEqual(session_status("2026-10-23"), "unknown_weekday")

    def test_documented_closure_requires_both_official_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "closures.json"
            entry = {"date": "2026-10-23", "twse": "https://www.twse.com.tw/a",
                     "tpex": "https://www.tpex.org.tw/b"}
            path.write_text(json.dumps({"closures": [entry]}))
            self.assertEqual(session_status("2026-10-23", path), "closed_documented")
            entry["tpex"] = "https://example.com/b"
            path.write_text(json.dumps({"closures": [entry]}))
            with self.assertRaisesRegex(ValueError, "official tpex"):
                session_status("2026-10-23", path)

    def test_revenue_window_is_independent_of_market_session(self):
        self.assertTrue(monthly_revenue_due("2026-10-10"))  # Saturday
        self.assertTrue(monthly_revenue_due("2026-10-12"))  # weekday
        self.assertFalse(monthly_revenue_due("2026-10-16"))


class FinancialsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name) / "candidate.db"
        with sqlite3.connect(self.db) as connection:
            connection.execute("CREATE TABLE stocks(stock_id TEXT PRIMARY KEY,revenue_growth REAL,"
                               "eps REAL,eps_growth REAL,pe_ratio REAL,gross_margin REAL,"
                               "operating_margin REAL,pretax_margin REAL,net_margin REAL)")
            connection.execute("CREATE TABLE daily_prices(stock_id TEXT,date TEXT,close REAL)")
            connection.execute("INSERT INTO stocks(stock_id,revenue_growth) VALUES ('2330',12)")
            connection.execute("INSERT INTO daily_prices VALUES ('2330','2026-10-22',100)")
            connection.execute("CREATE TABLE financial_reports(stock_id TEXT,year INTEGER,quarter INTEGER,"
                               "revenue REAL,gross_profit REAL,operating_income REAL,pretax_income REAL,"
                               "net_income REAL,eps REAL,gross_margin REAL,operating_margin REAL,"
                               "pretax_margin REAL,net_margin REAL,updated_at TEXT,"
                               "PRIMARY KEY(stock_id,year,quarter))")

    def test_parse_finmind_and_source_failure(self):
        class Response:
            def raise_for_status(self):
                pass

            def json(self):
                return {"status": 200, "data": [
                    {"date": "2026-06-30", "type": "Revenue", "value": 100000},
                    {"date": "2026-06-30", "type": "GrossProfit", "value": 40000},
                    {"date": "2026-06-30", "type": "EPS", "value": -1.5},
                ]}

        class Session:
            def get(self, *args, **kwargs):
                return Response()

        rows = financials_only.fetch_reports("2330", session=Session())
        self.assertEqual(rows[(2026, 2)]["revenue"], 100.0)
        self.assertEqual(rows[(2026, 2)]["gross_profit"], 40.0)
        self.assertEqual(rows[(2026, 2)]["gross_margin"], 40.0)
        self.assertEqual(rows[(2026, 2)]["eps"], -1.5)
        with self.assertRaises(ValueError):
            financials_only.report_period("2026-07-01")

        class FailedSession:
            def get(self, *args, **kwargs):
                raise requests.HTTPError("request URL contained a sensitive query")

        with self.assertRaises(RuntimeError) as caught:
            financials_only.fetch_reports("2330", session=FailedSession())
        self.assertNotIn("sensitive query", str(caught.exception))

    def test_change_no_change_and_rollback_leave_prices_alone(self):
        reports = {(2026, 2): {"revenue": 100.0, "eps": -1.5}}
        with patch.object(financials_only.database, "DB_PATH", self.db), \
             patch.object(financials_only.database, "DB_XZ_PATH", Path(str(self.db) + ".xz")), \
             patch.object(financials_only, "refresh_latest_stock_snapshot"), \
             patch.object(financials_only.database, "get_connection", side_effect=lambda: sqlite3.connect(self.db)):
            self.assertTrue(financials_only.update_financials_only(
                limit=1, fetcher=lambda *_: reports))
            self.assertTrue(Path(str(self.db) + ".xz").exists())
            Path(str(self.db) + ".xz").unlink()
            self.assertFalse(financials_only.update_financials_only(
                limit=1, fetcher=lambda *_: reports))
            self.assertFalse(Path(str(self.db) + ".xz").exists())
            def broken(*_):
                raise ConnectionError("source unavailable")
            with self.assertRaises(ConnectionError):
                financials_only.update_financials_only(limit=1, fetcher=broken)
        with sqlite3.connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT close FROM daily_prices").fetchone()[0], 100)
            self.assertEqual(connection.execute("SELECT revenue_growth FROM stocks").fetchone()[0], 12)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM financial_reports").fetchone()[0], 1)

    def test_quarterly_summary_preserves_monthly_growth_and_existing_eps_pe(self):
        with sqlite3.connect(self.db) as connection:
            connection.execute("UPDATE stocks SET eps=8,pe_ratio=12.5,eps_growth=7 WHERE stock_id='2330'")
            for quarter in range(1, 5):
                financials_only.store_reports(connection, "2330", {
                    (2026, quarter): {"eps": float(quarter), "gross_margin": 40.0}
                })
            financials_only.sync_quarterly_summary(connection, "2330")
            eps, pe, eps_growth, growth, margin = connection.execute(
                "SELECT eps,pe_ratio,eps_growth,revenue_growth,gross_margin FROM stocks WHERE stock_id='2330'"
            ).fetchone()
            self.assertEqual((eps, pe, eps_growth, growth, margin),
                             (8.0, 12.5, 7.0, 12.0, 40.0))

    def test_batch_limit(self):
        with self.assertRaises(ValueError):
            financials_only.update_financials_only(limit=251)

    def test_second_stock_source_error_aborts_whole_batch(self):
        with sqlite3.connect(self.db) as connection:
            connection.execute("INSERT INTO stocks(stock_id) VALUES ('2331')")
        calls = 0
        def fetcher(*_):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise ConnectionError("source failed halfway")
            return {(2026, 2): {"eps": 1.0}}
        with patch.object(financials_only.database, "DB_PATH", self.db), \
             patch.object(financials_only.database, "get_connection", side_effect=lambda: sqlite3.connect(self.db)):
            with self.assertRaises(ConnectionError):
                financials_only.update_financials_only(limit=2, fetcher=fetcher)
        with sqlite3.connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM financial_reports").fetchone()[0], 0)
        self.assertFalse(Path(str(self.db) + ".xz").exists())


if __name__ == "__main__":
    unittest.main()
