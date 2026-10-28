import unittest
import json
import lzma
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import fetch_data
import fetch_revenue
import revenue_only
import verify_copy


class EarlyDailyGateTests(unittest.TestCase):
    def test_closed_day_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "僅抓到 0 檔"):
            fetch_data.require_daily_coverage(set(), 2350)

    def test_partial_feed_rejected_not_called_holiday(self):
        with self.assertRaisesRegex(RuntimeError, "至少需 1997 檔"):
            fetch_data.require_daily_coverage(set(range(1996)), 2350)

    def test_sufficient_feed_allowed(self):
        fetch_data.require_daily_coverage(set(range(1997)), 2350)

    def test_force_financials_does_not_require_todays_quotes(self):
        fetch_data.require_daily_coverage(set(), 2350, force_financials=True)


class RevenueOnlyTests(unittest.TestCase):
    def test_bounded_monthly_cursor_skips_current_and_resumes(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "revenue.db"
            progress = Path(directory) / "progress.json"
            with sqlite3.connect(db_path) as conn:
                conn.execute("CREATE TABLE stocks(stock_id TEXT PRIMARY KEY)")
                conn.execute("CREATE TABLE monthly_revenue(stock_id TEXT,year INTEGER,month INTEGER)")
                conn.executemany("INSERT INTO stocks VALUES (?)",
                                 [("1001",), ("1002",), ("1003",), ("1004",)])
                conn.executemany("INSERT INTO monthly_revenue VALUES (?,2026,9)",
                                 [("1001",), ("1004",)])
            visited = []
            def visit(stock_id, *_args, **_kwargs):
                visited.append(stock_id)
                return 0
            with patch.object(fetch_revenue, "get_connection",
                              side_effect=lambda: sqlite3.connect(db_path)), \
                 patch.object(fetch_revenue, "get_expected_latest_month", return_value=(2026, 9)), \
                 patch.object(fetch_revenue, "update_monthly_revenue_for_stock", side_effect=visit), \
                 patch.object(fetch_revenue.time, "sleep"):
                self.assertEqual(fetch_revenue.update_all_stocks(
                    strict_source=True, max_requests=1, progress_path=progress), 0)
                first = json.loads(progress.read_text())
                self.assertEqual(first, {"offset": 0, "next_offset": 2,
                                         "checked": 1, "total": 4})
                self.assertEqual(fetch_revenue.update_all_stocks(
                    strict_source=True, max_requests=1, offset=first["next_offset"],
                    progress_path=progress), 0)
                self.assertEqual(json.loads(progress.read_text()),
                                 {"offset": 2, "next_offset": 1, "checked": 1, "total": 4})
            self.assertEqual(visited, ["1002", "1003"])

    def test_bounded_monthly_failure_does_not_advance_cursor(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "revenue.db"
            progress = Path(directory) / "progress.json"
            with sqlite3.connect(db_path) as conn:
                conn.execute("CREATE TABLE stocks(stock_id TEXT PRIMARY KEY)")
                conn.execute("CREATE TABLE monthly_revenue(stock_id TEXT,year INTEGER,month INTEGER)")
                conn.execute("INSERT INTO stocks VALUES ('2330')")
            with patch.object(fetch_revenue, "get_connection",
                              side_effect=lambda: sqlite3.connect(db_path)), \
                 patch.object(fetch_revenue, "get_expected_latest_month", return_value=(2026, 9)), \
                 patch.object(fetch_revenue, "update_monthly_revenue_for_stock",
                              side_effect=fetch_revenue.RevenueSourceError("source unavailable")):
                with self.assertRaises(fetch_revenue.RevenueSourceError):
                    fetch_revenue.update_all_stocks(strict_source=True,
                                                   max_requests=1, progress_path=progress)
            self.assertFalse(progress.exists())

    def test_registered_monthly_request_uses_header_not_query(self):
        class Response:
            status_code = 200

            def json(self):
                return {"status": 200, "data": []}

        with patch.object(fetch_revenue, "FINMIND_TOKEN", "synthetic-test-value"), \
             patch.object(fetch_revenue.requests, "get", return_value=Response()) as request:
            self.assertTrue(fetch_revenue.fetch_stock_revenue("2330", strict_source=True).empty)
        self.assertNotIn("token", request.call_args.kwargs["params"])
        self.assertIn("Authorization", request.call_args.kwargs["headers"])

    def test_strict_source_rejects_limit_and_http_failure(self):
        class Response:
            def __init__(self, status, body=None):
                self.status_code = status
                self.body = body

            def json(self):
                return self.body

        for status in (402, 403, 500):
            with self.subTest(status=status), \
                 patch.object(fetch_revenue.requests, "get", return_value=Response(status)):
                with self.assertRaises(fetch_revenue.RevenueSourceError):
                    fetch_revenue.fetch_stock_revenue("2330", strict_source=True)
        with patch.object(fetch_revenue.requests, "get",
                          side_effect=ConnectionError("sensitive request URL")):
            with self.assertRaises(fetch_revenue.RevenueSourceError) as caught:
                fetch_revenue.fetch_stock_revenue("2330", strict_source=True)
            self.assertNotIn("sensitive request URL", str(caught.exception))
        for body in ({"status": 402, "data": []},
                     {"status": 200, "data": "not-a-list"}):
            with self.subTest(body=body), \
                 patch.object(fetch_revenue.requests, "get", return_value=Response(200, body)):
                with self.assertRaises(fetch_revenue.RevenueSourceError):
                    fetch_revenue.fetch_stock_revenue("2330", strict_source=True)

    def test_strict_monthly_batch_does_not_accept_partial_source_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "revenue.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute("CREATE TABLE stocks(stock_id TEXT PRIMARY KEY)")
                conn.execute("CREATE TABLE monthly_revenue(stock_id TEXT,year INTEGER,month INTEGER)")
                conn.executemany("INSERT INTO stocks VALUES (?)", [("2330",), ("2331",)])
            with patch.object(fetch_revenue, "get_connection",
                              side_effect=lambda: sqlite3.connect(db_path)), \
                 patch.object(fetch_revenue, "get_expected_latest_month", return_value=(2026, 9)), \
                 patch.object(fetch_revenue, "update_monthly_revenue_for_stock",
                              side_effect=[1, fetch_revenue.RevenueSourceError("limit")]), \
                 patch.object(fetch_revenue.time, "sleep"):
                with self.assertRaises(fetch_revenue.RevenueSourceError):
                    fetch_revenue.update_all_stocks(strict_source=True)

    def test_historical_finmind_replay_is_not_counted_as_new(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / 'revenue.db'
            with sqlite3.connect(db_path) as conn:
                conn.execute('''CREATE TABLE monthly_revenue (
                    stock_id TEXT, year INTEGER, month INTEGER, revenue REAL,
                    cumulative_revenue REAL, yoy_growth REAL, cumulative_yoy REAL,
                    last_year_revenue REAL, last_year_cumulative REAL,
                    updated_at TEXT, PRIMARY KEY (stock_id, year, month))''')
            row = dict(stock_id='2330', revenue_year=2026, revenue_month=9,
                       revenue=100.0, cumulative_revenue=900.0,
                       yoy_growth=None, cumulative_yoy=None,
                       last_year_revenue=None, last_year_cumulative=None)
            with patch.object(fetch_revenue, 'get_connection',
                              side_effect=lambda: sqlite3.connect(db_path)):
                self.assertEqual(fetch_revenue.save_to_database(pd.DataFrame([row])), 1)
                self.assertEqual(fetch_revenue.save_to_database(pd.DataFrame([row])), 0)
                row['revenue'] = 101.0
                self.assertEqual(fetch_revenue.save_to_database(pd.DataFrame([row])), 1)
            with sqlite3.connect(db_path) as conn:
                self.assertEqual(conn.execute('SELECT COUNT(*) FROM monthly_revenue').fetchone()[0], 1)
                self.assertEqual(conn.execute('SELECT revenue FROM monthly_revenue').fetchone()[0], 101.0)

    def test_stage_copy_only_preserves_source_and_skips_rebuild(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / 'live.db'
            target = Path(directory) / 'staged.db'
            with sqlite3.connect(source) as conn:
                conn.execute('CREATE TABLE marker (value INTEGER)')
                conn.execute('INSERT INTO marker VALUES (42)')
            with patch('sys.argv', ['verify_copy.py', '--source', str(source),
                                    '--output', str(target), '--copy-only']), \
                 patch.object(verify_copy, 'rebuild') as rebuild:
                verify_copy.main()
                rebuild.assert_not_called()
            with sqlite3.connect(source) as original, sqlite3.connect(target) as copy:
                self.assertEqual(original.execute('SELECT * FROM marker').fetchall(),
                                 copy.execute('SELECT * FROM marker').fetchall())

    @patch.object(revenue_only, "update_all_stocks", return_value=0)
    @patch.object(revenue_only, "sync_revenue")
    def test_no_change_never_rebuilds_or_packs(self, sync, update):
        self.assertFalse(revenue_only.update_revenue_only())
        sync.assert_not_called()
        update.assert_called_once()

    def test_changed_revenue_rebuilds_snapshot_and_packs_same_db(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "stock_data.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute("CREATE TABLE marker (value INTEGER)")
                conn.execute("INSERT INTO marker VALUES (42)")
            with patch.object(revenue_only, "update_all_stocks", return_value=1), \
                 patch.object(revenue_only.database, "DB_PATH", db_path), \
                 patch.object(revenue_only.database, "DB_XZ_PATH", Path(str(db_path) + ".xz")), \
                 patch.object(revenue_only.database, "get_connection", side_effect=lambda: sqlite3.connect(db_path)), \
                 patch.object(revenue_only, "sync_revenue") as sync, \
                 patch.object(revenue_only, "refresh_latest_stock_snapshot") as snapshot:
                self.assertTrue(revenue_only.update_revenue_only())
                sync.assert_called_once()
                snapshot.assert_called_once()
            with lzma.open(str(db_path) + ".xz", "rb") as packed:
                self.assertEqual(packed.read(), db_path.read_bytes())


if __name__ == "__main__":
    unittest.main()
