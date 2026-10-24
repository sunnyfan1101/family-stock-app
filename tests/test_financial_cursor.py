"""Disposable crash/restart tests for the quarterly source batch cursor."""

import json
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from financial_cursor import (advance_nochange, finalize, load, prepare,
                              reconcile, reserve_requests, save)
import financials_only


class FinancialCursorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.state = self.root / "state.json"
        self.batch = self.root / "progress.json"
        self.candidate = self.root / "candidate.xz"
        self.live = self.root / "live.xz"
        self.batch.write_text(json.dumps({"offset": 0, "next_offset": 150,
                                          "checked": 150, "total": 1995}))
        self.candidate.write_bytes(b"validated-archive")

    def test_nochange_advances_without_push(self):
        self.assertEqual(advance_nochange(self.state, self.batch), 150)
        self.assertEqual(load(self.state)["next_offset"], 150)
        with self.assertRaisesRegex(RuntimeError, "stale"):
            advance_nochange(self.state, self.batch)

    def test_failed_push_retries_exact_live_archive_then_advances(self):
        prepare(self.state, self.batch, self.candidate)
        self.live.write_bytes(self.candidate.read_bytes())
        def failed_push():
            raise ConnectionError("remote unavailable")
        with self.assertRaises(ConnectionError):
            reconcile(self.state, self.live, failed_push)
        self.assertEqual(load(self.state)["next_offset"], 0)
        calls = []
        self.assertTrue(reconcile(self.state, self.live, lambda: calls.append("push")))
        self.assertEqual(calls, ["push"])
        self.assertEqual(load(self.state)["next_offset"], 150)
        self.assertIsNone(load(self.state)["pending"])
        self.assertFalse(reconcile(self.state, self.live, lambda: calls.append("wrong")))

    def test_unpromoted_candidate_cannot_advance_or_push_old_db(self):
        prepare(self.state, self.batch, self.candidate)
        self.live.write_bytes(b"previous-release")
        with self.assertRaisesRegex(RuntimeError, "manual review"):
            reconcile(self.state, self.live, lambda: self.fail("must not push"))
        with self.assertRaisesRegex(RuntimeError, "differs"):
            finalize(self.state, self.live)
        self.assertEqual(load(self.state)["next_offset"], 0)

    def test_corrupt_state_is_not_reset_silently(self):
        save(self.state, {"version": 1, "next_offset": -1, "pending": None})
        with self.assertRaises(ValueError):
            load(self.state)

    def test_universe_shrink_wraps_cursor_without_skipping_current_stock(self):
        db_path = self.root / "quotes.db"
        with sqlite3.connect(db_path) as db:
            db.execute("CREATE TABLE stocks(stock_id TEXT PRIMARY KEY)")
            db.execute("INSERT INTO stocks VALUES ('2330')")
            db.execute("CREATE TABLE financial_reports(stock_id TEXT,year INTEGER,quarter INTEGER)")
        visited = []
        def fetch(stock_id, _start):
            visited.append(stock_id)
            return {}
        with patch.object(financials_only.database, "get_connection",
                          side_effect=lambda: sqlite3.connect(db_path)):
            self.assertFalse(financials_only.update_financials_only(
                limit=150, offset=5000, fetcher=fetch, progress_path=self.batch))
        self.assertEqual(visited, ["2330"])
        self.assertEqual(json.loads(self.batch.read_text()),
                         {"offset": 5000, "next_offset": 0, "checked": 1, "total": 1})

    def test_daily_budget_is_reserved_before_network_and_resets_next_day(self):
        budget = self.root / "budget.json"
        self.assertEqual(reserve_requests(budget, 100, day="2026-10-24"), 100)
        with self.assertRaisesRegex(RuntimeError, "cap reached"):
            reserve_requests(budget, 51, day="2026-10-24")
        self.assertEqual(reserve_requests(budget, 50, day="2026-10-24"), 150)
        self.assertEqual(reserve_requests(budget, 10, day="2026-10-25"), 10)
        with self.assertRaisesRegex(ValueError, "exceeds"):
            reserve_requests(budget, 151, day="2026-10-25")

    def test_budget_gate_blocks_fetch_before_first_request(self):
        db_path = self.root / "quotes.db"
        budget = self.root / "budget.json"
        with sqlite3.connect(db_path) as db:
            db.execute("CREATE TABLE stocks(stock_id TEXT PRIMARY KEY)")
            db.execute("INSERT INTO stocks VALUES ('2330')")
        today = datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
        save(budget, {"day": today, "used": 150})
        with patch.object(financials_only.database, "get_connection",
                          side_effect=lambda: sqlite3.connect(db_path)):
            with self.assertRaisesRegex(RuntimeError, "cap reached"):
                financials_only.update_financials_only(
                    fetcher=lambda *_: self.fail("network must not be called"),
                    budget_path=budget)


if __name__ == "__main__":
    unittest.main()
