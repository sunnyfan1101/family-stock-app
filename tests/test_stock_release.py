import lzma
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import os

from stock_release import promote, validate


def make_db(path, date="2026-10-22", count=2000):
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE daily_prices(stock_id TEXT, date TEXT, close REAL, change_pct REAL)")
        db.execute("CREATE TABLE latest_stock_snapshot(close REAL, ma_20 REAL, bias_20 REAL, volume REAL, vol_ma_20 REAL, vol_spike REAL)")
        db.executemany("INSERT INTO daily_prices VALUES(?,?,?,?)",
                       [(str(i), date, 10.0, None) for i in range(count)])
        db.execute("INSERT INTO latest_stock_snapshot VALUES(10, 10, 0, 100, 100, 1)")


class StockReleaseTests(unittest.TestCase):
    def test_requires_today_when_requested(self):
        with tempfile.TemporaryDirectory() as root:
            candidate = Path(root) / "candidate.db"
            make_db(candidate, date="2020-01-01")
            Path(str(candidate) + ".xz").write_bytes(lzma.compress(candidate.read_bytes()))
            with self.assertRaisesRegex(ValueError, "不是今天"):
                validate(candidate, require_today=True)

    def test_rejects_stale_and_shrunken_candidate(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            live, candidate = root / "live.db", root / "candidate.db"
            make_db(live, date="2026-10-23", count=2100)
            make_db(candidate, date="2026-10-22", count=2000)
            Path(str(candidate) + ".xz").write_bytes(lzma.compress(candidate.read_bytes()))
            with self.assertRaisesRegex(ValueError, "退步"):
                validate(candidate, live)

    def test_rejects_latest_day_coverage_drop(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            live, candidate = root / "live.db", root / "candidate.db"
            make_db(live, count=2200)
            make_db(candidate, count=2000)
            Path(str(candidate) + ".xz").write_bytes(lzma.compress(candidate.read_bytes()))
            with self.assertRaisesRegex(ValueError, "退步"):
                validate(candidate, live)

    def test_rejects_historical_row_loss(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            live, candidate = root / "live.db", root / "candidate.db"
            make_db(live)
            with sqlite3.connect(live) as db:
                db.executemany("INSERT INTO daily_prices VALUES(?,?,?,?)",
                               [(str(i), "2026-10-21", 9.0, None) for i in range(2000)])
            make_db(candidate)
            Path(str(candidate) + ".xz").write_bytes(lzma.compress(candidate.read_bytes()))
            with self.assertRaisesRegex(ValueError, "歷史行情大量減少"):
                validate(candidate, live)

    def test_rejects_valid_but_different_xz(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            live, candidate = root / "live.db", root / "candidate.db"
            make_db(live)
            make_db(candidate, date="2026-10-23")
            Path(str(candidate) + ".xz").write_bytes(lzma.compress(live.read_bytes()))
            with self.assertRaisesRegex(ValueError, "不同"):
                validate(candidate, live)

    def test_failed_second_rename_restores_live_db(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            live, candidate = root / "live.db", root / "candidate.db"
            make_db(live)
            make_db(candidate, date="2026-10-23")
            Path(str(live) + ".xz").write_bytes(lzma.compress(live.read_bytes()))
            Path(str(candidate) + ".xz").write_bytes(lzma.compress(candidate.read_bytes()))
            original_replace = os.replace
            calls = 0

            def fail_second(source, target):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise OSError("simulated publish failure")
                return original_replace(source, target)

            with patch("stock_release.os.replace", side_effect=fail_second):
                with self.assertRaises(OSError):
                    promote(candidate, live)
            self.assertEqual(sqlite3.connect(live).execute("SELECT MAX(date) FROM daily_prices").fetchone()[0], "2026-10-22")
            validate(live, min_latest=2000)

    def test_rejects_corrupt_xz_without_touching_live(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            live, candidate = root / "live.db", root / "candidate.db"
            make_db(live)
            make_db(candidate, date="2026-10-23")
            Path(str(candidate) + ".xz").write_bytes(b"not-an-xz")
            with self.assertRaises(lzma.LZMAError):
                validate(candidate, live)
            self.assertEqual(sqlite3.connect(live).execute("SELECT MAX(date) FROM daily_prices").fetchone()[0], "2026-10-22")

    def test_validated_candidate_promotes_with_backup(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            live, candidate = root / "live.db", root / "candidate.db"
            make_db(live)
            make_db(candidate, date="2026-10-23")
            Path(str(live) + ".xz").write_bytes(lzma.compress(live.read_bytes()))
            Path(str(candidate) + ".xz").write_bytes(lzma.compress(candidate.read_bytes()))
            promote(candidate, live)
            self.assertEqual(sqlite3.connect(live).execute("SELECT MAX(date) FROM daily_prices").fetchone()[0], "2026-10-23")
            self.assertEqual(sqlite3.connect(root / "previous.db").execute("SELECT MAX(date) FROM daily_prices").fetchone()[0], "2026-10-22")
            validate(live, min_latest=2000)


if __name__ == "__main__":
    unittest.main()
