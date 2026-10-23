"""Only disposable SQLite files; no production database or network access."""

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from data_correctness import weekly_averages
from phase4_offline import SHARDS, assemble, refresh_weekly_changed, split, verify_source


def make_schema(connection):
    connection.execute("CREATE TABLE stocks(stock_id TEXT PRIMARY KEY, name TEXT)")
    connection.execute("CREATE TABLE daily_prices(stock_id TEXT,date TEXT,close REAL,"
                       "weekly_ma_5 REAL,weekly_ma_20 REAL,PRIMARY KEY(stock_id,date),"
                       "FOREIGN KEY(stock_id) REFERENCES stocks(stock_id))")
    connection.execute("CREATE TABLE monthly_revenue(stock_id TEXT,year INTEGER,month INTEGER,revenue REAL)")
    connection.execute("CREATE TABLE financial_reports(stock_id TEXT,year INTEGER,quarter INTEGER)")
    connection.execute("CREATE TABLE market_stats(date TEXT PRIMARY KEY)")
    connection.execute("CREATE TABLE latest_stock_snapshot(stock_id TEXT,name TEXT)")
    connection.execute("CREATE TABLE correlation_cache(target_id TEXT,stock_id TEXT,corr REAL)")
    connection.execute("CREATE TABLE user_presets(name TEXT PRIMARY KEY,settings TEXT)")
    connection.execute("CREATE TABLE watchlist(stock_id TEXT PRIMARY KEY,note TEXT)")
    connection.execute("CREATE INDEX idx_daily_close ON daily_prices(close)")


def full_weekly(connection):
    for stock_id, in connection.execute("SELECT stock_id FROM stocks").fetchall():
        data = pd.read_sql_query(
            "SELECT date,close FROM daily_prices WHERE stock_id=? AND close>0 ORDER BY date",
            connection, params=(stock_id,),
        )
        connection.execute(
            "UPDATE daily_prices SET weekly_ma_5=NULL,weekly_ma_20=NULL WHERE stock_id=?",
            (stock_id,),
        )
        if data.empty:
            continue
        close = pd.Series(data.close.to_numpy(), index=pd.to_datetime(data.date))
        values = weekly_averages(close)
        connection.executemany(
            "UPDATE daily_prices SET weekly_ma_5=?,weekly_ma_20=? "
            "WHERE stock_id=? AND date=?",
            [(None if pd.isna(a) else float(a), None if pd.isna(b) else float(b),
              stock_id, date.strftime("%Y-%m-%d"))
             for date, a, b in values.itertuples(name=None)],
        )
    connection.commit()


class OfflinePartitionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "source.db"
        with sqlite3.connect(self.source) as connection:
            make_schema(connection)
            connection.execute("INSERT INTO stocks VALUES ('A','Example')")
            connection.execute("INSERT INTO daily_prices VALUES ('A','2026-10-22',10,NULL,NULL)")
            connection.execute("INSERT INTO monthly_revenue VALUES ('A',2026,9,100)")
            connection.execute("INSERT INTO correlation_cache VALUES ('A','A',0.1)")
            connection.execute("INSERT INTO correlation_cache VALUES ('A','A',0.1)")
            connection.execute("INSERT INTO user_presets VALUES ('secret-local','{}')")
            connection.execute("INSERT INTO watchlist VALUES ('A','private')")

    def test_split_assemble_and_private_boundary(self):
        parts = self.root / "parts"
        manifest = split(self.source, parts)
        self.assertEqual(set(manifest["shards"]), set(SHARDS))
        with sqlite3.connect(parts / "market.db") as market:
            tables = {r[0] for r in market.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertFalse(tables & {"user_presets", "watchlist", "daily_prices"})
        with sqlite3.connect(parts / "local.db") as local:
            self.assertEqual(local.execute("SELECT note FROM watchlist").fetchone()[0], "private")
        assembled = self.root / "assembled.db"
        assemble(parts, assembled)
        counts = verify_source(self.source, assembled)
        self.assertEqual(counts["daily_prices"], 1)
        self.assertEqual(counts["correlation_cache"], 2)
        with sqlite3.connect(assembled) as connection:
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
            self.assertIn("idx_daily_close", {
                r[0] for r in connection.execute("SELECT name FROM sqlite_master WHERE type='index'")
            })

    def test_unknown_table_and_tampered_manifest_are_rejected(self):
        with sqlite3.connect(self.source) as connection:
            connection.execute("CREATE TABLE future_table(data TEXT)")
        with self.assertRaisesRegex(ValueError, "unknown or missing"):
            split(self.source, self.root / "bad")
        with sqlite3.connect(self.source) as connection:
            connection.execute("DROP TABLE future_table")
        parts = self.root / "parts"
        split(self.source, parts)
        manifest = json.loads((parts / "manifest.json").read_text())
        manifest["shards"]["market"]["sha256"] = "0" * 64
        (parts / "manifest.json").write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, "hash mismatch"):
            assemble(parts, self.root / "bad-assembly.db")


class IncrementalWeeklyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "history.db"
        with sqlite3.connect(self.source) as connection:
            make_schema(connection)
            connection.executemany("INSERT INTO stocks VALUES (?,?)", [("A", "A"), ("B", "B")])
            dates = pd.bdate_range("2026-04-01", periods=138)
            for stock_id, offset in (("A", 0), ("B", 100)):
                connection.executemany(
                    "INSERT INTO daily_prices(stock_id,date,close) VALUES (?,?,?)",
                    [(stock_id, day.strftime("%Y-%m-%d"), float(i + 10 + offset))
                     for i, day in enumerate(dates)],
                )
            full_weekly(connection)
            self.first, self.last = dates[0], dates[-1]

    def _copies(self):
        baseline, incremental = self.root / "baseline.db", self.root / "incremental.db"
        with sqlite3.connect(self.source) as source:
            with sqlite3.connect(baseline) as target:
                source.backup(target)
            with sqlite3.connect(incremental) as target:
                source.backup(target)
        return baseline, incremental

    def _compare(self, baseline, incremental):
        with sqlite3.connect(incremental) as connection:
            connection.execute("ATTACH DATABASE ? AS baseline", (str(baseline),))
            mismatch = connection.execute(
                "SELECT COUNT(*) FROM daily_prices i JOIN baseline.daily_prices b "
                "USING(stock_id,date) WHERE i.weekly_ma_5 IS NOT b.weekly_ma_5 "
                "OR i.weekly_ma_20 IS NOT b.weekly_ma_20"
            ).fetchone()[0]
            self.assertEqual(mismatch, 0)

    def test_new_week_and_historical_correction_match_full_rebuild(self):
        baseline, incremental = self._copies()
        future = (self.last + pd.offsets.BDay(1)).strftime("%Y-%m-%d")
        corrected = (self.last - pd.offsets.BDay(12)).strftime("%Y-%m-%d")
        for path in (baseline, incremental):
            with sqlite3.connect(path) as connection:
                connection.execute("INSERT INTO daily_prices(stock_id,date,close) VALUES ('A',?,250)",
                                   (future,))
                connection.execute("UPDATE daily_prices SET close=999 WHERE stock_id='B' AND date=?",
                                   (corrected,))
        with sqlite3.connect(baseline) as connection:
            full_weekly(connection)
        with sqlite3.connect(incremental) as connection:
            written = refresh_weekly_changed(connection, {"A": future, "B": corrected})
        self.assertLess(written, 50)
        self._compare(baseline, incremental)

    def test_invalidated_price_is_cleared_and_future_matches_full(self):
        baseline, incremental = self._copies()
        changed = (self.last - pd.offsets.BDay(10)).strftime("%Y-%m-%d")
        for path in (baseline, incremental):
            with sqlite3.connect(path) as connection:
                connection.execute("UPDATE daily_prices SET close=NULL WHERE stock_id='A' AND date=?",
                                   (changed,))
        with sqlite3.connect(baseline) as connection:
            full_weekly(connection)
        with sqlite3.connect(incremental) as connection:
            refresh_weekly_changed(connection, {"A": changed})
            self.assertEqual(connection.execute(
                "SELECT weekly_ma_5 FROM daily_prices WHERE stock_id='A' AND date=?",
                (changed,),
            ).fetchone()[0], None)
        self._compare(baseline, incremental)


if __name__ == "__main__":
    unittest.main()
