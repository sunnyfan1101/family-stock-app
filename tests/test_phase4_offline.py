"""Only disposable SQLite files; no production database or network access."""

import json
import io
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

import pandas as pd

from data_correctness import weekly_averages
from phase4_offline import SHARDS, assemble, refresh_weekly_changed, split, verify_source
from phase4_bundle import (build_public_bundle, build_compressed_public_bundle,
                           install_public_bundle, prepare_public_bundle,
                           validate_public_bundle)
from phase4_delivery import fetch_release_bundle, require_install_headroom
import database


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

    def test_public_bundle_install_keeps_private_data_local_and_pins_versions(self):
        parts = self.root / "parts"
        split(self.source, parts)
        bundle = self.root / "public"
        manifest = build_public_bundle(parts, bundle)
        self.assertEqual(set(manifest["shards"]), {"prices", "market"})
        self.assertNotIn("private", (bundle / "bundle.json").read_text())
        self.assertNotIn("local", (bundle / "bundle.json").read_text())
        self.assertFalse((bundle / "local.db").exists())
        self.assertNotIn(b"private", (bundle / "prices.db").read_bytes())
        self.assertNotIn(b"private", (bundle / "market.db").read_bytes())
        target = self.root / "installed.db"
        with sqlite3.connect(target) as db:
            db.execute("CREATE TABLE old_marker(value TEXT)")
            db.execute("INSERT INTO old_marker VALUES ('recoverable')")
        identity = install_public_bundle(bundle, parts / "local.db", target)
        self.assertEqual(identity, manifest["bundle_id"])
        self.assertTrue((self.root / "installed.db.previous").exists())
        with sqlite3.connect(target) as db:
            self.assertEqual(db.execute("SELECT note FROM watchlist").fetchone()[0], "private")
            self.assertEqual(db.execute("SELECT bundle_id FROM _bundle_release").fetchone()[0], identity)
        with sqlite3.connect(self.root / "installed.db.previous") as db:
            self.assertEqual(db.execute("SELECT value FROM old_marker").fetchone()[0], "recoverable")

    def test_public_bundle_rejects_corruption_and_rolls_back_failed_swap(self):
        parts = self.root / "parts"
        split(self.source, parts)
        bundle = self.root / "public"
        build_public_bundle(parts, bundle)
        target = self.root / "installed.db"
        with sqlite3.connect(target) as db:
            db.execute("CREATE TABLE old_marker(value TEXT)")
            db.execute("INSERT INTO old_marker VALUES ('old')")
        def fail():
            raise RuntimeError("injected interruption")
        with self.assertRaisesRegex(RuntimeError, "injected"):
            install_public_bundle(bundle, parts / "local.db", target, after_swap=fail)
        with sqlite3.connect(target) as db:
            self.assertEqual(db.execute("SELECT value FROM old_marker").fetchone()[0], "old")
        with (bundle / "prices.db").open("ab") as stream:
            stream.write(b"tampered")
        with self.assertRaisesRegex(ValueError, "hash mismatch"):
            validate_public_bundle(bundle)
        with sqlite3.connect(target) as db:
            self.assertEqual(db.execute("SELECT value FROM old_marker").fetchone()[0], "old")

    def test_public_bundle_refuses_active_wal_sidecar(self):
        parts = self.root / "parts"
        split(self.source, parts)
        bundle = self.root / "public"
        build_public_bundle(parts, bundle)
        target = self.root / "installed.db"
        with sqlite3.connect(target) as db:
            db.execute("CREATE TABLE marker(value INTEGER)")
        Path(str(target) + "-wal").touch()
        with self.assertRaisesRegex(RuntimeError, "WAL sidecars"):
            install_public_bundle(bundle, parts / "local.db", target)
        with sqlite3.connect(target) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM marker").fetchone()[0], 0)

    def test_compressed_bundle_opt_in_preserves_local_and_rolls_versions(self):
        parts = self.root / "parts"
        split(self.source, parts)
        bundle = self.root / "compressed"
        manifest = build_compressed_public_bundle(parts, bundle, {"prices": 0, "market": 0})
        self.assertEqual(validate_public_bundle(bundle)["bundle_id"], manifest["bundle_id"])
        self.assertFalse((bundle / "local.db.xz").exists())
        target = self.root / "app.db"
        with sqlite3.connect(target) as db:
            make_schema(db)
            db.execute("INSERT INTO user_presets VALUES ('private','{}')")
            db.execute("INSERT INTO watchlist VALUES ('A','private-note')")
        with patch.object(database, "DB_PATH", target), \
             patch.dict("os.environ", {"STOCK_BUNDLE_DIR": str(bundle),
                                    "STOCK_BUNDLE_ID": manifest["bundle_id"]}):
            database.ensure_database()
            database.ensure_database()
        with sqlite3.connect(target) as db:
            self.assertEqual(db.execute("SELECT note FROM watchlist").fetchone()[0], "private-note")
            self.assertEqual(db.execute("SELECT bundle_id FROM _bundle_release").fetchone()[0], manifest["bundle_id"])
        self.assertTrue((self.root / "app.db.previous").exists())

        # A second staged version must preserve local data and keep the old backup.
        with sqlite3.connect(parts / "market.db") as db:
            db.execute("UPDATE stocks SET name='New' WHERE stock_id='A'")
        split_manifest = json.loads((parts / "manifest.json").read_text())
        from phase4_offline import sha256
        market = parts / "market.db"
        split_manifest["shards"]["market"]["sha256"] = sha256(market)
        split_manifest["shards"]["market"]["bytes"] = market.stat().st_size
        (parts / "manifest.json").write_text(json.dumps(split_manifest))
        next_bundle = self.root / "next-bundle"
        next_manifest = build_compressed_public_bundle(parts, next_bundle, {"prices": 0, "market": 0})
        with patch.object(database, "DB_PATH", target), \
             patch.dict("os.environ", {"STOCK_BUNDLE_DIR": str(next_bundle),
                                    "STOCK_BUNDLE_ID": next_manifest["bundle_id"]}):
            database.ensure_database()
        with sqlite3.connect(target) as db:
            self.assertEqual(db.execute("SELECT name FROM stocks").fetchone()[0], "New")
            self.assertEqual(db.execute("SELECT note FROM watchlist").fetchone()[0], "private-note")
        self.assertGreaterEqual(len(list(self.root.glob("app.db.previous*"))), 2)

    def test_compressed_bundle_rejects_bad_release_and_corruption(self):
        parts = self.root / "parts"
        split(self.source, parts)
        bundle = self.root / "compressed"
        manifest = build_compressed_public_bundle(parts, bundle, {"prices": 0, "market": 0})
        target = self.root / "app.db"
        with patch.object(database, "DB_PATH", target), \
             patch.dict("os.environ", {"STOCK_BUNDLE_DIR": str(bundle),
                                    "STOCK_BUNDLE_ID": "0" * 64}):
            with self.assertRaisesRegex(ValueError, "trusted release ID"):
                database.ensure_database()
        self.assertFalse(target.exists())
        with (bundle / "prices.db.xz").open("ab") as archive:
            archive.write(b"corrupt")
        with patch.object(database, "DB_PATH", target), \
             patch.dict("os.environ", {"STOCK_BUNDLE_DIR": str(bundle),
                                    "STOCK_BUNDLE_ID": manifest["bundle_id"]}):
            with self.assertRaisesRegex(ValueError, "hash mismatch"):
                database.ensure_database()
        self.assertFalse(target.exists())

    def test_preparer_uses_consistent_snapshot_and_never_exports_private_table(self):
        public = self.root / "prepared"
        manifest = prepare_public_bundle(self.source, public,
                                         {"prices": 0, "market": 0})
        self.assertEqual(manifest["bundle_id"], validate_public_bundle(public)["bundle_id"])
        self.assertEqual({p.name for p in public.iterdir()},
                         {"prices.db.xz", "market.db.xz", "bundle.json"})
        with self.assertRaisesRegex(ValueError, "output must not exist"):
            prepare_public_bundle(self.source, public,
                                  {"prices": 0, "market": 0})

    def test_trusted_release_download_and_app_install(self):
        parts = self.root / "parts"
        split(self.source, parts)
        release = self.root / "release"
        manifest = build_compressed_public_bundle(parts, release, {"prices": 0, "market": 0})
        urls = []

        def opener(url, timeout):
            urls.append(url)
            return io.BytesIO((release / url.rsplit("/", 1)[-1]).read_bytes())

        cache = self.root / "cache"
        downloaded = fetch_release_bundle("phase4-canary", manifest["bundle_id"], cache, opener)
        self.assertEqual(validate_public_bundle(downloaded)["bundle_id"], manifest["bundle_id"])
        self.assertEqual(len(urls), 3)
        self.assertTrue(all("/releases/download/phase4-canary/" in url for url in urls))
        self.assertEqual(fetch_release_bundle("phase4-canary", manifest["bundle_id"], cache, opener), downloaded)
        self.assertEqual(len(urls), 3)  # Verified cache needs no network.

        target = self.root / "app.db"
        with sqlite3.connect(target) as db:
            make_schema(db)
            db.execute("INSERT INTO watchlist VALUES ('A','private-note')")
        with patch.object(database, "DB_PATH", target), \
             patch("phase4_delivery.urlopen", opener), \
             patch.dict("os.environ", {"STOCK_BUNDLE_DIR": "", "STOCK_BUNDLE_RELEASE_TAG": "phase4-canary",
                                    "STOCK_BUNDLE_ID": manifest["bundle_id"]}):
            database.ensure_database()
        with sqlite3.connect(target) as db:
            self.assertEqual(db.execute("SELECT note FROM watchlist").fetchone()[0], "private-note")
            self.assertEqual(db.execute("SELECT bundle_id FROM _bundle_release").fetchone()[0], manifest["bundle_id"])

    def test_trusted_release_rejects_wrong_pin_corruption_and_low_disk(self):
        parts = self.root / "parts"
        split(self.source, parts)
        release = self.root / "release"
        manifest = build_compressed_public_bundle(parts, release, {"prices": 0, "market": 0})
        cache = self.root / "cache"

        def opener(url, timeout):
            return io.BytesIO((release / url.rsplit("/", 1)[-1]).read_bytes())

        with self.assertRaisesRegex(ValueError, "trusted ID"):
            fetch_release_bundle("phase4-canary", "0" * 64, cache, opener)
        self.assertFalse((cache / ("0" * 64)).exists())
        self.assertFalse(list(cache.glob("bundle-download-*")))
        with self.assertRaisesRegex(ValueError, "invalid release tag"):
            fetch_release_bundle("../bad", manifest["bundle_id"], cache, opener)

        def corrupt(url, timeout):
            data = (release / url.rsplit("/", 1)[-1]).read_bytes()
            if url.endswith("prices.db.xz"):
                data += b"broken"
            return io.BytesIO(data)

        with self.assertRaisesRegex(ValueError, "hash mismatch"):
            fetch_release_bundle("phase4-canary", manifest["bundle_id"], cache, corrupt)
        self.assertFalse((cache / manifest["bundle_id"]).exists())
        self.assertFalse(list(cache.glob("bundle-download-*")))
        def interrupted(url, timeout):
            if url.endswith("prices.db.xz"):
                raise OSError("injected network failure")
            return opener(url, timeout)
        with self.assertRaisesRegex(OSError, "injected network failure"):
            fetch_release_bundle("phase4-canary", manifest["bundle_id"], cache, interrupted)
        self.assertFalse(list(cache.glob("bundle-download-*")))
        with patch("phase4_delivery.shutil.disk_usage") as disk:
            disk.return_value.free = 0
            with self.assertRaisesRegex(OSError, "insufficient disk"):
                require_install_headroom(release, self.root / "app.db")
        with patch.object(database, "DB_PATH", self.root / "app.db"), \
             patch.dict("os.environ", {"STOCK_BUNDLE_DIR": str(release),
                                    "STOCK_BUNDLE_RELEASE_TAG": "phase4-canary",
                                    "STOCK_BUNDLE_ID": manifest["bundle_id"]}):
            with self.assertRaisesRegex(ValueError, "exactly one bundle source"):
                database.ensure_database()


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

    def test_multiday_backfill_cross_week_and_week_start_match_full(self):
        baseline, incremental = self._copies()
        monday = pd.Timestamp(self.last).to_period("W").start_time
        first_change = (monday - pd.Timedelta(days=18)).strftime("%Y-%m-%d")
        for path in (baseline, incremental):
            with sqlite3.connect(path) as db:
                db.execute("UPDATE daily_prices SET close=close+30 WHERE stock_id='A' AND date=?", (first_change,))
                db.execute("UPDATE daily_prices SET close=close+40 WHERE stock_id='A' AND date=?",
                           (monday.strftime("%Y-%m-%d"),))
                for i in range(1, 4):
                    day = (self.last + pd.offsets.BDay(i)).strftime("%Y-%m-%d")
                    db.execute("INSERT INTO daily_prices(stock_id,date,close) VALUES ('A',?,?)",
                               (day, 250.0 + i))
        with sqlite3.connect(baseline) as db:
            full_weekly(db)
        with sqlite3.connect(incremental) as db:
            refresh_weekly_changed(db, {"A": first_change})
        self._compare(baseline, incremental)

    def test_backfilled_missing_price_and_earlier_history_match_full(self):
        baseline, incremental = self._copies()
        changed = (self.first + pd.offsets.BDay(15)).strftime("%Y-%m-%d")
        for path in (baseline, incremental):
            with sqlite3.connect(path) as db:
                db.execute("UPDATE daily_prices SET close=NULL WHERE stock_id='B' AND date=?", (changed,))
                db.execute("INSERT INTO daily_prices(stock_id,date,close) VALUES ('A','2026-03-30',88)")
                db.execute("INSERT INTO daily_prices(stock_id,date,close) VALUES ('A','2026-03-31',89)")
        with sqlite3.connect(baseline) as db:
            full_weekly(db)
        with sqlite3.connect(incremental) as db:
            refresh_weekly_changed(db, {"A": "2026-03-30", "B": changed})
        self._compare(baseline, incremental)


if __name__ == "__main__":
    unittest.main()
