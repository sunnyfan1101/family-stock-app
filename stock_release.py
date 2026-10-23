"""Validate and publish a staged stock DB. Caller holds the DB update flock."""

import argparse
import hashlib
import lzma
import os
from pathlib import Path
import shutil
import sqlite3
import sys
from datetime import date


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def validate(candidate, previous=None, min_latest=2000, require_today=False):
    candidate = Path(candidate)
    packed = Path(str(candidate) + ".xz")
    if not candidate.is_file() or not packed.is_file():
        raise ValueError("候選 DB 或 xz 不存在")
    if packed.stat().st_size >= 100 * 1024 * 1024:
        raise ValueError("候選 xz 達 GitHub 100 MiB 單檔限制，禁止本機先發布")
    with sqlite3.connect(f"file:{candidate}?mode=ro", uri=True) as db:
        if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("候選 SQLite integrity_check 失敗")
        latest = db.execute("SELECT MAX(date) FROM daily_prices").fetchone()[0]
        if require_today and latest != date.today().isoformat():
            raise ValueError(f"最新行情不是今天: {latest}")
        latest_count = db.execute(
            "SELECT COUNT(*) FROM daily_prices WHERE date=? AND close>0", (latest,)
        ).fetchone()[0]
        row_count = db.execute("SELECT COUNT(*) FROM daily_prices").fetchone()[0]
        if latest_count < min_latest:
            raise ValueError(f"最新日有效收盤數過少: {latest_count}")
        if previous and Path(previous).is_file():
            with sqlite3.connect(f"file:{previous}?mode=ro", uri=True) as old:
                old_latest = old.execute("SELECT MAX(date) FROM daily_prices").fetchone()[0]
                old_count = old.execute(
                    "SELECT COUNT(*) FROM daily_prices WHERE date=? AND close>0", (old_latest,)
                ).fetchone()[0]
                old_rows = old.execute("SELECT COUNT(*) FROM daily_prices").fetchone()[0]
            if latest < old_latest or latest_count < old_count * 0.95:
                raise ValueError(f"候選行情退步: {latest}/{latest_count} < {old_latest}/{old_count}")
            if row_count < old_rows * 0.99:
                raise ValueError(f"歷史行情大量減少: {row_count} < {old_rows}")
        mismatch = db.execute(
            "SELECT COUNT(*) FROM latest_stock_snapshot "
            "WHERE ma_20>0 AND bias_20 IS NOT NULL "
            "AND ABS(bias_20-(close/ma_20-1))>0.000001"
        ).fetchone()[0]
        if mismatch:
            raise ValueError(f"乖離率不一致: {mismatch}")
        volume_mismatch = db.execute(
            "SELECT COUNT(*) FROM latest_stock_snapshot "
            "WHERE vol_ma_20>0 AND vol_spike IS NOT NULL "
            "AND ABS(vol_spike-volume/vol_ma_20)>0.000001"
        ).fetchone()[0]
        if volume_mismatch:
            raise ValueError(f"量比不一致: {volume_mismatch}")
        returns_mismatch = db.execute(
            "WITH priced AS (SELECT close,change_pct,"
            "LAG(close) OVER(PARTITION BY stock_id ORDER BY date) AS prior "
            "FROM daily_prices WHERE close>0) "
            "SELECT COUNT(*) FROM priced WHERE prior>0 AND "
            "(change_pct IS NULL OR ABS(change_pct-(close/prior-1)*100)>0.000001)"
        ).fetchone()[0]
        if returns_mismatch:
            raise ValueError(f"歷史漲跌幅不一致: {returns_mismatch}")
    # Read the *entire* xz, and compare the decompressed content, not just its header.
    h = hashlib.sha256()
    with lzma.open(packed, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    if h.hexdigest() != digest(candidate):
        raise ValueError("xz 解壓內容與候選 DB 不同")
    print(f"VALIDATED latest={latest} valid_closes={latest_count} rows={row_count} sha256={h.hexdigest()}", flush=True)


def promote(candidate, live, require_today=False):
    candidate = Path(candidate)
    live = Path(live)
    validate(candidate, live, require_today=require_today)
    packed = Path(str(candidate) + ".xz")
    live_packed = Path(str(live) + ".xz")
    if candidate.stat().st_dev != live.parent.stat().st_dev:
        raise ValueError("候選 DB 必須與正式 DB 位於同一檔案系統")
    previous = candidate.parent / "previous.db"
    previous_packed = candidate.parent / "previous.db.xz"
    if previous.exists() or previous_packed.exists():
        raise ValueError("備份名稱已被占用，停止發布")
    with sqlite3.connect(f"file:{live}?mode=ro", uri=True) as old, sqlite3.connect(previous) as backup:
        old.backup(backup)
    # A SQLite online backup can be byte-different from the source file.
    # Compress the backup itself so rollback always restores a matching pair.
    with previous.open("rb") as source, lzma.open(previous_packed, "wb", preset=1) as target:
        shutil.copyfileobj(source, target, length=1024 * 1024)
    try:
        os.replace(candidate, live)
        os.replace(packed, live_packed)
    except Exception:
        os.replace(previous, live)
        if previous_packed.exists():
            os.replace(previous_packed, live_packed)
        raise
    print(f"PROMOTED backup={previous} backup_xz={previous_packed}", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("validate", "promote"))
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--live", type=Path)
    parser.add_argument("--require-today", action="store_true")
    args = parser.parse_args()
    if args.command == "promote":
        if not args.live:
            parser.error("promote requires --live")
        promote(args.candidate, args.live, require_today=args.require_today)
    else:
        validate(args.candidate, args.live, require_today=args.require_today)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"RELEASE_REJECTED: {exc}", file=sys.stderr)
        raise SystemExit(1)
