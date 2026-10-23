"""Offline-only partition and weekly-MA experiments.

This module is deliberately not imported by the production updater. It never
fetches data, changes cron, or publishes a shard. All output paths are new.
"""

import argparse
import hashlib
import json
import sqlite3
from pathlib import Path

import pandas as pd

from data_correctness import weekly_averages


# An explicit allowlist prevents a future table from silently joining a
# publishable shard. The local shard must never be uploaded as market data.
SHARDS = {
    "prices": ("daily_prices",),
    "market": (
        "stocks", "monthly_revenue", "financial_reports", "market_stats",
        "latest_stock_snapshot", "correlation_cache",
    ),
    "local": ("user_presets", "watchlist"),
}


def quoted(name):
    return '"' + name.replace('"', '""') + '"'


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def schema(connection):
    objects = connection.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE sql IS NOT NULL"
    ).fetchall()
    if any(kind not in ("table", "index") for kind, *_ in objects):
        raise ValueError("prototype cannot preserve views or triggers")
    tables = {name: sql for kind, name, _, sql in objects if kind == "table"}
    expected = {name for names in SHARDS.values() for name in names}
    if set(tables) != expected:
        raise ValueError(f"unknown or missing tables: {sorted(set(tables) ^ expected)}")
    indexes = [(table, sql) for kind, _, table, sql in objects if kind == "index"]
    return tables, indexes


def _same_table(connection, table, left="original", right="main"):
    name = quoted(table)
    left_count = connection.execute(f"SELECT COUNT(*) FROM {left}.{name}").fetchone()[0]
    right_count = connection.execute(f"SELECT COUNT(*) FROM {right}.{name}").fetchone()[0]
    if left_count != right_count:
        raise ValueError(f"row-count mismatch for {table}: {left_count} != {right_count}")
    for a, b in ((left, right), (right, left)):
        if connection.execute(
            f"SELECT * FROM {a}.{name} EXCEPT SELECT * FROM {b}.{name} LIMIT 1"
        ).fetchone() is not None:
            raise ValueError(f"content mismatch for {table}")
    return left_count


def split(source, output_dir):
    source = Path(source).resolve(strict=True)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=False)
    manifest = {"version": 1, "source_sha256": sha256(source), "shards": {}}
    with sqlite3.connect(source.as_uri() + "?mode=ro", uri=True) as original:
        tables, indexes = schema(original)
        if original.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("source SQLite integrity check failed")
        for kind, names in SHARDS.items():
            path = output_dir / f"{kind}.db"
            with path.open("xb"):
                pass
            with sqlite3.connect(path, uri=True) as dest:
                dest.execute("ATTACH DATABASE ? AS original", (source.as_uri() + "?mode=ro",))
                for table in names:
                    dest.execute(tables[table])
                    name = quoted(table)
                    dest.execute(f"INSERT INTO main.{name} SELECT * FROM original.{name}")
                for table, sql in indexes:
                    if table in names:
                        dest.execute(sql)
                dest.commit()
                counts = {name: _same_table(dest, name) for name in names}
                if dest.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise ValueError(f"{kind} shard failed integrity check")
            manifest["shards"][kind] = {
                "file": path.name, "sha256": sha256(path), "bytes": path.stat().st_size,
                "counts": counts,
            }
    with (output_dir / "manifest.json").open("x", encoding="utf-8") as stream:
        json.dump(manifest, stream, indent=2, sort_keys=True)
    return manifest


def assemble(shard_dir, output):
    shard_dir, output = Path(shard_dir), Path(output)
    manifest = json.loads((shard_dir / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("version") != 1 or set(manifest["shards"]) != set(SHARDS):
        raise ValueError("unsupported shard manifest")
    for kind in SHARDS:
        entry = manifest["shards"][kind]
        path = shard_dir / f"{kind}.db"
        if entry["file"] != path.name or sha256(path) != entry["sha256"]:
            raise ValueError(f"{kind} shard hash mismatch")
    with output.open("xb"):
        pass
    with sqlite3.connect(output, uri=True) as dest:
        for kind, names in SHARDS.items():
            path = (shard_dir / f"{kind}.db").resolve(strict=True)
            with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as part:
                object_types = {row[0] for row in part.execute(
                    "SELECT DISTINCT type FROM sqlite_master WHERE sql IS NOT NULL"
                )}
                if not object_types <= {"table", "index"}:
                    raise ValueError(f"unsupported {kind} schema object")
                tables, indexes = (
                    {name: sql for typ, name, _, sql in part.execute(
                        "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE type='table'"
                    )},
                    [(table, sql) for table, sql in part.execute(
                        "SELECT tbl_name,sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL"
                    )],
                )
                if set(tables) != set(names):
                    raise ValueError(f"unexpected {kind} tables")
                if any(table not in names for table, _ in indexes):
                    raise ValueError(f"unexpected {kind} index")
            dest.execute("ATTACH DATABASE ? AS part", (path.as_uri() + "?mode=ro",))
            for name in names:
                dest.execute(tables[name])
                quoted_name = quoted(name)
                dest.execute(f"INSERT INTO main.{quoted_name} SELECT * FROM part.{quoted_name}")
                count = dest.execute(f"SELECT COUNT(*) FROM main.{quoted_name}").fetchone()[0]
                if count != manifest["shards"][kind]["counts"][name]:
                    raise ValueError(f"{name} count changed during assembly")
            for _, sql in indexes:
                dest.execute(sql)
            dest.commit()
            dest.execute("DETACH DATABASE part")
        if dest.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("assembled DB failed integrity check")
        violations = dest.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise ValueError(f"assembled DB has {len(violations)} foreign-key violations")


def verify_source(source, assembled):
    source = Path(source).resolve(strict=True)
    with sqlite3.connect(assembled) as connection:
        connection.execute("ATTACH DATABASE ? AS original", (source.as_uri() + "?mode=ro",))
        return {table: _same_table(connection, table) for names in SHARDS.values() for table in names}


def refresh_weekly_changed(connection, changed_since):
    """Recalculate point-in-time weekly values only from each first changed date.

    Read the stock's entire history to retain the previous 19 completed weeks;
    write only the changed date onward. This is a safe first prototype, not an
    optimized history reader. Caller supplies exact changed stock/date pairs.
    """
    written = 0
    for stock_id, first_date in changed_since.items():
        first_date = pd.Timestamp(first_date)
        data = pd.read_sql_query(
            "SELECT date,close FROM daily_prices WHERE stock_id=? AND close>0 ORDER BY date",
            connection, params=(stock_id,),
        )
        if data.empty:
            connection.execute(
                "UPDATE daily_prices SET weekly_ma_5=NULL,weekly_ma_20=NULL "
                "WHERE stock_id=? AND date>=?",
                (stock_id, first_date.strftime("%Y-%m-%d")),
            )
            continue
        close = pd.Series(data.close.to_numpy(), index=pd.to_datetime(data.date))
        values = weekly_averages(close)
        connection.execute(
            "UPDATE daily_prices SET weekly_ma_5=NULL,weekly_ma_20=NULL "
            "WHERE stock_id=? AND date>=? AND (close IS NULL OR close<=0)",
            (stock_id, first_date.strftime("%Y-%m-%d")),
        )
        rows = [
            (None if pd.isna(a) else float(a), None if pd.isna(b) else float(b),
             stock_id, date.strftime("%Y-%m-%d"))
            for date, a, b in values.loc[first_date:].itertuples(name=None)
        ]
        connection.executemany(
            "UPDATE daily_prices SET weekly_ma_5=?,weekly_ma_20=? WHERE stock_id=? AND date=?",
            rows,
        )
        written += len(rows)
    connection.commit()
    return written


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    part = sub.add_parser("split")
    part.add_argument("source", type=Path)
    part.add_argument("output_dir", type=Path)
    merge = sub.add_parser("assemble")
    merge.add_argument("shard_dir", type=Path)
    merge.add_argument("output", type=Path)
    check = sub.add_parser("verify")
    check.add_argument("source", type=Path)
    check.add_argument("assembled", type=Path)
    args = parser.parse_args()
    if args.command == "split":
        print(json.dumps(split(args.source, args.output_dir)["shards"], indent=2))
    elif args.command == "assemble":
        assemble(args.shard_dir, args.output)
        print("ASSEMBLED")
    else:
        print(json.dumps(verify_source(args.source, args.assembled), indent=2))


if __name__ == "__main__":
    main()
