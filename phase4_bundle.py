"""Staged public-shard bundle and single-DB atomic install prototype.

The public bundle contains only prices + market shards. The private local
shard is supplied at install time; no remote manifest or artifact names it.
The App imports this module only when the bundle opt-in environment is set.
The default production App and updater do not use this format.
"""

import hashlib
import argparse
import json
import lzma
import os
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

from phase4_offline import SHARDS, assemble, sha256, split


PUBLIC = ("prices", "market")
MAX_PUBLIC_BYTES = 100 * 1024 * 1024
MAX_RAW_SHARD_BYTES = 1024 * 1024 * 1024


def _metadata(path, counts):
    return {"file": path.name, "sha256": sha256(path),
            "bytes": path.stat().st_size, "counts": counts}


def build_public_bundle(split_dir, output_dir):
    split_dir, output_dir = Path(split_dir), Path(output_dir)
    source = json.loads((split_dir / "manifest.json").read_text(encoding="utf-8"))
    if source.get("version") != 1 or set(source.get("shards", {})) != set(SHARDS):
        raise ValueError("invalid split manifest")
    output_dir.mkdir(parents=True, exist_ok=False)
    entries = {}
    for kind in PUBLIC:
        source_path = split_dir / f"{kind}.db"
        expected = source["shards"][kind]
        if source_path.is_symlink() or expected["file"] != source_path.name or sha256(source_path) != expected["sha256"]:
            raise ValueError("split shard hash mismatch")
        target = output_dir / source_path.name
        shutil.copyfile(source_path, target)
        entries[kind] = _metadata(target, expected["counts"])
    bundle_id = hashlib.sha256(json.dumps(entries, sort_keys=True).encode()).hexdigest()
    public = {"format": 1, "bundle_id": bundle_id, "shards": entries}
    (output_dir / "bundle.json").write_text(json.dumps(public, indent=2, sort_keys=True), encoding="utf-8")
    return public


def build_compressed_public_bundle(split_dir, output_dir, presets=None):
    """Package public shards only; bundle ID pins both wire and SQLite bytes."""
    split_dir, output_dir = Path(split_dir), Path(output_dir)
    source = json.loads((split_dir / "manifest.json").read_text(encoding="utf-8"))
    if source.get("version") != 1 or set(source.get("shards", {})) != set(SHARDS):
        raise ValueError("invalid split manifest")
    output_dir.mkdir(parents=True, exist_ok=False)
    entries = {}
    presets = presets or {"prices": 9, "market": 6}
    for kind in PUBLIC:
        path = split_dir / f"{kind}.db"
        expected = source["shards"][kind]
        if path.is_symlink() or expected["file"] != path.name or sha256(path) != expected["sha256"]:
            raise ValueError("split shard hash mismatch")
        archive = output_dir / f"{kind}.db.xz"
        with path.open("rb") as origin, lzma.open(archive, "wb", preset=presets[kind]) as packed:
            shutil.copyfileobj(origin, packed, length=1024 * 1024)
        if archive.stat().st_size >= MAX_PUBLIC_BYTES:
            raise ValueError("public shard exceeds GitHub single-file limit")
        entries[kind] = {**_metadata(archive, expected["counts"]),
                         "raw_sha256": expected["sha256"], "raw_bytes": expected["bytes"]}
    identity = hashlib.sha256(json.dumps(entries, sort_keys=True).encode()).hexdigest()
    public = {"format": 2, "bundle_id": identity, "shards": entries}
    (output_dir / "bundle.json").write_text(json.dumps(public, indent=2, sort_keys=True), encoding="utf-8")
    return public


def prepare_public_bundle(source_db, output_dir, presets=None):
    """Snapshot SQLite once, then stage only validated public archives atomically."""
    source_db, output_dir = Path(source_db), Path(output_dir)
    if source_db.is_symlink() or not source_db.is_file() or output_dir.exists():
        raise ValueError("source must be a regular DB and output must not exist")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="bundle-stage-", dir=output_dir.parent) as temporary:
        stage = Path(temporary)
        snapshot = stage / "snapshot.db"
        with sqlite3.connect(source_db.resolve().as_uri() + "?mode=ro", uri=True) as source, sqlite3.connect(snapshot) as dest:
            source.backup(dest)
        parts = stage / "parts"
        split(snapshot, parts)
        public = stage / "public"
        manifest = build_compressed_public_bundle(parts, public, presets)
        validate_public_bundle(public)
        os.replace(public, output_dir)
        return manifest


def validate_public_bundle(bundle_dir):
    bundle_dir = Path(bundle_dir)
    manifest_path = bundle_dir / "bundle.json"
    if manifest_path.is_symlink():
        raise ValueError("bundle manifest must be a regular file")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("format") not in (1, 2) or set(manifest.get("shards", {})) != set(PUBLIC):
        raise ValueError("unsupported public bundle")
    entries = manifest["shards"]
    for kind in PUBLIC:
        entry = entries[kind]
        path = bundle_dir / (f"{kind}.db.xz" if manifest["format"] == 2 else f"{kind}.db")
        if path.is_symlink() or not path.is_file() or entry.get("file") != path.name:
            raise ValueError("unexpected shard path")
        if path.stat().st_size != entry.get("bytes") or sha256(path) != entry.get("sha256"):
            raise ValueError("public shard hash mismatch")
        if set(entry.get("counts", {})) != set(SHARDS[kind]):
            raise ValueError("public shard table mismatch")
        if manifest["format"] == 2 and (path.stat().st_size >= MAX_PUBLIC_BYTES or
            type(entry.get("raw_bytes")) is not int or
            not 0 < entry["raw_bytes"] <= MAX_RAW_SHARD_BYTES or
            not isinstance(entry.get("raw_sha256"), str) or len(entry["raw_sha256"]) != 64):
            raise ValueError("invalid compressed shard metadata")
    identity = hashlib.sha256(json.dumps(entries, sort_keys=True).encode()).hexdigest()
    if manifest.get("bundle_id") != identity:
        raise ValueError("mixed public shard versions")
    return manifest


def install_public_bundle(bundle_dir, private_shard, target, after_swap=None, expected_bundle_id=None):
    """Validate/reassemble locally, then replace *one* SQLite file atomically.

    Leaves ``target.previous`` as a coherent SQLite backup for rollback. The
    caller must hold the same exclusive lock as all writers; this prototype
    does not acquire the production lock or touch the production DB itself.
    """
    bundle_dir, private_shard, target = Path(bundle_dir), Path(private_shard), Path(target)
    manifest = validate_public_bundle(bundle_dir)
    if expected_bundle_id is not None and manifest["bundle_id"] != expected_bundle_id:
        raise ValueError("public bundle does not match trusted release ID")
    if private_shard.is_symlink() or not private_shard.is_file():
        raise ValueError("private shard must be a local regular file")
    if target.is_symlink():
        raise ValueError("target DB must not be a symlink")
    target.parent.mkdir(parents=True, exist_ok=True)
    had_target = target.exists()
    if had_target and (target.with_name(target.name + "-wal").exists()
                            or target.with_name(target.name + "-shm").exists()):
        raise RuntimeError("active SQLite WAL sidecars; stop writers before atomic install")
    with tempfile.TemporaryDirectory(prefix="shard-install-", dir=target.parent) as temp_name:
        temp = Path(temp_name)
        entries = dict(manifest["shards"])
        for kind in PUBLIC:
            raw = temp / f"{kind}.db"
            if manifest["format"] == 2:
                with lzma.open(bundle_dir / f"{kind}.db.xz", "rb") as packed, raw.open("xb") as out:
                    size = 0
                    while block := packed.read(1024 * 1024):
                        size += len(block)
                        if size > entries[kind]["raw_bytes"]:
                            raise ValueError("decompressed shard exceeds declared size")
                        out.write(block)
                if raw.stat().st_size != entries[kind]["raw_bytes"] or sha256(raw) != entries[kind]["raw_sha256"]:
                    raise ValueError("decompressed shard hash mismatch")
                entries[kind] = _metadata(raw, entries[kind]["counts"])
            else:
                shutil.copyfile(bundle_dir / f"{kind}.db", raw)
        shutil.copyfile(private_shard, temp / "local.db")
        entries["local"] = _metadata(temp / "local.db", _table_counts(temp / "local.db", SHARDS["local"]))
        (temp / "manifest.json").write_text(json.dumps({"version": 1, "shards": entries}), encoding="utf-8")
        candidate = temp / "candidate.db"
        assemble(temp, candidate)
        with sqlite3.connect(candidate) as connection:
            connection.execute("CREATE TABLE _bundle_release (bundle_id TEXT NOT NULL PRIMARY KEY, "
                               "prices_sha256 TEXT NOT NULL, market_sha256 TEXT NOT NULL)")
            connection.execute("INSERT INTO _bundle_release VALUES (?,?,?)",
                               (manifest["bundle_id"], entries["prices"]["sha256"], entries["market"]["sha256"]))
            connection.commit()
            if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise ValueError("candidate DB integrity check failed")
        previous = target.with_name(target.name + ".previous")
        backup_created = False
        if had_target:
            backup = temp / "previous.db"
            with sqlite3.connect(target.resolve().as_uri() + "?mode=ro", uri=True) as source, sqlite3.connect(backup) as dest:
                source.backup(dest)
            if previous.exists():
                previous = target.with_name(target.name + f".previous.{time.time_ns()}")
            os.replace(backup, previous)
            backup_created = True
        swapped = False
        try:
            os.replace(candidate, target)
            swapped = True
            if after_swap:
                after_swap()  # fault injection for offline tests only
        except Exception:
            if backup_created:
                os.replace(previous, target)
            elif swapped and not had_target:
                target.unlink()
            raise
    return manifest["bundle_id"]


def extract_private_shard(source_db, output):
    """Copy only local tables from an installed DB, or bootstrap empty local tables."""
    source_db, output = Path(source_db), Path(output)
    if source_db.is_symlink():
        raise ValueError("private source must not be a symlink")
    if source_db.exists() and (Path(str(source_db) + "-wal").exists() or
                               Path(str(source_db) + "-shm").exists()):
        raise RuntimeError("active SQLite WAL sidecars; stop writers before private extraction")
    with sqlite3.connect(output) as dest:
        if source_db.exists():
            with sqlite3.connect(source_db.resolve().as_uri() + "?mode=ro", uri=True) as source:
                dest.execute("ATTACH DATABASE ? AS original", (source_db.resolve().as_uri() + "?mode=ro",))
                for name in SHARDS["local"]:
                    row = source.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
                    if row is None:
                        raise ValueError("missing private table")
                    dest.execute(row[0])
                    dest.execute(f'INSERT INTO "{name}" SELECT * FROM original."{name}"')
                for table, sql in source.execute("SELECT tbl_name,sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL"):
                    if table in SHARDS["local"]:
                        dest.execute(sql)
        else:
            dest.execute("CREATE TABLE user_presets(name TEXT PRIMARY KEY, settings TEXT)")
            dest.execute("CREATE TABLE watchlist(stock_id TEXT PRIMARY KEY, added_date TEXT, note TEXT)")
        dest.commit()
        if dest.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("private shard integrity check failed")


def _table_counts(path, names):
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as connection:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if tables != set(names):
            raise ValueError("private shard schema mismatch")
        return {name: connection.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0] for name in names}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("prepare")
    build.add_argument("source_db", type=Path)
    build.add_argument("output_dir", type=Path)
    check = commands.add_parser("validate")
    check.add_argument("bundle_dir", type=Path)
    args = parser.parse_args()
    if args.command == "prepare":
        manifest = prepare_public_bundle(args.source_db, args.output_dir)
    else:
        manifest = validate_public_bundle(args.bundle_dir)
    print(f"BUNDLE_ID={manifest['bundle_id']}")


if __name__ == "__main__":
    main()
