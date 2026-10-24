"""Offline public-shard bundle and single-DB atomic install prototype.

The public bundle contains only prices + market shards. The private local
shard is supplied at install time; no remote manifest or artifact names it.
This module is deliberately not imported by the production app or updater.
"""

import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

from phase4_offline import SHARDS, assemble, sha256


PUBLIC = ("prices", "market")


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


def validate_public_bundle(bundle_dir):
    bundle_dir = Path(bundle_dir)
    manifest_path = bundle_dir / "bundle.json"
    if manifest_path.is_symlink():
        raise ValueError("bundle manifest must be a regular file")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("format") != 1 or set(manifest.get("shards", {})) != set(PUBLIC):
        raise ValueError("unsupported public bundle")
    entries = manifest["shards"]
    for kind in PUBLIC:
        entry = entries[kind]
        path = bundle_dir / f"{kind}.db"
        if path.is_symlink() or not path.is_file() or entry.get("file") != path.name:
            raise ValueError("unexpected shard path")
        if path.stat().st_size != entry.get("bytes") or sha256(path) != entry.get("sha256"):
            raise ValueError("public shard hash mismatch")
        if set(entry.get("counts", {})) != set(SHARDS[kind]):
            raise ValueError("public shard table mismatch")
    identity = hashlib.sha256(json.dumps(entries, sort_keys=True).encode()).hexdigest()
    if manifest.get("bundle_id") != identity:
        raise ValueError("mixed public shard versions")
    return manifest


def install_public_bundle(bundle_dir, private_shard, target, after_swap=None):
    """Validate/reassemble locally, then replace *one* SQLite file atomically.

    Leaves ``target.previous`` as a coherent SQLite backup for rollback. The
    caller must hold the same exclusive lock as all writers; this prototype
    does not acquire the production lock or touch the production DB itself.
    """
    bundle_dir, private_shard, target = Path(bundle_dir), Path(private_shard), Path(target)
    manifest = validate_public_bundle(bundle_dir)
    if private_shard.is_symlink() or not private_shard.is_file():
        raise ValueError("private shard must be a local regular file")
    target.parent.mkdir(parents=True, exist_ok=True)
    had_target = target.exists()
    if had_target and (target.with_name(target.name + "-wal").exists()
                            or target.with_name(target.name + "-shm").exists()):
        raise RuntimeError("active SQLite WAL sidecars; stop writers before atomic install")
    with tempfile.TemporaryDirectory(prefix="shard-install-", dir=target.parent) as temp_name:
        temp = Path(temp_name)
        entries = dict(manifest["shards"])
        for kind in PUBLIC:
            shutil.copyfile(bundle_dir / f"{kind}.db", temp / f"{kind}.db")
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
        if had_target:
            backup = temp / "previous.db"
            with sqlite3.connect(target.resolve().as_uri() + "?mode=ro", uri=True) as source, sqlite3.connect(backup) as dest:
                source.backup(dest)
            if previous.exists():
                raise FileExistsError("previous rollback backup already exists")
            os.replace(backup, previous)
        swapped = False
        try:
            os.replace(candidate, target)
            swapped = True
            if after_swap:
                after_swap()  # fault injection for offline tests only
        except Exception:
            if previous.exists():
                os.replace(previous, target)
            elif swapped and not had_target:
                target.unlink()
            raise
    return manifest["bundle_id"]


def _table_counts(path, names):
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as connection:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if tables != set(names):
            raise ValueError("private shard schema mismatch")
        return {name: connection.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0] for name in names}
