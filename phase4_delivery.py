"""Fetch a public shard release only when a separately pinned ID matches.

The pin is supplied by deployment configuration, never learned from the
downloaded manifest. This opt-in transport does not change the default App.
"""

import re
import shutil
import tempfile
from pathlib import Path
from urllib.parse import quote
from urllib.request import urlopen

from phase4_bundle import MAX_PUBLIC_BYTES, validate_public_bundle


REPOSITORY = "sunnyfan1101/family-stock-app"
ASSETS = ("bundle.json", "prices.db.xz", "market.db.xz")
MAX_MANIFEST_BYTES = 64 * 1024


def _download(url, target, limit, opener):
    with opener(url, timeout=45) as response, target.open("xb") as output:
        size = 0
        while block := response.read(1024 * 1024):
            size += len(block)
            if size > limit:
                raise ValueError("release asset exceeds size limit")
            output.write(block)
    if size == 0:
        raise ValueError("empty release asset")


def fetch_release_bundle(tag, expected_id, cache_root, opener=None):
    """Return a fully verified cached release directory, or fail closed.

    GitHub's tag is a locator, not the trust anchor. The caller must supply
    ``expected_id`` independently (for example, a reviewed Cloud setting).
    """
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", tag):
        raise ValueError("invalid release tag")
    if not re.fullmatch(r"[0-9a-f]{64}", expected_id):
        raise ValueError("invalid trusted bundle ID")
    cache_root = Path(cache_root)
    if cache_root.is_symlink():
        raise ValueError("bundle cache must not be a symlink")
    cache_root.mkdir(parents=True, exist_ok=True)
    destination = cache_root / expected_id
    if destination.is_symlink():
        raise ValueError("cached bundle must not be a symlink")
    if destination.exists():
        manifest = validate_public_bundle(destination)
        if manifest["format"] != 2 or manifest["bundle_id"] != expected_id:
            raise ValueError("cached release does not match trusted ID")
        return destination

    opener = opener or urlopen
    with tempfile.TemporaryDirectory(prefix="bundle-download-", dir=cache_root) as temporary:
        staging = Path(temporary)
        for asset in ASSETS:
            url = (f"https://github.com/{REPOSITORY}/releases/download/"
                   f"{quote(tag, safe='')}/{asset}")
            _download(url, staging / asset,
                      MAX_MANIFEST_BYTES if asset == "bundle.json" else MAX_PUBLIC_BYTES,
                      opener)
        manifest = validate_public_bundle(staging)
        if manifest["format"] != 2 or manifest["bundle_id"] != expected_id:
            raise ValueError("downloaded release does not match trusted ID")
        shutil.move(str(staging), str(destination))
    return destination


def require_install_headroom(bundle_dir, target):
    """Fail before DB replacement when disk cannot hold assembly and backup."""
    manifest = validate_public_bundle(bundle_dir)
    if manifest["format"] != 2:
        raise ValueError("Cloud install requires compressed public shards")
    raw = sum(entry["raw_bytes"] for entry in manifest["shards"].values())
    existing = Path(target).stat().st_size if Path(target).exists() else 0
    # Raw shards + assembled candidate + old DB backup + 256 MiB margin.
    required = raw * 2 + existing + 256 * 1024 * 1024
    free = shutil.disk_usage(Path(target).parent).free
    if free < required:
        raise OSError(f"insufficient disk for bundle install: need {required} free bytes")
