"""Crash-recoverable cursor for staged quarterly batches.

The cursor advances only after an unchanged batch or a confirmed remote push.
An interrupted/failed push leaves a pending candidate hash. On restart the
same live archive is pushed again before any new FinMind request is made.
"""

import argparse
import hashlib
import json
import os
import subprocess
import tempfile
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo


def digest(path):
    hashed = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            hashed.update(block)
    return hashed.hexdigest()


def load(path):
    path = Path(path)
    if not path.exists():
        return {"version": 1, "next_offset": 0, "pending": None}
    data = json.loads(path.read_text(encoding="utf-8"))
    if set(data) != {"version", "next_offset", "pending"} or data["version"] != 1:
        raise ValueError("unsupported financial cursor state")
    if type(data["next_offset"]) is not int or data["next_offset"] < 0:
        raise ValueError("invalid financial cursor offset")
    pending = data["pending"]
    if pending is not None and (not isinstance(pending, dict) or
                                set(pending) != {"sha256", "next_offset"} or
                                type(pending["next_offset"]) is not int or
                                pending["next_offset"] < 0 or
                                not isinstance(pending["sha256"], str) or
                                len(pending["sha256"]) != 64):
        raise ValueError("invalid pending financial publication")
    return data


def save(path, state):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".financial-cursor-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(state, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def reserve_requests(path, count, daily_cap=150, day=None):
    """Reserve before HTTP work; an interrupted batch consumes its allowance.

    This protects only this quarterly runner, not unrelated FinMind callers.
    The staged updater holds the exclusive stock-update lock.
    """
    if not 0 <= count <= daily_cap or daily_cap < 1:
        raise ValueError("quarterly batch exceeds configured daily request cap")
    today = day or datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
    path = Path(path)
    state = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"day": today, "used": 0}
    if set(state) != {"day", "used"} or type(state["used"]) is not int or state["used"] < 0:
        raise ValueError("invalid quarterly request budget")
    if state["day"] != today:
        state = {"day": today, "used": 0}
    if state["used"] + count > daily_cap:
        raise RuntimeError("quarterly FinMind daily request cap reached")
    state["used"] += count
    save(path, state)
    return state["used"]


def progress(path):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if set(data) != {"offset", "next_offset", "checked", "total"}:
        raise ValueError("invalid batch progress")
    if any(type(value) is not int or value < 0 for value in data.values()):
        raise ValueError("invalid batch progress values")
    if data["checked"] > data["total"] or data["next_offset"] > data["total"]:
        raise ValueError("batch progress exceeds universe")
    return data


def advance_nochange(state_path, progress_path):
    state, batch = load(state_path), progress(progress_path)
    if state["pending"] is not None or state["next_offset"] != batch["offset"]:
        raise RuntimeError("unresolved push or stale financial batch")
    state["next_offset"] = batch["next_offset"]
    save(state_path, state)
    return state["next_offset"]


def prepare(state_path, progress_path, candidate_xz):
    state, batch = load(state_path), progress(progress_path)
    if state["pending"] is not None or state["next_offset"] != batch["offset"]:
        raise RuntimeError("unresolved push or stale financial batch")
    state["pending"] = {"sha256": digest(candidate_xz), "next_offset": batch["next_offset"]}
    save(state_path, state)


def finalize(state_path, live_xz):
    state = load(state_path)
    pending = state["pending"]
    if pending is None or digest(live_xz) != pending["sha256"]:
        raise RuntimeError("live financial archive differs from pending push")
    state["next_offset"] = pending["next_offset"]
    state["pending"] = None
    save(state_path, state)
    return state["next_offset"]


def reconcile(state_path, live_xz, push):
    state = load(state_path)
    if state["pending"] is None:
        return False
    if digest(live_xz) != state["pending"]["sha256"]:
        raise RuntimeError("pending quarterly publication differs from live archive; manual review required")
    push()
    finalize(state_path, live_xz)
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("current", "advance-nochange", "prepare", "finalize", "reconcile"))
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--progress", type=Path)
    parser.add_argument("--candidate-xz", type=Path)
    parser.add_argument("--live-xz", type=Path)
    parser.add_argument("--push-script", type=Path)
    args = parser.parse_args()
    if args.action == "current":
        state = load(args.state)
        if state["pending"] is not None:
            raise RuntimeError("pending publication must be reconciled first")
        print(state["next_offset"])
    elif args.action == "advance-nochange":
        advance_nochange(args.state, args.progress)
    elif args.action == "prepare":
        prepare(args.state, args.progress, args.candidate_xz)
    elif args.action == "finalize":
        finalize(args.state, args.live_xz)
    else:
        if args.push_script is None:
            parser.error("reconcile requires --push-script")
        reconcile(args.state, args.live_xz, lambda: subprocess.run([str(args.push_script)], check=True))


if __name__ == "__main__":
    main()
