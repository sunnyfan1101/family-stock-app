"""Fail-closed market-session preflight; never infer a closure from empty quotes.

Only weekends or explicitly documented *both-exchange* closures can skip a
daily quote run. Unknown weekdays run the fetcher; poor coverage is failure.
"""

import argparse
import json
from datetime import date
from pathlib import Path
from urllib.parse import urlparse


OFFICIAL_HOSTS = {"www.twse.com.tw", "www.tpex.org.tw"}


def session_status(day, closure_file=None):
    day = date.fromisoformat(day) if isinstance(day, str) else day
    if day.weekday() >= 5:
        return "closed_weekend"
    if closure_file is None:
        return "unknown_weekday"
    closures = json.loads(Path(closure_file).read_text(encoding="utf-8"))
    if not isinstance(closures, dict) or set(closures) != {"closures"} or not isinstance(closures["closures"], list):
        raise ValueError("invalid closure list")
    seen = set()
    for entry in closures["closures"]:
        if not isinstance(entry, dict) or set(entry) != {"date", "twse", "tpex"}:
            raise ValueError("closure entry requires date and both exchange sources")
        date.fromisoformat(entry["date"])
        if entry["date"] in seen:
            raise ValueError("duplicate closure date")
        seen.add(entry["date"])
        for market, host in (("twse", "www.twse.com.tw"), ("tpex", "www.tpex.org.tw")):
            source = urlparse(entry[market])
            if source.scheme != "https" or source.hostname != host or not source.path:
                raise ValueError(f"invalid official {market} reference")
    return "closed_documented" if day.isoformat() in seen else "unknown_weekday"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--closure-file", type=Path)
    args = parser.parse_args()
    status = session_status(args.date, args.closure_file)
    print(f"MARKET_SESSION_{status.upper()} date={args.date}")
    return 10 if status.startswith("closed") else 0


if __name__ == "__main__":
    raise SystemExit(main())
