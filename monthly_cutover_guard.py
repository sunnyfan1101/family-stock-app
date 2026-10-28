"""Refuse the independent monthly runner while legacy forced jobs are scheduled.

This is a deployment interlock, not a replacement for a global API quota.
The daily stock-price path may continue; its phase-3 version does not call
monthly revenue. Cron is inspected read-only.
"""

import subprocess
import sys


def conflicting_jobs(crontab):
    conflicts = []
    for line in crontab.splitlines():
        job = line.strip()
        if not job or job.startswith("#"):
            continue
        if "local_stock_updater.sh" in job and "--force-financials" in job:
            conflicts.append(job)
    return conflicts


def assert_safe_cutover():
    result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError("cannot inspect cron; refusing monthly cutover")
    conflicts = conflicting_jobs(result.stdout)
    if conflicts:
        raise RuntimeError(
            f"{len(conflicts)} legacy forced-financial cron jobs remain; "
            "refusing concurrent monthly runner"
        )


if __name__ == "__main__":
    try:
        assert_safe_cutover()
    except RuntimeError as error:
        print(f"MONTHLY_CUTOVER_BLOCKED: {error}", file=sys.stderr)
        raise SystemExit(2)
    print("MONTHLY_CUTOVER_GUARD_OK")
