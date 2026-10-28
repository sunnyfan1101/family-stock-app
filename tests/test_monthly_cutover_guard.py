import unittest
from unittest.mock import patch

import monthly_cutover_guard as guard


class MonthlyCutoverGuardTests(unittest.TestCase):
    def test_only_active_legacy_forced_jobs_conflict(self):
        crontab = """# 0 18 * * * /repo/local_stock_updater.sh --force-financials
15 15 * * 1-5 /repo/local_stock_updater.sh
0 18 10-15 * * /repo/local_stock_updater.sh --force-financials
0 19 * * * /repo/local_stock_updater.sh --revenue-only
"""
        self.assertEqual(len(guard.conflicting_jobs(crontab)), 1)

    def test_refuses_unknown_cron_and_active_legacy_job(self):
        class Result:
            def __init__(self, code, output):
                self.returncode, self.stdout = code, output

        with patch.object(guard.subprocess, "run", return_value=Result(1, "")):
            with self.assertRaisesRegex(RuntimeError, "cannot inspect cron"):
                guard.assert_safe_cutover()
        with patch.object(guard.subprocess, "run", return_value=Result(
            0, "0 18 10-15 * * /repo/local_stock_updater.sh --force-financials")):
            with self.assertRaisesRegex(RuntimeError, "legacy forced-financial"):
                guard.assert_safe_cutover()
        with patch.object(guard.subprocess, "run", return_value=Result(
            0, "0 18 * * * /repo/local_stock_updater.sh --revenue-only")):
            guard.assert_safe_cutover()


if __name__ == "__main__":
    unittest.main()
