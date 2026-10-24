"""Pure dry-run scheduling checks; never install or trigger cron."""

import unittest

from schedule_policy import proposed_jobs


class ProposedScheduleTests(unittest.TestCase):
    def test_weekend_monthly_revenue_is_independent_of_trading(self):
        self.assertEqual(proposed_jobs("2026-10-10"), ("monthly_revenue",))

    def test_reporting_month_quarterly_batch_runs_even_on_weekend(self):
        self.assertEqual(proposed_jobs("2026-11-14"),
                         ("monthly_revenue", "quarterly_financials"))

    def test_weekday_still_needs_market_session_quality_gate(self):
        self.assertEqual(proposed_jobs("2026-10-23"), ("daily_prices",))

    def test_quarterly_job_is_bounded_to_window(self):
        self.assertNotIn("quarterly_financials", proposed_jobs("2026-11-16"))


if __name__ == "__main__":
    unittest.main()
