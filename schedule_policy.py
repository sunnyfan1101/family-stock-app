"""Proposed Phase-3 schedule rules (not installed in cron)."""

from datetime import date


def monthly_revenue_due(day):
    """Revenue notices are independent of whether equities traded today."""
    day = date.fromisoformat(day) if isinstance(day, str) else day
    return 10 <= day.day <= 15


def proposed_jobs(day):
    """Offline schedule proposal only; callers must check source/closure gates.

    Quarterly periods are spread across the statutory reporting months so a
    150-symbol batch can cover the ~2,000-stock universe without bursts.
    This function does not install jobs or assert that a weekday traded.
    """
    day = date.fromisoformat(day) if isinstance(day, str) else day
    jobs = []
    if day.weekday() < 5:
        jobs.append("daily_prices")
    if monthly_revenue_due(day):
        jobs.append("monthly_revenue")
    if day.month in (3, 5, 8, 11) and 1 <= day.day <= 15:
        jobs.append("quarterly_financials")
    return tuple(jobs)
