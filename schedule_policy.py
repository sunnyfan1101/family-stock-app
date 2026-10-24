"""Proposed Phase-3 schedule rules (not installed in cron)."""

from datetime import date


def monthly_revenue_due(day):
    """Revenue notices are independent of whether equities traded today."""
    day = date.fromisoformat(day) if isinstance(day, str) else day
    return 10 <= day.day <= 15
