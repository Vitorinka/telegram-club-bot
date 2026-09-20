from datetime import datetime, timedelta


ANALYTICS_PERIODS = frozenset((7, 30, 90, 365))


class AdminAnalyticsQueryError(ValueError):
    pass


def analytics_period_bounds(days, now=None):
    try:
        days = int(days)
    except (TypeError, ValueError):
        raise AdminAnalyticsQueryError("invalid_period") from None
    if days not in ANALYTICS_PERIODS:
        raise AdminAnalyticsQueryError("invalid_period")
    end = now or datetime.utcnow()
    start = end - timedelta(days=days)
    return start, end, start - timedelta(days=days), start


def analytics_projection(days, start, end, metrics, comparison):
    keys = (
        "total_users_now", "active_paid_now", "new_registrations",
        "access_closed", "initial_purchases", "recurring_payments",
        "successful_payments", "failed_payments", "recovered_after_failure",
        "auto_renew_disabled", "grace_period_now", "payment_failed_now",
    )
    return {
        "period_days": int(days),
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "metrics": {key: int(metrics.get(key) or 0) for key in keys},
        "comparison": {key: int(comparison.get(key) or 0) for key in keys},
        "revenue_by_currency": {
            str(currency): int(amount or 0)
            for currency, amount in (metrics.get("revenue_by_currency") or {}).items()
        },
        "tracking": {
            "content_views": False,
            "lesson_starts": False,
            "lesson_completions": False,
            "active_minutes": False,
            "dau_wau_mau": False,
        },
    }
