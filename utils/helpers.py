"""Utility helpers: market hours, formatting, retry."""

import asyncio
from datetime import datetime, date
from functools import wraps
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

_nyse = mcal.get_calendar("NYSE")


def now_et() -> datetime:
    """Return current time in US/Eastern."""
    return datetime.now(ET)


def is_market_open() -> bool:
    """Check if NYSE is currently open (regular hours only).
    Stops 10 minutes before close to avoid late-day order rejections
    while Ollama is still processing signals.
    """
    from datetime import timedelta
    now = now_et()
    today = now.date()
    schedule = _nyse.schedule(start_date=today, end_date=today)
    if schedule.empty:
        return False
    market_open = schedule.iloc[0]["market_open"].to_pydatetime().astimezone(ET)
    market_close = schedule.iloc[0]["market_close"].to_pydatetime().astimezone(ET)
    cutoff = market_close - timedelta(minutes=10)  # stop at 3:50 PM ET
    return market_open <= now <= cutoff


def next_market_open() -> datetime | None:
    """Return the next market open datetime in ET, or None."""
    now = now_et()
    today = now.date()
    # Look ahead up to 10 days for next open
    from datetime import timedelta
    for i in range(10):
        check_date = today + timedelta(days=i)
        schedule = _nyse.schedule(start_date=check_date, end_date=check_date)
        if not schedule.empty:
            open_time = schedule.iloc[0]["market_open"].to_pydatetime().astimezone(ET)
            if open_time > now:
                return open_time
    return None


def format_currency(value: float) -> str:
    """Format a float as currency string."""
    sign = "+" if value > 0 else ""
    return f"{sign}${value:,.2f}"


def format_pct(value: float) -> str:
    """Format a float as percentage string."""
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.1f}%"


def pnl_emoji(pnl: float) -> str:
    return "\U0001f7e2" if pnl >= 0 else "\U0001f534"


def async_retry(max_retries: int = 3, backoff_base: float = 2.0, exceptions=(Exception,)):
    """Decorator for async retry with exponential backoff."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    if attempt < max_retries - 1:
                        wait = backoff_base ** attempt
                        await asyncio.sleep(wait)
            raise last_exc
        return wrapper
    return decorator
