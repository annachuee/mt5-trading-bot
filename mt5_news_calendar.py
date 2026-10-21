"""Async fetch and strict parsing for the weekly Forex Factory calendar feed.

The JSON endpoint is a public third-party mirror of Forex Factory calendar
data. It is an unofficial dependency, so callers must treat fetch or parsing
errors as unavailable data and fail closed.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import aiohttp


DEFAULT_CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"


class CalendarUnavailable(RuntimeError):
    """Raised when the calendar cannot be safely loaded and parsed."""


@dataclass(frozen=True)
class EconomicEvent:
    title: str
    currency: str
    impact: str
    release_utc: datetime


async def fetch_weekly_calendar(
    url: str = DEFAULT_CALENDAR_URL, *, timeout_seconds: float = 8.0,
    currencies: tuple[str, ...] = ("USD", "EUR", "GBP"),
) -> tuple[EconomicEvent, ...]:
    """Fetch current-week high-impact events for selected currencies.

    Event timestamps must carry an explicit UTC offset. Ambiguous or malformed
    high-impact target-currency events invalidate the entire refresh.
    """
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    headers = {"Accept": "application/json", "User-Agent": "MT5RiskCalendar/1.0"}
    try:
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise CalendarUnavailable(f"calendar HTTP status {response.status}")
                payload = await response.json(content_type=None)
    except CalendarUnavailable:
        raise
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
        raise CalendarUnavailable(f"calendar request failed ({type(exc).__name__})") from exc

    if not isinstance(payload, list):
        raise CalendarUnavailable("calendar payload must be a JSON event list")
    if not payload:
        raise CalendarUnavailable("calendar returned no events; current week cannot be verified")
    target = {code.upper() for code in currencies}
    events: list[EconomicEvent] = []
    for row in payload:
        if not isinstance(row, dict):
            raise CalendarUnavailable("calendar contains a non-object event")
        if not ({"country", "currency"} & row.keys()) or "impact" not in row:
            raise CalendarUnavailable("calendar event is missing currency or impact fields")
        currency = str(row.get("country", row.get("currency", ""))).strip().upper()
        impact = str(row.get("impact", "")).strip().lower()
        if currency not in target or impact not in {"high", "high impact", "high impact expected"}:
            continue
        title = str(row.get("title", "")).strip()
        raw_date = row.get("date", row.get("datetime", row.get("time")))
        if not title or not isinstance(raw_date, str) or not raw_date.strip():
            raise CalendarUnavailable(f"high-impact {currency} event is missing title or timestamp")
        try:
            release = datetime.fromisoformat(raw_date.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise CalendarUnavailable(f"invalid timestamp for high-impact event {title!r}") from exc
        if release.tzinfo is None or release.utcoffset() is None:
            raise CalendarUnavailable(f"timezone missing for high-impact event {title!r}")
        events.append(EconomicEvent(
            title=title,
            currency=currency,
            impact="high",
            release_utc=release.astimezone(timezone.utc),
        ))
    return tuple(sorted(events, key=lambda event: event.release_utc))
