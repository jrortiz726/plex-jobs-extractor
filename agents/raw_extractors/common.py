"""Shared helpers for RAW extractors."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Tuple


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def time_range(since: datetime | None, lookback_days: int) -> Tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    if since:
        start = since
    else:
        start = now - timedelta(days=lookback_days)
    start = start.astimezone(timezone.utc)
    return start, now
