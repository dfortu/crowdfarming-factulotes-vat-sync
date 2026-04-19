from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo


SPAIN_TZ = ZoneInfo("Europe/Madrid")


@dataclass(frozen=True)
class DateRange:
    start_date: str
    end_date: str


def quarter_to_date_range(quarter: str, tz: ZoneInfo = SPAIN_TZ) -> DateRange:
    normalized = quarter.strip().upper()
    if len(normalized) != 6 or normalized[4] != "Q":
        raise ValueError("Quarter must use format YYYYQn, e.g. 2026Q1")

    year = int(normalized[:4])
    quarter_number = int(normalized[5])
    if quarter_number not in {1, 2, 3, 4}:
        raise ValueError("Quarter number must be between 1 and 4")

    month_ranges = {
        1: (1, 3, 31),
        2: (4, 6, 30),
        3: (7, 9, 30),
        4: (10, 12, 31),
    }
    start_month, end_month, end_day = month_ranges[quarter_number]

    start = datetime(year, start_month, 1, 0, 0, 0, tzinfo=tz)
    end = datetime(year, end_month, end_day, 23, 59, 59, tzinfo=tz)
    return DateRange(start_date=start.isoformat(), end_date=end.isoformat())
