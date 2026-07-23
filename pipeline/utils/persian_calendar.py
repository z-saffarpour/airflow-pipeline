"""
Gregorian ↔ Jalali (Persian/Shamsi) calendar helpers.

Used when ClickHouse tables are partitioned by Persian date keys
(e.g. PersianYearMonthInt = YYYYMM in Jalali calendar).
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Tuple, Union


DateLike = Union[str, date, datetime]


def _parse_gregorian(value: DateLike) -> date:
    """Normalize YYYYMMDD / date / datetime to a Gregorian date."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        text = value.strip()
        if len(text) == 8 and text.isdigit():
            return datetime.strptime(text, "%Y%m%d").date()
        if len(text) == 10 and text[4] == "-" and text[7] == "-":
            return datetime.strptime(text, "%Y-%m-%d").date()
    raise ValueError(
        f"Invalid Gregorian date: {value!r}. Expected YYYYMMDD, YYYY-MM-DD, date, or datetime."
    )


def gregorian_to_jalali(gy: int, gm: int, gd: int) -> Tuple[int, int, int]:
    """
    Convert Gregorian (gy, gm, gd) to Jalali (jy, jm, jd).

    Algorithm adapted from the commonly used jalaali conversion
    (compatible with PersianYear* keys used in DIM_Date).
    """
    g_d_m = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]

    if gy > 1600:
        jy = 979
        gy -= 1600
    else:
        jy = 0
        gy -= 621

    gy2 = gy + 1 if gm > 2 else gy
    days = (
        365 * gy
        + (gy2 + 3) // 4
        - (gy2 + 99) // 100
        + (gy2 + 399) // 400
        - 80
        + gd
        + g_d_m[gm - 1]
    )

    jy += 33 * (days // 12053)
    days %= 12053
    jy += 4 * (days // 1461)
    days %= 1461

    if days > 365:
        jy += (days - 1) // 365
        days = (days - 1) % 365

    if days < 186:
        jm = 1 + days // 31
        jd = 1 + days % 31
    else:
        jm = 7 + (days - 186) // 30
        jd = 1 + (days - 186) % 30

    return jy, jm, jd


def to_jalali(value: DateLike) -> Tuple[int, int, int]:
    """Convert a Gregorian date-like value to (year, month, day) Jalali."""
    g = _parse_gregorian(value)
    return gregorian_to_jalali(g.year, g.month, g.day)


def to_persian_year(value: DateLike) -> str:
    """Return Jalali year as YYYY string (e.g. '1405')."""
    jy, _, _ = to_jalali(value)
    return f"{jy:04d}"


def to_persian_year_month(value: DateLike) -> str:
    """Return Jalali year-month as YYYYMM string (e.g. '140504')."""
    jy, jm, _ = to_jalali(value)
    return f"{jy:04d}{jm:02d}"


def to_persian_date_key(value: DateLike) -> str:
    """Return Jalali date key as YYYYMMDD string (e.g. '14050401')."""
    jy, jm, jd = to_jalali(value)
    return f"{jy:04d}{jm:02d}{jd:02d}"


def to_persian_date(value: DateLike) -> str:
    """Return Jalali date as YYYY/MM/DD string (e.g. '1405/05/01')."""
    jy, jm, jd = to_jalali(value)
    return f"{jy:04d}/{jm:02d}/{jd:02d}"
