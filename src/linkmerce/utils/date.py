from __future__ import annotations

from functools import total_ordering
import datetime as dt

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, TypeVar
    from pytz import BaseTzInfo
    _NUMERIC = TypeVar("_NUMERIC", float, int, bool, dt.date, dt.datetime)


DATE_UNIT = ["second", "minute", "hour", "day", "month", "year"]


###################################################################
############################# strptime ############################
###################################################################

def strptime(
        datetime: dt.datetime | str,
        format: str = "%Y-%m-%d",
        tzinfo: BaseTzInfo | str | None = None,
        astimezone: BaseTzInfo | str | None = None,
        droptz: bool = False,
    ) -> dt.datetime:
    if isinstance(datetime, dt.datetime):
        return datetime
    datetime = dt.datetime.strptime(str(datetime), format)
    return set_timezone(datetime, tzinfo, astimezone, droptz)


def safe_strptime(
        datetime: dt.datetime | str,
        format: str = "%Y-%m-%d",
        default: dt.datetime | None = None,
        tzinfo: BaseTzInfo | str | None = None,
        astimezone: BaseTzInfo | str | None = None,
        droptz: bool = False,
    ) -> dt.datetime:
    try:
        return strptime(datetime, format, tzinfo, astimezone, droptz)
    except:
        return default


###################################################################
############################# strpdate ############################
###################################################################

def strpdate(date: dt.date | str, format: str = "%Y-%m-%d") -> dt.date:
    if isinstance(date, dt.date):
        return date
    else:
        return dt.datetime.strptime(str(date), format).date()


def safe_strpdate(date: dt.date | str, format: str = "%Y-%m-%d", default: dt.date | None = None) -> dt.date:
    try:
        return strpdate(date, format)
    except:
        return default


###################################################################
############################# Datetime ############################
###################################################################

def now(
        days: int = 0,
        seconds: int = 0,
        microseconds: int = 0,
        milliseconds: int = 0,
        minutes: int = 0,
        hours: int = 0,
        weeks: int = 0,
        delta: Literal['+','-'] = '-',
        tzinfo: BaseTzInfo | str | None = None,
        droptz: bool = False,
        unit: Literal["second","minute","hour","day","month","year"] | None = "second",
    ) -> dt.datetime:
    datetime = dt.datetime.now(get_timezone(tzinfo))
    if days or seconds or microseconds or milliseconds or minutes or hours or weeks:
        timedelta = dt.timedelta(days, seconds, microseconds, milliseconds, minutes, hours, weeks)
        datetime = (datetime - timedelta) if delta == '-' else (datetime + timedelta)
    if droptz:
        datetime = datetime.replace(tzinfo=None)
    if unit:
        datetime = trunc_datetime(datetime, unit)
    return datetime


def today(
        days: int = 0,
        weeks: int = 0,
        delta: Literal['+','-'] = '-',
        tzinfo: BaseTzInfo | str | None = None,
        unit: Literal["month","year"] | None = None,
    ) -> dt.date:
    return now(days=days, weeks=weeks, delta=delta, tzinfo=tzinfo, unit=unit).date()


def trunc_datetime(
        datetime: dt.datetime,
        unit: Literal["second","minute","hour","day","month","year"] | None = None
    ) -> dt.datetime:
    if unit not in DATE_UNIT:
        return datetime
    index = DATE_UNIT.index(unit.lower())
    if index >= 0:
        datetime = datetime.replace(microsecond=0)
    if index >= 1:
        datetime = datetime.replace(second=0)
    if index >= 2:
        datetime = datetime.replace(minute=0)
    if index >= 3:
        datetime = datetime.replace(hour=0)
    if index >= 4:
        datetime = datetime.replace(day=1)
    if index >= 5:
        datetime = datetime.replace(month=1)
    return datetime


###################################################################
############################ Time Zone ############################
###################################################################

def get_timezone(tzinfo: BaseTzInfo | str | None = None) -> BaseTzInfo:
    if tzinfo:
        from pytz import timezone, UnknownTimeZoneError
        try:
            return timezone(tzinfo)
        except UnknownTimeZoneError:
            return


def set_timezone(
        datetime: dt.datetime,
        tzinfo: BaseTzInfo | str | None = None,
        astimezone: BaseTzInfo | str | None = None,
        droptz: bool = False
    ) -> dt.datetime:
    if tzinfo and (tz := get_timezone(tzinfo)):
        datetime = datetime.astimezone(tz) if datetime.tzinfo else tz.localize(datetime)
    if astimezone and datetime.tzinfo:
        datetime = datetime.astimezone(get_timezone(astimezone))
    return datetime.replace(tzinfo=None) if droptz else datetime


###################################################################
############################ Date Range ###########################
###################################################################

@total_ordering
class YearMonth:
    def __init__(self, year: int, month: int):
        self.year = year
        self.month = min(max(month, 1), 12)

    def date(self, day: int = 1) -> dt.date:
        return dt.date(self.year, self.month, day)

    def eomonth(self) -> dt.date:
        from calendar import monthrange
        return self.date(monthrange(self.year, self.month)[1])

    def _compare(self):
        return self.year * 100 + self.month

    def __eq__(self, other):
        if isinstance(other, YearMonth):
            return self._compare() == other._compare()
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, YearMonth):
            return self._compare() < other._compare()
        return NotImplemented

    def __pos__(self) -> YearMonth:
        self.__add__(1)
        return self

    def __add__(self, other: int) -> YearMonth:
        year = self.year
        month = self.month + other
        if month > 12:
            year += (month // 12)
            month = (month % 12)
        elif month < 1:
            delta = 1 - month
            year -= (delta - 1) // 12 + 1
            month = 12 - ((delta - 1) % 12)
        else:
            year = self.year
            month = month
        return YearMonth(year, month)

    def __sub__(self, other: int) -> YearMonth:
        return self.__add__(-other)

    def __str__(self) -> str:
        return "{}-{}".format(self.year, str(self.month).rjust(2, '0'))


def date_range(
        start: dt.date | str,
        end: dt.date | str,
        freq: Literal["D","W","M"] = "D",
        format: str = "%Y-%m-%d",
    ) -> list[dt.date]:
    return _generate_date_range(start, end, freq, format, add_one=False)


def date_pairs(
        start: dt.date | str,
        end: dt.date | str,
        freq: Literal["D","W","M"] = "D",
        format: str = "%Y-%m-%d",
    ) -> list[tuple[dt.date,dt.date]]:
    if freq.upper() == "D":
        return [(date, date) for date in _generate_date_range(start, end, "D", format, add_one=False)]
    else:
        ranges = _generate_date_range(start, end, freq, add_one=True)
        return [(ranges[i], ranges[i+1] - dt.timedelta(days=1)) for i in range(len(ranges)-1)]


def date_split(
        start: dt.date | str,
        end: dt.date | str,
        delta: int | dict[Literal["days","seconds","microseconds","milliseconds","minutes","hours","weeks"],float] = 1,
        format: str = "%Y-%m-%d",
    ) -> list[tuple[dt.date,dt.date]]:
    if isinstance(delta, int):
        delta = dict(days=delta)

    if delta.get("days") == 1:
        return date_pairs(start, end, freq="D", format=format)
    else:
        ranges, start, end = list(), strpdate(start, format), strpdate(end, format)
        cur = start
        while cur <= end:
            next = cur + dt.timedelta(**delta)
            ranges.append((cur, min(next, end)))
            cur = next + dt.timedelta(days=1)
        return ranges


def _generate_date_range(
        start: dt.date | str,
        end: dt.date | str,
        freq: Literal["D","W","M"] = "D",
        format: str = "%Y-%m-%d",
        add_one: bool = False,
    ) -> list[dt.date]:
    start, end = strpdate(start, format), strpdate(end, format)
    freq = freq.upper()
    if freq == "D":
        return _generate_range(start, end, dt.timedelta(days=1))
    elif freq == "W":
        start_of_week = start - dt.timedelta(days=start.weekday())
        end_of_week = end + dt.timedelta(days=(6-end.weekday())) + dt.timedelta(days=(7 * int(add_one)))
        return _generate_range(start_of_week, end_of_week, dt.timedelta(days=7))
    elif freq == "M":
        start_month = YearMonth(start.year, start.month)
        end_month = YearMonth(end.year, end.month) + int(add_one)
        return [ym.date() for ym in _generate_range(start_month, end_month, 1)]
    else:
        raise ValueError("Invalid frequency value. Supported frequencies are: 'D', 'W', 'M'")


def _generate_range(start: _NUMERIC, end: _NUMERIC, delta: _NUMERIC) -> list[_NUMERIC]:
    ranges = list()
    cur = start
    while cur <= end:
        ranges.append(cur)
        cur += delta
    return ranges
