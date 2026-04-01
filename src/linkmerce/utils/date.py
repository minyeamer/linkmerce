from __future__ import annotations

from functools import total_ordering
import datetime as dt

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, TypeVar
    from pytz import BaseTzInfo
    from pendulum.tz.timezone import Timezone, FixedTimezone
    import pendulum
    _NUMERIC = TypeVar("_NUMERIC", float, int, bool, dt.date, dt.datetime)


DATE_UNIT = ["second", "minute", "hour", "day", "month", "year"]
DEFAULT_TIMEZONE = "Asia/Seoul"


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
    """문자열을 지정된 포맷으로 파싱하여 `datetime` 객체로 변환한다."""
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
    """문자열을 `datetime` 객체로 안전하게 변환한다. 실패 시 기본값을 반환한다."""
    try:
        return strptime(datetime, format, tzinfo, astimezone, droptz)
    except:
        return default


###################################################################
############################# strpdate ############################
###################################################################

def strpdate(date: dt.date | str, format: str = "%Y-%m-%d") -> dt.date:
    """문자열을 `date` 객체로 변환한다."""
    if isinstance(date, dt.date):
        return date
    else:
        return dt.datetime.strptime(str(date), format).date()


def safe_strpdate(date: dt.date | str, format: str = "%Y-%m-%d", default: dt.date | None = None) -> dt.date:
    """문자열을 `date` 객체로 안전하게 변환한다. 실패 시 기본값을 반환한다."""
    try:
        return strpdate(date, format)
    except:
        return default


###################################################################
############################# datetime ############################
###################################################################

def now(
        days: int = 0,
        seconds: int = 0,
        microseconds: int = 0,
        milliseconds: int = 0,
        minutes: int = 0,
        hours: int = 0,
        weeks: int = 0,
        delta: Literal['+', '-'] = '-',
        tzinfo: BaseTzInfo | str | None = None,
        droptz: bool = False,
        unit: Literal["second", "minute", "hour", "day", "month", "year"] | None = "second",
    ) -> dt.datetime:
    """현재 시각을 반환한다. `timedelta` 오프셋과 날짜 단위를 지정할 수 있다."""
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
        delta: Literal['+', '-'] = '-',
        tzinfo: BaseTzInfo | str | None = None,
        unit: Literal["month", "year"] | None = None,
    ) -> dt.date:
    """오늘 날짜를 반환한다. 일/주 단위 오프셋을 지정할 수 있다."""
    return now(days=days, weeks=weeks, delta=delta, tzinfo=tzinfo, unit=unit).date()


def trunc_datetime(
        datetime: dt.datetime,
        unit: Literal["second", "minute", "hour", "day", "month", "year"] | None = None
    ) -> dt.datetime:
    """지정된 단위로 `datetime` 객체를 절삭한다."""
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
############################# pendulum ############################
###################################################################

def in_timezone(
        datetime: pendulum.DateTime,
        tz: str | Timezone | FixedTimezone | Literal[":default:"] | None = ":default:",
        add: dict | None = None,
        subtract: dict | None = None,
        subdays: int | None = None,
    ) -> pendulum.DateTime:
    """`pendulum.DateTime` 객체의 시간대를 지정하고, `delta`를 더하거나 빼는 연산을 한다.

    별도로 시간대를 지정하지 않았다면 기본으로 한국표준시(KST)를 설정한다."""
    datetime = datetime.in_timezone(DEFAULT_TIMEZONE if tz == ":default:" else tz)
    if add and isinstance(add, dict):
        datetime = datetime.add(**add)
    if subtract and isinstance(subtract, dict):
        datetime = datetime.subtract(**subtract)
    if subdays and isinstance(subdays, int):
        datetime = datetime.subtract(days=subdays)
    return datetime


def format_date(
        datetime: pendulum.DateTime,
        fmt: str = "YYYY-MM-DD",
        locale: str = "ko",
        add: dict | None = None,
        subtract: dict | None = None,
        subdays: int | None = None,
        tz: str | Timezone | FixedTimezone | Literal[":default:"] | None = ":default:",
    ) -> str:
    """`pendulum.DateTime` 객체에 대해 `in_timezone` 연산 후 `fmt` 형식의 문자열로 변환한다."""
    return in_timezone(datetime, tz, add, subtract, subdays).format(fmt, locale)


###################################################################
############################ Time Zone ############################
###################################################################

def get_timezone(tzinfo: BaseTzInfo | str | None = None) -> BaseTzInfo:
    """타임존 문자열을 `BaseTzInfo` 객체로 변환한다."""
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
    """`datetime` 객체에 타임존을 설정하거나 변환한다."""
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
    """연-월을 표현하는 클래스. 비교, 연산, 날짜 변환을 지원한다."""

    def __init__(self, year: int, month: int):
        self.year = year
        self.month = min(max(month, 1), 12)

    def date(self, day: int = 1) -> dt.date:
        """`date` 객체로 변환한다."""
        return dt.date(self.year, self.month, day)

    def eomonth(self) -> dt.date:
        """해당 월의 마지막 날짜를 반환한다."""
        from calendar import monthrange
        return self.date(monthrange(self.year, self.month)[1])

    def _compare(self) -> int:
        """연-월을 정수로 치환한다."""
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
        """연-월을 `YYYY-MM` 포맷의 문자열로 반환한다."""
        return "{}-{}".format(self.year, str(self.month).rjust(2, '0'))


def date_range(
        start: dt.date | str,
        end: dt.date | str,
        freq: Literal["D", "W", "M"] = "D",
        format: str = "%Y-%m-%d",
    ) -> list[dt.date]:
    """시작일부터 종료일까지의 날짜 범위 리스트를 생성한다.

    일/주/월 빈도에 따라 다음과 같은 날짜 범위가 만들어진다.
    - `D`: 일 단위 `date` 객체를 반환한다.
    - `W`: 월요일 기준 주 단위로 `date` 객체를 반환한다.
    - `M`: 매월 1일 기준 월 단위로 `date` 객체를 반환한다."""
    return _generate_date_range(start, end, freq, format, add_one=False)


def date_pairs(
        start: dt.date | str,
        end: dt.date | str,
        freq: Literal["D", "W", "M"] = "D",
        format: str = "%Y-%m-%d",
    ) -> list[tuple[dt.date, dt.date]]:
    """시작일부터 종료일까지의 기간을 (시작일, 종료일) 쌍의 리스트로 분할한다.

    일/주/월 빈도에 따라 다음과 같은 날짜 범위가 만들어진다.
    - `D`: 일 단위로 기간을 분할한다. 분할된 각각의 쌍은 시작일과 종료일이 동일하다.
    - `W`: 월요일 기준 주 단위로 기간을 분할한다. 중간 기간은 (월요일, 일요일) 쌍으로 생성된다.
    - `M`: 매월 1일 기준 월 단위로 기간을 분할한다. 중간 기간은 (1일, 말일) 쌍으로 생성된다."""
    if freq.upper() == "D":
        return [(date, date) for date in _generate_date_range(start, end, "D", format, add_one=False)]
    else:
        ranges = _generate_date_range(start, end, freq, add_one=True)
        return [(ranges[i], ranges[i+1] - dt.timedelta(days=1)) for i in range(len(ranges)-1)]


def date_split(
        start: dt.date | str,
        end: dt.date | str,
        delta: int | dict[Literal["days", "seconds", "microseconds", "milliseconds", "minutes", "hours", "weeks"], float] = 1,
        format: str = "%Y-%m-%d",
    ) -> list[tuple[dt.date, dt.date]]:
    """시작일부터 종료일까지의 기간을 지정된 간격으로 (시작일, 종료일) 쌍의 리스트로 분할한다.

    `delta`가 정수형인 경우 `days`로 인식하며, `timedelta`에 전달할 파라미터를 딕셔너리로 지정할 수도 있다."""
    if isinstance(delta, int):
        delta = {"days": delta}

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
        freq: Literal["D", "W", "M"] = "D",
        format: str = "%Y-%m-%d",
        add_one: bool = False,
    ) -> list[dt.date]:
    """일/주/월 기준으로 날짜 범위를 생성하는 내부 함수."""
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
    """시작값부터 종료값까지 `delta` 간격으로 값 리스트를 생성한다."""
    ranges = list()
    cur = start
    while cur <= end:
        ranges.append(cur)
        cur += delta
    return ranges
