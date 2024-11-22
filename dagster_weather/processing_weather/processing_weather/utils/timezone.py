from __future__ import annotations

from datetime import datetime, timezone, tzinfo
from typing import Union

import pytz
from dagster import EnvVar

TZType = Union[str, tzinfo]
DEFAULT_TIMEZONE = EnvVar("TIME_ZONE").get_value("UTC")
IST = pytz.timezone("Asia/Kolkata")


def get_timezone(tzname: TZType):
    return pytz.timezone(tzname) if not isinstance(tzname, tzinfo) else tzname


def get_default_timezone():
    return get_timezone(DEFAULT_TIMEZONE)


def to_timezone(dt_obj: datetime, tzname: TZType):
    tzinfo = get_timezone(tzname)
    return dt_obj.astimezone(tzinfo)


def now(tzname: TZType | None = DEFAULT_TIMEZONE):
    """Returns current datetime object in default timezone.
    Set tzname=None for UTC timezone
    """
    now_utc = datetime.now(timezone.utc)
    return to_timezone(now_utc, tzname) if tzname else now_utc


def is_aware(d: datetime):
    return d.tzinfo is not None and d.tzinfo.utcoffset(d) is not None


def is_naive(d: datetime):
    return not is_aware(d)
