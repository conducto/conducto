import re

_nanosecond_size = 1
_microsecond_size = 1000 * _nanosecond_size
_millisecond_size = 1000 * _microsecond_size
_second_size = 1000 * _millisecond_size
_minute_size = 60 * _second_size
_hour_size = 60 * _minute_size
_day_size = 24 * _hour_size
_week_size = 7 * _day_size
_month_size = 30 * _day_size
_year_size = 365 * _day_size

units = {
    "ns": _nanosecond_size,
    "us": _microsecond_size,
    "µs": _microsecond_size,
    "μs": _microsecond_size,
    "ms": _millisecond_size,
    "s": _second_size,
    "m": _minute_size,
    "h": _hour_size,
    "d": _day_size,
    "w": _week_size,
    "mm": _month_size,
    "y": _year_size,
}

# NOTE: THIS MUST STAY IN SYNC WITH private/services/app/src/utils/duration.js
def duration_string(duration) -> float:
    """Parse a duration string to number of seconds, as a float"""

    if duration in ("0", "+0", "-0"):
        return 0

    pattern = re.compile(r"([\d.]+)([a-zµμ]+)")
    matches = pattern.findall(duration)
    if not len(matches):
        raise ValueError("Invalid duration {}".format(duration))

    total = 0

    for (value, unit) in matches:
        if unit not in units:
            raise ValueError("Unknown unit {} in duration {}".format(unit, duration))
        try:
            total += float(value) * units[unit]
        except Exception:
            raise ValueError("Invalid value {} in duration {}".format(value, duration))

    return total / _second_size
