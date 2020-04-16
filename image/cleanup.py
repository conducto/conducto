from datetime import datetime, timedelta, timezone
import re
import typing

import conducto as co
from conducto.shared import async_utils

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


async def cleanup(label, age):
    image_ids = await _get_image_ids(label)
    last_use_times = await _get_last_use_times(image_ids)
    age_seconds = _parse_age(age)
    cutoff = datetime.now().astimezone(timezone.utc) - timedelta(seconds=age_seconds)

    to_delete = [img for img, tm in last_use_times.items() if tm < cutoff]

    print("Deleting unused images:", to_delete)

    # Set stop_on_error=False because some images cannot be deleted and that's okay.
    # This can happen if some containers are still running, or if they're basic images
    # like 'python' or 'alpine' that are used by images outside of this label.
    if to_delete:
        await async_utils.run_and_check(
            "docker", "image", "rm", *to_delete, stop_on_error=False
        )


def _parse_age(duration) -> float:
    """Parse a duration string to number of seconds, as a float"""

    if duration in ("0", "+0", "-0"):
        return 0

    pattern = re.compile(r"([\d.]+)([a-zµμ]+)")
    matches = pattern.findall(duration)
    if not len(matches):
        raise ValueError("Invalid duration {}".format(duration))

    total = 0
    sign = -1 if duration[0] == "-" else 1

    for (value, unit) in matches:
        if unit not in units:
            raise ValueError("Unknown unit {} in duration {}".format(unit, duration))
        try:
            total += float(value) * units[unit]
        except Exception:
            raise ValueError("Invalid value {} in duration {}".format(value, duration))

    return sign * total / _second_size


async def _get_image_ids(label):
    out, _err = await async_utils.run_and_check(
        "docker", "image", "ls", "--format", "{{.ID}}", "--filter", f"label={label}"
    )
    return out.decode().splitlines()


async def _get_last_use_times(image_ids) -> typing.Dict[str, datetime]:
    out, _err = await async_utils.run_and_check(
        "docker",
        "image",
        "inspect",
        "--format",
        "{{.Metadata.LastTagTime}}",
        *image_ids,
    )
    output = {}
    for image_id, tm_str in zip(image_ids, out.decode().splitlines()):
        without_nanos = re.sub("\.\d+", "", tm_str)
        dt = datetime.strptime(without_nanos, "%Y-%m-%d %H:%M:%S %z %Z")
        output[image_id] = dt.astimezone(timezone.utc)
    return output


if __name__ == "__main__":
    co.main(default=cleanup)
