"""UTC, half-open time ranges shared by database reads and the worker."""
import pandas as pd


def utc_time(value):
    t = pd.Timestamp(value)
    if pd.isna(t) or t.tzinfo is None:
        raise ValueError('Time must include a timezone, e.g. 2026-10-07T06:40:20Z')
    return t.tz_convert('UTC')


def bounds(start, stop, aligned=False):
    start, stop = utc_time(start), utc_time(stop)
    if start >= stop:
        raise ValueError('start must be earlier than stop (stop is exclusive)')
    if aligned and (start != start.floor('10s') or stop != stop.floor('10s')):
        raise ValueError('Worker start/stop must align to 10-second bins')
    return start, stop
