import time
from datetime import datetime, timezone

TIMEFRAME_MS = {
    '1m':  60 * 1000,
    '3m':  3 * 60 * 1000,
    '5m':  5 * 60 * 1000,
    '15m': 15 * 60 * 1000,
    '30m': 30 * 60 * 1000,
    '1h':  60 * 60 * 1000,
    '2h':  2 * 60 * 60 * 1000,
    '4h':  4 * 60 * 60 * 1000,
    '6h':  6 * 60 * 60 * 1000,
    '8h':  8 * 60 * 60 * 1000,
    '12h': 12 * 60 * 60 * 1000,
    '1d':  24 * 60 * 60 * 1000,
    '3d':  3 * 24 * 60 * 60 * 1000,
    '1w':  7 * 24 * 60 * 60 * 1000,
    '1M':  30 * 24 * 60 * 60 * 1000,
}

def to_datetime(timestamp_ms: int) -> datetime:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)

def to_timestamp_ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

def is_near_minute_end(tolerance_sec=1):

    now_ms = int(time.time() * 1000)
    now_dt = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
    sec_in_min = now_dt.second

    return sec_in_min >= 60 - tolerance_sec or sec_in_min <= tolerance_sec