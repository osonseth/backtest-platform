import math
from utils.logger import get_logger
from utils.time_utils import to_datetime, TIMEFRAME_MS

logger = get_logger(__name__)

class CandleValidator:
    """
    Validates OHLCV candle data integrity.
    Checks for null values, OHLCV consistency, and timestamp continuity.
    Maintains state between calls to detect issues across batches.
    """
    def __init__(self):
        self.last_close = None
        self.last_timestamp = None
    
    def validate(self, batch: list, timeframe: str ):
        """
        Validates a batch of OHLCV candles.
        :raises ValueError: If null values or OHLCV inconsistency detected
        :param batch: List of OHLCV candles [[timestamp, open, high, low, close, volume], ...]
        :param timeframe: Time interval (e.g. '1h', '4h', '1d')
        """
        for candle in batch:
            self._check_nulls(candle)
            self._check_ohlcv_consistency(candle)
            self._check_candle_continuity(candle, timeframe)

    def init_from_last_candle(self, last_candle: list):
        """
        Initialize validator state from the last known candle.
        Call this before starting a stream to ensure continuity with existing DB data.
        :param last_candle: Last candle stored in DB [timestamp, open, high, low, close, volume]
        """
        self.last_close = last_candle[4]
        self.last_timestamp = last_candle[0]

    def _check_ohlcv_consistency(self, candle: list):
        timestamp, open, high, low, close, volume = candle
        if low > open or low > high or low > close:
            raise ValueError(f"Inconsistent low value in candle: {candle}")
        if high < open or high < close:
            raise ValueError(f"Inconsistent high value in candle: {candle}")
        if volume < 0:
            raise ValueError(f"Inconsistent volume value in candle: {candle}")
        if self.last_close and self.last_close != open:
            logger.debug(f"Opening is different from the closing of the last candle: {candle}")
        self.last_close = close

    def _check_nulls(self, candle: list):
        for value in candle:
            if value is None or (isinstance(value, float) and math.isnan(value)):
                raise ValueError(f"Null or NaN value detected in candle: {candle}")

    def _check_candle_continuity(self, candle: list, timeframe: str):
        if timeframe != '1M':
            if self.last_timestamp is not None and self.last_timestamp + TIMEFRAME_MS[timeframe] != candle[0]:
                logger.warning( f"Timestamp continuity issue detected:\n"
                                f"Previous timestamp: {self.last_timestamp} - {to_datetime(self.last_timestamp)}\n"
                                f"current timestamps: {candle[0]} - {to_datetime(candle[0])}\n"
                                f"  Current candle: {candle}")
            self.last_timestamp = candle[0]
            

    