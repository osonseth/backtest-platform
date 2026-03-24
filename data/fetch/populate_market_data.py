import pandas as pd
import ccxt
from datetime import datetime, timezone
from data.broker.binance import BinanceClient
from data.repository.db import Database
from data.validate.candle_validator import CandleValidator
from utils.logger import get_logger

logger = get_logger(__name__)


ASSET = "BTC/USDT"
TIMEFRAME = "1d"

def to_datetime(timestamp_ms: int) -> datetime:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)\
    
def candles_to_tuples(candles: list, asset_id: int, timeframe_id: int) -> list:
    return [(asset_id, timeframe_id, to_datetime(candle[0]), *candle[1:]) for candle in candles]

def main():
  
    timestamp_ms = int(pd.Timestamp('2017-01-01T00:00:00Z').timestamp() * 1000)

    try:
        exchange = BinanceClient()
        database = Database()
        validator = CandleValidator()
        timeframe_id = database.get_timeframe_id(TIMEFRAME)
        asset_id = database.get_or_create_asset_id(ASSET)
        while True:
            candles = exchange.fetch_candles(ASSET, TIMEFRAME, since = timestamp_ms)
            if candles:
                validator.validate(candles, TIMEFRAME)
                timestamp_ms = candles[-1][0] + 1
                candles = candles_to_tuples(candles, asset_id, timeframe_id)
                database.insert_candles(candles)
            if len(candles) < exchange.default_limit:
                break
    except ccxt.ExchangeError as e:
        logger.error(f"{e}")
    except (ccxt.NetworkError, ccxt.RateLimitExceeded) as e:
        logger.error(f"{e}")
    except Exception as e:
        logger.error(f"{e}")

if __name__ == "__main__":
    main()

