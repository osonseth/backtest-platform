import pandas as pd
import ccxt
from data.broker.binance import BinanceClient
from utils.logger import get_logger

logger = get_logger(__name__)


ASSET = "BTC/USDT"
TIMEFRAME = "1d"
def main():
    exchange = BinanceClient()
    timestamp_ms = int(pd.Timestamp('2017-01-01T00:00:00Z').timestamp() * 1000)
    try:
        while True:
            candles = exchange.fetch_candles(ASSET, TIMEFRAME, since = timestamp_ms)
            #validate candle
            #insert candle
            timestamp_ms = candles[-1][0] + 1
            if len(candles) < 1000:
                break
    except ccxt.ExchangeError as e:
        logger.error(f"{e}")
    except (ccxt.NetworkError, ccxt.RateLimitExceeded) as e:
        logger.error(f"{e}")
    except Exception as e:
        logger.error(f"{e}")

if __name__ == "__main__":
    main()

