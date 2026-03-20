import ccxt
import time
from base import BrokerBase
from utils.logger import get_logger

logger = get_logger(__name__)

class BinanceClient(BrokerBase):
    """class for communicating with the Binance API."""

    def __init__(self):
        super().__init__('https://api.binance.com')
        self.exchange = ccxt.binance({'enableRateLimit': True})
        self.start_timestamp = self.exchange.parse8601('2017-01-01T00:00:00Z')
        self.default_limit = 1000

    def fetch_candles(self, asset: str, timeframe: str, limit: int = None, since: int = None)-> list[list]:
        """
        Fetch OHLCV candles from the Binance REST API.
        :param asset: Trading pair (e.g. 'BTC/USDT')
        :param timeframe: Time interval (e.g. '1h', '4h', '1d')
        :param limit: Max number of candles to fetch (default 1000)
        :param since: Start timestamp in milliseconds (default 2017-01-01)
        :return: List of OHLCV candles [[timestamp, open, high, low, close, volume], ...]
        """
        max_retries = 3
        if since is None:
            since = self.start_timestamp
        if limit is None:
            limit = self.default_limit
        for attempt in range(max_retries):
            try:
                response = self.exchange.fetch_ohlcv(asset, timeframe, since = since, limit = limit)
                break
            except ccxt.ExchangeError as e:
                logger.error(f"ExchangeError: {e}")
                raise
            except (ccxt.NetworkError, ccxt.RateLimitExceeded) as e:
                wait = 2 ** attempt
                if attempt == max_retries - 1:
                    logger.warning(f"NetworkError attempt {attempt + 1}/{max_retries}: {e}. Max retries reached.")
                    raise
                logger.warning(f"NetworkError attempt {attempt + 1}/{max_retries}: {e}. Retrying in {wait}s...")
                time.sleep(wait)
            except Exception as e:
                logger.error(f"Exception:{e}")
                raise
        return response
    