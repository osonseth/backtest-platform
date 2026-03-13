import ccxt
from base import BrokerBase
import time

class BinanceClient(BrokerBase):

    def __init__(self):
        super().__init__('https://api.binance.com')
        self.exchange = ccxt.binance({'enableRateLimit': True})
        self.start_timestamp = self.exchange.parse8601('2017-08-16T00:00:00Z')
        self.default_limit = 1000

    def fetch_candles(self, asset: str, timeframe: str, limit: int = None, since: int = None)-> list[list]:
        max_retries = 3
        if since is None:
            since = self.start_timestamp
        if limit is None:
            limit = self.default_limit
        for attempt in range(max_retries):
            try:
                response = self.exchange.fetch_ohlcv(asset, timeframe, since = since, limit = limit)
                break
            except ccxt.ExchangeError:
                # logger
                raise
            except (ccxt.NetworkError, ccxt.RateLimitExceeded):
                if attempt == max_retries - 1:
                    # logger et stopper
                    raise
                wait = 2 ** attempt
                time.sleep(wait)
            except Exception:
                #logger
                raise
        return response
    