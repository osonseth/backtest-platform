import ccxt
import pandas as pd
from base import BrokerBase

class BinanceClient(BrokerBase):
    CANDLE_COLUMNS = ["timestamp", "open", "highest", "lowest", "closing", "vol"]

    def __init__(self):
        super().__init__('https://api.binance.com')
        self.exchange = ccxt.binance()
    
    def fetch_candles(self, asset: str, timeframe: str, since: int = None):
        all_candles = []
        if since is None:
            since = self.exchange.parse8601('2017-08-16T00:00:00Z')
        while True:
            response = self.exchange.fetch_ohlcv(asset,timeframe, since = since,)
            all_candles.extend(response)
            if len(response) < 1000:
                break
            if timeframe == '1M':
                since = pd.Timestamp(all_candles[-1][0], unit='ms') + pd.DateOffset(months=1)
                since = int(since.timestamp() * 1000)
            else:
                since = all_candles[-1][0] + self.exchange.parse_timeframe(timeframe) * 1000
        df = pd.DataFrame(all_candles, columns=self.CANDLE_COLUMNS)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        print(df)
        df.to_csv('test_candles.csv', index=False)