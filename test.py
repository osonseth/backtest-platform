import ccxt
import pandas as pd

exchange = ccxt.binance()
since = exchange.parse8601('2016-03-13T08:00:00Z')
response = exchange.fetch_ohlcv('BTC/USDT','1d', limit=10)


col = ["timestamp", "open", "highest", "lowest", "closing", "vol"]

df = pd.DataFrame(response, columns=col)
print(df)
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
print(df)
