from binance import BinanceClient
import pandas as pd

ASSET = "BTC/USDT"
TIMEFRAME = "1d"
def main():
    exchange = BinanceClient()
    timestamp_ms = int(pd.Timestamp('2017-01-01T00:00:00Z').timestamp() * 1000)
    while True:
        candles = exchange.fetch_candles(ASSET, TIMEFRAME, since = timestamp_ms)
        #validate candle
        #insert candle
        timestamp_ms = candles[-1][0] + 1
        if len(candles) < 1000:
            break

if __name__ == "__main__":
    main()

