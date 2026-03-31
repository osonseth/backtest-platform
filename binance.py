import ccxt

exchange = ccxt.binance({'enableRateLimit': True})

timeframes = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']

for tf in timeframes:
    candles = exchange.fetch_ohlcv('BTC/USDT', tf, limit=2)
    for c in candles:
        print(f"{tf:4} | open_time={c[0]} | open={c[1]} | high={c[2]} | low={c[3]} | close={c[4]} | volume={c[5]}")
    print()