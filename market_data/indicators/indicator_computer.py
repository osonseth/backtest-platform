import talib
import numpy as np

from market_data.repository.db import Database

class IndicatorComputer ():


    def __init__(self, database: Database):
       self.database = database
    
    def compute_rsi_history (self, asset_id: int, timeframe_id: int):
        candles = self.database.get_candles(asset_id, timeframe_id)
     
        closes = np.array([candle[7] for candle in candles])


        rsi_values = np.round(talib.RSI(closes, timeperiod=14), 2)
        for c in candles:
            print(c)
        for x in rsi_values:
            print(x)
        test = [(candles[i][0], float(val)) for i, val in enumerate(rsi_values)]

        for x in test:
            print(x)
        for i, rsi_val in enumerate(rsi_values):
            if not np.isnan(rsi_val):
                candle_id = candles[i][0]
            