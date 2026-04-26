from market_data.indicators.indicator_computer import IndicatorComputer
from market_data.repository.db import Database

if __name__ == "__main__":
    database = Database()
    computer = IndicatorComputer(database)
    asset_id = database.get_asset_id("BTC/USDT")
    tf_id = database.get_timeframe_id("1d")
    computer.compute_rsi_history(asset_id, tf_id)
    
