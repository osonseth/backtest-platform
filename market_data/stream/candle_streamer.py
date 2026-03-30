import json
from market_data.broker.binance import BinanceClient
from market_data.repository.db import Database


class CandleStreamer:
    def __init__(self):
        self.exchange = BinanceClient()
        self.database = Database()
        self.last_seen_ts = {}   
        self.timeframe_id = None 
        self.asset_ids = None

    def init_and_backfill(self, config_path: str) -> None:
        with open(config_path) as f:
            return json.load(f)
        self.timeframe_id = self.database.get_timeframe_id("1m")
        self.asset_ids = {asset: self.database.get_asset_id(asset) for asset in data["assets"]}

       