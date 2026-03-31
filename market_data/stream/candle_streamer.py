import json
import asyncio
from market_data.broker.binance import BinanceClient
from market_data.repository.db import Database
from market_data.validate.candle_validator import CandleValidator
from utils.candle_utils import format_candles_for_db, normalizes_candle
from utils.time_utils import TIMEFRAME_MS
from utils.logger import get_logger

logger = get_logger(__name__)



class CandleStreamer:
    def __init__(self):
        self.exchange = BinanceClient()
        self.database = Database()
        self.last_seen_ts = {}   
        self.timeframe_id = None 
        self.asset_ids = None


    def _load_config(self, config_path: str)-> list:
        with open(config_path) as f:
            data = json.load(f)
        return data["assets"]

    def init_and_backfill(self, config_path: str)-> None:
        assets = self._load_config(config_path)

        self.timeframe_id = self.database.get_timeframe_id("1m")
        self.asset_ids = {
            asset: self.database.get_asset_id(asset)
            for asset in assets
        }

        self._backfill()

    def _backfill(self, asset_ids=None) -> None:
        if asset_ids is None:
            asset_ids = self.asset_ids

        for asset, asset_id in asset_ids.items():
            validator = CandleValidator()

            db_last_ts = self.database.get_last_candle_timestamp(asset_id, self.timeframe_id)
            fetch_since_ms = int(db_last_ts.timestamp() * 1000) + 1
  
            while True:
                candles = self.exchange.fetch_candles(asset, "1m", since=fetch_since_ms)
   
                if candles:
                    validator.validate(candles, "1m")

                    last_candle_ts = candles[-1][0]
                    fetch_since_ms = last_candle_ts + 1

                    self.last_seen_ts[asset_id] = last_candle_ts

                    candles = format_candles_for_db(candles, asset_id, self.timeframe_id)
                    self.database.insert_candles(candles)

                if len(candles) < self.exchange.default_limit:
                    break
    
    async def run_streaming(self)-> None:
        
        attempt_time_sec = 30
        connected = False
        while True:
            try:
                async for msg in self.exchange.connect_stream(list(self.asset_ids)):
                    if connected is False:
                        connected = True
                        attempt_time_sec = 30
                    data = json.loads(msg)
                    candle = data["data"]["k"]

                    if candle["x"]:
                        stream_symbol = candle["s"].lower()
                        asset = self.exchange.ws_symbol_map[stream_symbol]
                        asset_id = self.asset_ids[asset]

                        candle = normalizes_candle(candle)
                        
                        last_ts = self.last_seen_ts.get(asset_id, 0)
                        if candle[0] < last_ts:
                            continue
                        if candle[0] > last_ts + TIMEFRAME_MS["1m"]:
                            self._backfill({asset: asset_id})
                        
                        self.last_seen_ts[asset_id] = candle[0]
                        candle = format_candles_for_db([candle], asset_id, self.timeframe_id)
                        self.database.insert_candle_stream(candle)
                    
            except Exception as e:
                logger.error(f"{e}")
                connected = False
                await asyncio.sleep(attempt_time_sec)
                if attempt_time_sec < 60 * 60:
                    attempt_time_sec *= 2