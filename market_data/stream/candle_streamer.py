import json
import asyncio
import time
from market_data.broker.binance import BinanceClient
from market_data.repository.db import Database
from market_data.validate.candle_validator import CandleValidator
from utils.candle_utils import format_candles_for_db, normalizes_candle
from utils.time_utils import to_timestamp_ms, is_near_minute_end, TIMEFRAME_MS
from utils.logger import get_logger

logger = get_logger(__name__)

class CandleStreamer:
    def __init__(self):
        self.exchange = BinanceClient()
        self.database = Database()
        self.last_seen_ts = {}   
        self.timeframes_id = {}
        self.asset_ids = {}


    def _load_config(self, config_path: str)-> list:
        with open(config_path) as f:
            data = json.load(f)
        return data["assets"]

    def init_and_backfill(self, config_path: str)-> None:
        assets = self._load_config(config_path)

        self.timeframes_id = { 
            timeframe: self.database.get_timeframe_id(timeframe)
            for timeframe in TIMEFRAME_MS.keys()
        }

        self.asset_ids = {
            asset: self.database.get_asset_id(asset)
            for asset in assets
        }

        self._backfill()

        for asset_id in self.asset_ids.values():
            try:
                last_ts = self.database.get_last_candle_timestamp(
                    asset_id,
                    self.timeframes_id["1m"]
                )
                self.last_seen_ts[asset_id] = to_timestamp_ms(last_ts)
            except LookupError:
                self.last_seen_ts[asset_id] = 0


    def _backfill(self, asset_ids=None) -> None:

            if asset_ids is None:
                asset_ids = self.asset_ids

            for asset, asset_id in asset_ids.items():
                for timeframe, timeframe_id in self.timeframes_id.items():
                    validator = CandleValidator()

                    try:
                        db_last_ts = self.database.get_last_candle_timestamp(asset_id, timeframe_id)
                        fetch_since_ms = int(db_last_ts.timestamp() * 1000)
                    except LookupError as e:
                        logger.warning(f"{e}")
                        fetch_since_ms = self.exchange.start_timestamp
    
                    while True:

                        candles = self.exchange.fetch_candles(asset, timeframe, since=fetch_since_ms)
        
                        if candles:
                            validator.validate(candles, timeframe)

                            last_candle_ts = candles[-1][0]
                            fetch_since_ms = last_candle_ts + 1
                            if timeframe == "1m":
                                self.last_seen_ts[asset_id] = last_candle_ts

                            candles = format_candles_for_db(candles, asset_id, timeframe_id)
                            self.database.insert_candle_stream(candles)

                        if len(candles) < self.exchange.default_limit:
                            break
        
    async def run_streaming(self)-> None:
        
        attempt_time_sec = 30
        connected = False
        if is_near_minute_end():
            time.sleep(2)
            self._backfill()
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
                        logger.critical(f"asset str = {asset}")
                        asset_id = self.asset_ids[asset]
                        logger.critical(f"asset_id = {asset_id}")
                        candle = normalizes_candle(candle)
                        logger.critical(f" normalized candle stream = {candle}")
                        last_ts = self.last_seen_ts.get(asset_id, 0)
                        if candle[0] < last_ts:
                            continue
                        if candle[0] > last_ts + TIMEFRAME_MS["1m"]:
                            logger.critical("BACKFILL LIGNE 101 de candle_streamer.py")
                            logger.critical(f"candle [0] = {candle[0]} last_ts = {last_ts} ET last_ts + TIMEFRAME_MS = {last_ts + TIMEFRAME_MS['1m']}")
                            self._backfill({asset: asset_id})
                
                        self.last_seen_ts[asset_id] = candle[0]
                        candle = format_candles_for_db([candle], asset_id, self.timeframes_id["1m"])
                        self.database.insert_candle_stream(candle)
                        self._update_higher_timeframes(asset_id)
            except Exception as e:
                logger.error(f"{e}")
                connected = False
                await asyncio.sleep(attempt_time_sec)
                if attempt_time_sec < 60 * 60:
                    attempt_time_sec *= 2

    def _update_higher_timeframes (self, asset_id: int):
        for timeframe in list(TIMEFRAME_MS.keys())[1:]:
            if (self.last_seen_ts[asset_id] + TIMEFRAME_MS["1m"]) % TIMEFRAME_MS[timeframe] == 0:
                #--------------------------------------------
                logger.critical(f"UPDATE HIGHTER TIMEFRAMES -> Asset_id = {asset_id}, timeframe = {timeframe}, last_seen_ts = {self.last_seen_ts[asset_id]}")
                #--------------------------------------------
                aggregate_candle = self._aggregate_candle(timeframe, asset_id)
                logger.critical(f"AGGREGATED CANDLE -> {aggregate_candle}")
                self.database.insert_candle_stream(aggregate_candle)


    def _aggregate_candle(self, timeframe: str, asset_id: int):
        n = TIMEFRAME_MS[timeframe] // TIMEFRAME_MS["1m"]
        #--------------------------------------------
        logger.critical(f"AGGREGATE CANDLES -> timeframe = {timeframe}, n = {n}")
        #--------------------------------------------
        candles = self.database.get_candles_for_aggregation(n, asset_id, self.timeframes_id["1m"])
        #--------------------------------------------
        logger.critical(f"AGGREGATE CANDLES -> candles = {candles}")
        #--------------------------------------------
        candles = [(*candle[:2], to_timestamp_ms(candle[2]), *candle[3:])
                    for candle in candles]
        open_time = candles[-1][2]
        open_price = candles[-1][3]
        high = max(candle[4] for candle in candles)
        low = min(candle[5] for candle in candles)
        close_price = candles[0][6]
        volume = sum(candle[7] for candle in candles)
        return format_candles_for_db([[open_time, open_price, high, low, close_price, volume]], asset_id, self.timeframes_id[timeframe]) 