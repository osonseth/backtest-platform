import asyncio
import json
from data.broker.binance import BinanceClient
from data.repository.db import Database
from data.validate.candle_validator import CandleValidator TIMEFRAME_MS
from utils.candle_utils import normalizes_candle
from utils.candle_utils import format_candles_for_db
from utils.logger import get_logger

logger = get_logger(__name__)

def backfill(asset_ids: dict, timeframe_id: int, last_seen_ts: dict, database: Database, exchange: BinanceClient) -> None:

    for asset, asset_id in asset_ids.items():
        validator = CandleValidator()

        db_last_ts = database.get_last_candle_timestamp(asset_id, timeframe_id)
        fetch_since_ms = int(db_last_ts.timestamp() * 1000) + 1

        while True:
            candles = exchange.fetch_candles(asset, "1m", since=fetch_since_ms)

            if candles:
                validator.validate(candles, "1m")

                last_candle_ts = candles[-1][0]
                fetch_since_ms = last_candle_ts + 1

                last_seen_ts[asset_id] = last_candle_ts

                candles = format_candles_for_db(candles, asset_id, timeframe_id)
                database.insert_candles(candles)

            if len(candles) < exchange.default_limit:
                break

async def main():
    path = "config/initial_streams.json"
    try:
        exchange = BinanceClient()
        database = Database()

        with open(path) as f:
            data = json.load(f)

        last_seen_ts = {}  
        timeframe_id = database.get_timeframe_id("1m")
        asset_ids = {asset: database.get_asset_id(asset) for asset in data["assets"]}

        backfill(asset_ids, timeframe_id, last_seen_ts, database, exchange)

    except Exception as e:
        logger.error(f"{e}")

    while True:
    
        try:
            async for msg in exchange.connect_stream(data["assets"]):
                data = json.loads(msg)
                candle = data["data"]["k"]
        
                if candle["x"]:
                    stream_symbol = candle["s"].lower()
                    asset = exchange.ws_symbol_map[stream_symbol]
                    asset_id = asset_ids[asset]

                    candle = normalizes_candle(candle)
                    
                    last_ts = last_seen_ts.get(asset_id, 0)
                    if candle[0] <= last_ts:
                        continue
                    if candle[0] > last_ts + 60000:
                        backfill({asset: asset_id}, timeframe_id, last_seen_ts, database, exchange)
                    
                    last_seen_ts[asset_id] = candle[0]
                    candle = format_candles_for_db([candle], asset_id, timeframe_id)
                    database.insert_candle_stream(candle)
                
        except Exception as e:
            logger.error(f"{e}")

if __name__ == "__main__":
    asyncio.run(main())