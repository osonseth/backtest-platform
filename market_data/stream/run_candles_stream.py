import asyncio
import json

from market_data.stream.candle_streamer import CandleStreamer

from market_data.broker.binance import BinanceClient
from market_data.repository.db import Database
from market_data.validate.candle_validator import CandleValidator
from utils.time_utils import TIMEFRAME_MS
from utils.candle_utils import normalizes_candle
from utils.candle_utils import format_candles_for_db
from utils.logger import get_logger

logger = get_logger(__name__)


async def main():
    path = "config/initial_streams.json"
    try:
        candlestream = CandleStreamer()
        candlestream.init_and_backfill(path)
        await candlestream.run_streaming()

    except Exception as e:
        logger.error(f"{e}")
        return

if __name__ == "__main__":
    asyncio.run(main())