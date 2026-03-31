import asyncio

from market_data.stream.candle_streamer import CandleStreamer
from utils.time_utils import TIMEFRAME_MS

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