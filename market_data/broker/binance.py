import ccxt
import time
import websockets
from typing import AsyncGenerator
from market_data.broker.base import BrokerBase
from utils.logger import get_logger

logger = get_logger(__name__)

class BinanceClient(BrokerBase):
    """class for communicating with the Binance API."""

    def __init__(self):
        super().__init__('https://api.binance.com')
        self.exchange = ccxt.binance({'enableRateLimit': True})
        self.start_timestamp = self.exchange.parse8601('2017-01-01T00:00:00Z')
        self.default_limit = 1000
        self.ws_base_url = "wss://stream.binance.com:9443/stream?streams="
        self.ws_stream_url = ""
        self.ws_symbol_map = {}

    def fetch_candles(self, asset: str, timeframe: str, limit: int = None, since: int = None)-> list[list]:
        """
        Fetch OHLCV candles from the Binance REST API.
        :param asset: Trading pair (e.g. 'BTC/USDT')
        :param timeframe: Time interval (e.g. '1h', '4h', '1d')
        :param limit: Max number of candles to fetch (default 1000)
        :param since: Start timestamp in milliseconds (default 2017-01-01)
        :return: List of OHLCV candles [[timestamp, open, high, low, close, volume], ...]
        """
        max_retries = 3
        if since is None:
            since = self.start_timestamp
        if limit is None:
            limit = self.default_limit
        logger.info(f"start fetch candle: asset = {asset}, timeframe = {timeframe}")
        for attempt in range(max_retries):
            try:
                response = self.exchange.fetch_ohlcv(asset, timeframe, since = since, limit = limit)
                logger.info(f"fetched {len(response)} candles")
                break
            except ccxt.ExchangeError as e:
                raise
            except (ccxt.NetworkError, ccxt.RateLimitExceeded) as e:
                wait = 2 ** attempt
                if attempt == max_retries - 1:
                    logger.warning(f"NetworkError attempt {attempt + 1}/{max_retries}: {e}. Max retries reached.")
                    raise
                logger.warning(f"NetworkError attempt {attempt + 1}/{max_retries}: {e}. Retrying in {wait}s...")
                time.sleep(wait)
            except Exception as e:
                raise
        return response
    
    def _build_assets_symbols_streams_map (self, assets: list)-> None:
        """
        Build a mapping from WebSocket symbol format to standard asset format.
        Stores the result in self.ws_symbol_map.
        :param assets: List of asset names in standard format (e.g. ['BTC/USDT', 'ETH/USDT'])
        """
        self.ws_symbol_map = {asset.replace('/', '').lower(): asset for asset in assets}

    def _build_stream_url(self, assets: list)-> None:
        """
        Build the WebSocket combined stream URL for all given assets.
        Stores the result in self.ws_stream_url.
        :param assets: List of asset names in standard format (e.g. ['BTC/USDT', 'ETH/USDT'])
        """
        streams = [asset.replace('/', '').lower() + "@kline_1m" for asset in assets]
        self.ws_stream_url = self.ws_base_url + '/'.join(streams)

    async def connect_stream(self, assets: list)-> AsyncGenerator[str, None]:
        """
        Open a WebSocket connection to Binance and yield raw messages as they arrive.
        Subscribes to 1m kline streams for all given assets via a combined stream.
        :param assets: List of asset names in standard format (e.g. ['BTC/USDT', 'ETH/USDT'])
        :return: Async generator yielding raw JSON messages as strings
        """
        self._build_assets_symbols_streams_map(assets)
        self._build_stream_url(assets)
        async with websockets.connect(self.ws_stream_url) as ws:
            while True:
                msg = await ws.recv()
                yield msg

    