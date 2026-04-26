from abc import ABC, abstractmethod

class BrokerBase(ABC):
    """
    Abstract base class defining the common interface for all brokers.
    Each broker must implement fetch_candles().
    connect_stream() is optional depending on the broker.
    """
    def __init__(self, url: str):
        self.url = url

    @abstractmethod
    def fetch_candles(self, asset: str, timeframe: str, start_time: str):
        """
        Fetch OHLCV candles from the broker's REST API.

        :param asset: Trading pair (e.g. 'BTC/USDT')
        :param timeframe: Time interval (e.g. '1h', '4h')
        :param start_time: Start timestamp for the fetch
        """
        pass

    def connect_stream(self):
        """
        Open a WebSocket to receive candles in real time.
        Optional — not implemented by default.
        """
        pass