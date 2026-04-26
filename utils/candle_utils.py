from utils.time_utils import to_datetime

def format_candles_for_db(candles: list, asset_id: int, timeframe_id: int) -> list:
    """
    Formats raw OHLCV candles into tuples ready for database insertion.
    Prepends asset_id and timeframe_id, and converts the timestamp from milliseconds to datetime.
    :param candles : List of OHLCV candles [[timestamp, open, high, low, close, volume], ...]
    :param asset_id: Database ID of the asset
    :param timeframe_id: Database ID of the timeframe
    :return : List of  tuples OHLCV candles [(asset_id, timeframe_id, timestamp, open, high, low, close, volume), ...]
    """
    return [(asset_id, timeframe_id, to_datetime(candle[0]), *candle[1:]) for candle in candles]

def normalizes_candle(k: dict) -> list:
    """
    Normalize a raw Binance WebSocket kline dict into a standard OHLCV list.
    Converts string values to float and timestamp to int.
    :param k: Raw kline dict from Binance WebSocket message (data["data"]["k"])
    :return: List [timestamp_ms, open, high, low, close, volume]
    """
    timestamp = int(k["t"])
    open = float(k["o"])
    high = float(k["h"])
    low = float(k["l"])
    close = float(k["c"])
    volume = float(k["v"])
    return [timestamp, open, high, low, close, volume]