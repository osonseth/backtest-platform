import pandas as pd
import argparse
import json
from market_data.broker.binance import BinanceClient
from market_data.repository.db import Database
from market_data.validate.candle_validator import CandleValidator
from utils.candle_utils import format_candles_for_db
from utils.logger import get_logger

logger = get_logger(__name__)


def argsParser() -> argparse.Namespace:
    """
    Parse and validate command-line arguments.
    Accepts either a JSON config file (-f) or a direct asset/timeframes pair (-a/-t), but not both.
    An optional start date (-s) can be provided, defaulting to 2017-01-01.
    :return: Parsed arguments as a Namespace object
    :raises SystemExit: If arguments are invalid or mutually exclusive rules are violated
    """
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-f", dest="file")
    group.add_argument("-a", dest="asset")

    parser.add_argument("-t", dest="timeframes", nargs="+")
    parser.add_argument("-s", dest="since", default="2017-01-01")
    args = parser.parse_args()
    if args.file and args.timeframes:
        parser.error("-f should not be used with -t")
    elif args.asset and not args.timeframes:
        parser.error("-a requires -t")
    return args

def parse_file(path: str) -> list:
    """
    Parse a JSON config file and return a list of (asset, timeframe) pairs.
    :param path: Path to the JSON config file
    :return: List of tuples [(asset, timeframe), ...]
    :raises FileNotFoundError: If the file does not exist
    """
    with open(path) as f:
        data = json.load(f)
    return [(pair["asset"], timeframe) for pair in data["asset_timeframes"] for timeframe in pair["timeframes"]]

def parse_args(asset: str, timeframes: list) -> list:
    """
    Build a list of (asset, timeframe) pairs from direct command-line arguments.
    :param asset: Trading pair (e.g. 'BTC/USDT')
    :param timeframes: List of timeframe labels (e.g. ['1d', '4h'])
    :return: List of tuples [(asset, timeframe), ...]
    """
    return [(asset, timeframe) for timeframe in timeframes]

def main():
    """
    Entry point for the market data population script.
    Parses arguments, fetches OHLCV candles from Binance for each (asset, timeframe) pair,
    validates them, and inserts them into the database.
    Supports resuming from a specific date via the -s argument.
    """
    args = argsParser()
    try:
        if args.file:
            asset_timeframe = parse_file(args.file)
        else:
            asset_timeframe = parse_args(args.asset, args.timeframes)

        exchange = BinanceClient()
        database = Database()

        for asset, timeframe in asset_timeframe:
            validator = CandleValidator()
            timestamp_ms = int(pd.Timestamp(args.since, tz="UTC").timestamp() * 1000)
            timeframe_id = database.get_timeframe_id(timeframe)
            asset_id = database.get_or_create_asset_id(asset)
            while True:
                candles = exchange.fetch_candles(asset, timeframe, since = timestamp_ms)
                if candles:
                    validator.validate(candles, timeframe)
                    timestamp_ms = candles[-1][0] + 1
                    candles = format_candles_for_db(candles, asset_id, timeframe_id)
                    database.insert_candles(candles)
                if len(candles) < exchange.default_limit:
                    break

    except Exception as e:
        logger.error(f"{e}")

if __name__ == "__main__":
    main()

