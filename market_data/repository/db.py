import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from datetime import datetime, timezone
from utils.logger import get_logger

logger = get_logger(__name__)

load_dotenv()

class Database:
    """
    Handles all database interactions with PostgreSQL.
    Provides methods to query and insert market data, assets, timeframes, and candles.
    """
    def __init__(self):
        self.host = os.getenv("DB_HOST")
        self.port = os.getenv("DB_PORT")
        self.name = os.getenv("DB_NAME")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        try:
            self.conn = psycopg2.connect(
                dbname=self.name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
        except psycopg2.OperationalError as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def get_or_create_asset_id(self, asset: str) -> int:
        """
        Get the id of an asset, creating it if it doesn't exist.
        :param asset: asset's name
        :return: asset's id
        """
        query_select = "SELECT id FROM asset WHERE name = %s"
        query_insert = "INSERT INTO asset (name) VALUES (%s) RETURNING id"
        with self.conn.cursor() as cur:
            cur.execute(query_select, (asset,))
            id = cur.fetchone()
            if id is None:
                cur.execute(query_insert, (asset,))
                id = cur.fetchone()
                self.conn.commit()
            return id[0]

    def get_asset_id(self, asset: str) -> int:
        """
        get id of asset in database.
        :param asset: asset' s name
        :return : asset' s id
        :raises KeyError: if the asset is not found in database
        """ 
        query = "SELECT id FROM asset WHERE name = %s"
        with self.conn.cursor() as cur:
            cur.execute(query, (asset,))
            id = cur.fetchone()
            if id is None:
                raise KeyError("Asset was not found in the database")
            return id[0]

    def get_timeframe_id(self, timeframe: str) -> int:
        """
        get id of timeframe in database.
        :param timeframe: timeframe' s name
        :return : timeframe' s id
        :raises KeyError: if the timeframe is not found in database
        """ 
        query = "SELECT id FROM timeframe WHERE label = %s"
        with self.conn.cursor() as cur:
            cur.execute(query, (timeframe,))
            id = cur.fetchone()
            if id is None:
                raise KeyError("Timeframe was not found in the database")
            return id[0]



    def insert_candles(self, batch: list) -> None:
        """
        Insert the batch of candles in the database
        :param batch : list of tuples [(asset_id, timeframe_id, timestamp, open, high, low, close, volume), ...]
        """
        query = """
        INSERT INTO market_data (asset_id, timeframe_id, open_time, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (asset_id, timeframe_id, open_time) DO NOTHING
        """
        with self.conn.cursor() as cur:
            execute_values(
                cur, 
                query,
                batch
            )
            self.conn.commit()

    def insert_candle_stream(self, candle: list ) -> None:
        """
        Insert a single candle from the WebSocket stream into the database.
        Uses ON CONFLICT DO UPDATE to overwrite any incomplete candle previously inserted by populate.
        :param candle: List containing a single tuple [(asset_id, timeframe_id, open_time, open, high, low, close, volume)]
        """
        query = """
        INSERT INTO market_data (asset_id, timeframe_id, open_time, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (asset_id, timeframe_id, open_time) DO UPDATE SET 
        open=EXCLUDED.open, 
        high=EXCLUDED.high, 
        low=EXCLUDED.low, 
        close=EXCLUDED.close, 
        volume=EXCLUDED.volume
        """
        with self.conn.cursor() as cur:
            execute_values(
                cur, 
                query,
                candle
            )
            self.conn.commit()

    def get_last_candle_timestamp(self, asset_id: int, timeframe_id: int) -> datetime:
        """
        Retrieve the timestamp of the most recent candle for a given asset and timeframe.
        :param asset_id: Database ID of the asset
        :param timeframe_id: Database ID of the timeframe
        :return: Datetime of the most recent candle's open_time
        :raises LookupError: If no candle is found for the given asset and timeframe
        """
        query = """
        SELECT open_time FROM market_data WHERE asset_id = %s AND timeframe_id = %s ORDER BY open_time DESC LIMIT 1
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (asset_id, timeframe_id))
            timestamp = cur.fetchone()
            if timestamp is None:
                raise LookupError(f"No candle found for asset_id={asset_id} and timeframe_id={timeframe_id}")
            return timestamp[0].replace(tzinfo=timezone.utc)
        
    def get_candles_for_aggregation(self, limit: int, asset_id: int, timeframe_id: int) -> list:
        query = """
        SELECT asset_id, timeframe_id, open_time, open, high, low, close, volume
        FROM market_data
        WHERE asset_id = %s AND timeframe_id = %s
        ORDER BY open_time DESC
        LIMIT %s
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (asset_id, timeframe_id, limit))
            candles = cur.fetchall()
            if not candles:
                raise LookupError(f"No candles found for aggregation with asset_id={asset_id}")
            return candles
        
    def get_candles(self, asset_id: int, timeframe_id: int) -> list:
        query = """
        SELECT id, asset_id, timeframe_id, open_time, open, high, low, close, volume
        FROM market_data
        WHERE asset_id = %s AND timeframe_id = %s
        ORDER BY open_time ASC
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (asset_id, timeframe_id))
            candles = cur.fetchall()
            if not candles:
                raise LookupError(f"No candles found with asset_id={asset_id}")
            return candles