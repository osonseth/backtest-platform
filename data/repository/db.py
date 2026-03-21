import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from utils.logger import get_logger

logger = get_logger(__name__)

load_dotenv()

class Database:

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
        query = "SELECT id FROM timeframe WHERE name = %s"
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
 

