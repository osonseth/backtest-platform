
import os
import ccxt
import psycopg2
from itertools import zip_longest
from datetime import timezone
from time import time
from dotenv import load_dotenv
import traceback
from utils.time_utils import TIMEFRAME_MS
load_dotenv()


def get_conn():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )


SINCE_UTC = "2026-04-01T01:00:00Z"

exchange = ccxt.binance({'enableRateLimit': True})
since_ms = int(exchange.parse8601(SINCE_UTC))


assets = ['BTC/USDT']
check_tfs = ['1m', '3m', '5m', '15m', '30m', '1h', '2h']

asset_id = 1
timeframe_id = 1

def is_closed_candle(binance_ts: int, timeframe: str) -> bool:
    now_ms = int(time() * 1000)
    tf_ms = TIMEFRAME_MS[timeframe]
    if binance_ts + tf_ms < now_ms:
        return True
    return False


def check_candles_match(db_candles, binance_candles):

    for  db_c, bin_c in zip_longest(db_candles, binance_candles):
        if is_closed_candle(bin_c[0], "1m"):
            for val_db_c, val_bin_c in zip(db_c, bin_c):
                if  round(val_db_c, 6) != round(val_bin_c, 6):
                    print(f"""
                    diff in : 
                    {db_c}
                    {bin_c}
                    db   : {val_db_c}
                    bin  : {val_bin_c}
                    """)
        
def datetime_to_timestamp_ms (db_candles: list) ->list:
    db_candles = [
         [ int(c[0].replace(tzinfo=timezone.utc).timestamp() * 1000), c[1], c[2], c[3], c[4], c[5] ] for c in db_candles
    ]
    return db_candles

if __name__ == "__main__":
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
            SELECT open_time, open, high, low, close, volume
            FROM market_data
            WHERE asset_id = %s AND timeframe_id = %s AND open_time >= %s
            ORDER BY open_time ASC
            """, (asset_id, timeframe_id, SINCE_UTC))

            db_candles = cur.fetchall()

        db_candles = datetime_to_timestamp_ms(db_candles)
        
   
        binance_candles = []
        while True:
            candles = exchange.fetch_ohlcv("BTC/USDT", "1m", since=since_ms, limit=1000)
            if candles:
                binance_candles.extend(candles)
            since_ms = candles[-1][0] + 1
            if len(candles) < 1000:
                break
   
        check_candles_match(db_candles, binance_candles)    
        conn.close()

    except Exception as e:
        print("ERROR:", e)
        traceback.print_exc()