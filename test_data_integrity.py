"""
Test script for backtesting platform data integrity.
Run from project root: python test_data_integrity.py
"""

import os
import sys
import ccxt
import psycopg2
from datetime import timezone
from dotenv import load_dotenv

load_dotenv()

# ─── DB CONNECTION ────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )

# ─── HELPERS ──────────────────────────────────────────────────────────────────

TIMEFRAME_MS = {
    '1m':  60_000,
    '3m':  3 * 60_000,
    '5m':  5 * 60_000,
    '15m': 15 * 60_000,
    '30m': 30 * 60_000,
    '1h':  60 * 60_000,
    '2h':  2 * 60 * 60_000,
    '4h':  4 * 60 * 60_000,
    '6h':  6 * 60 * 60_000,
    '8h':  8 * 60 * 60_000,
    '12h': 12 * 60 * 60_000,
    '1d':  24 * 60 * 60_000,
    '3d':  3 * 24 * 60 * 60_000,
    '1w':  7 * 24 * 60 * 60_000,
}

PASS = "\033[92m[PASS]\033[0m"
FAIL = "\033[91m[FAIL]\033[0m"
WARN = "\033[93m[WARN]\033[0m"

errors = []

def ok(msg):
    print(f"{PASS} {msg}")

def fail(msg):
    print(f"{FAIL} {msg}")
    errors.append(msg)

def warn(msg):
    print(f"{WARN} {msg}")

# ─── TEST 1 : OHLCV CONSISTENCY ───────────────────────────────────────────────

def test_ohlcv_consistency(conn):
    print("\n── Test 1 : OHLCV consistency ──")
    query = """
        SELECT id, asset_id, timeframe_id, open_time, open, high, low, close, volume
        FROM market_data
        WHERE high < open OR high < close OR low > open OR low > close OR volume < 0
    """
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    if not rows:
        ok("All OHLCV values are consistent")
    else:
        for row in rows:
            fail(f"Inconsistent candle id={row[0]} asset={row[1]} tf={row[2]} open_time={row[3]}")

# ─── TEST 2 : CONTINUITY ──────────────────────────────────────────────────────

def test_continuity(conn):
    print("\n── Test 2 : Timestamp continuity ──")
    query = """
        SELECT a.name, t.label, t.minutes,
               m.open_time,
               LAG(m.open_time) OVER (PARTITION BY m.asset_id, m.timeframe_id ORDER BY m.open_time) AS prev_time
        FROM market_data m
        JOIN asset a ON a.id = m.asset_id
        JOIN timeframe t ON t.id = m.timeframe_id
        WHERE t.label != '1M'
    """
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    gaps = []
    for asset, tf_label, tf_minutes, open_time, prev_time in rows:
        if prev_time is None:
            continue
        expected_ms = tf_minutes * 60_000
        actual_ms = int((open_time - prev_time).total_seconds() * 1000)
        if actual_ms != expected_ms:
            gaps.append((asset, tf_label, prev_time, open_time, actual_ms - expected_ms))

    if not gaps:
        ok("No continuity gaps detected")
    else:
        for asset, tf, prev, curr, delta_ms in gaps[:10]:  # show max 10
            fail(f"Gap in {asset} {tf}: {prev} → {curr} (delta={delta_ms}ms)")
        if len(gaps) > 10:
            warn(f"... and {len(gaps) - 10} more gaps")

# ─── TEST 3 : AGGREGATION CHECK ───────────────────────────────────────────────

def test_aggregation(conn):
    print("\n── Test 3 : Aggregation consistency (spot check last closed candle) ──")

    # pick BTC/USDT asset_id
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM asset WHERE name = 'BTC/USDT'")
        row = cur.fetchone()
        if not row:
            warn("BTC/USDT not found in asset table, skipping aggregation test")
            return
        asset_id = row[0]

        cur.execute("SELECT id FROM timeframe WHERE label = '1m'")
        tf_1m_id = cur.fetchone()[0]

    agg_timeframes = ['3m', '5m', '15m', '30m', '1h']

    for tf_label in agg_timeframes:
        n = TIMEFRAME_MS[tf_label] // TIMEFRAME_MS['1m']

        with conn.cursor() as cur:
            # get timeframe_id
            cur.execute("SELECT id FROM timeframe WHERE label = %s", (tf_label,))
            row = cur.fetchone()
            if not row:
                warn(f"Timeframe {tf_label} not found, skipping")
                continue
            tf_id = row[0]

            # get last closed candle for this tf
            cur.execute("""
                SELECT open_time, open, high, low, close, volume
                FROM market_data
                WHERE asset_id = %s AND timeframe_id = %s
                ORDER BY open_time DESC LIMIT 1
            """, (asset_id, tf_id))
            agg = cur.fetchone()
            if not agg:
                warn(f"No candle for BTC/USDT {tf_label}, skipping")
                continue
            agg_open_time, agg_open, agg_high, agg_low, agg_close, agg_volume = agg

            # get the n 1m candles that compose it
            cur.execute("""
                SELECT open_time, open, high, low, close, volume
                FROM market_data
                WHERE asset_id = %s AND timeframe_id = %s AND open_time >= %s
                ORDER BY open_time ASC LIMIT %s
            """, (asset_id, tf_1m_id, agg_open_time, n))
            candles_1m = cur.fetchall()

        if len(candles_1m) != n:
            warn(f"BTC/USDT {tf_label}: expected {n} 1m candles, got {len(candles_1m)} — skipping")
            continue

        expected_open   = candles_1m[0][1]
        expected_high   = max(c[2] for c in candles_1m)
        expected_low    = min(c[3] for c in candles_1m)
        expected_close  = candles_1m[-1][4]
        expected_volume = round(sum(c[5] for c in candles_1m), 5)
        actual_volume   = round(agg_volume, 5)

        ok_flag = True
        if abs(agg_open - expected_open) > 0.01:
            fail(f"BTC/USDT {tf_label} open mismatch: got {agg_open}, expected {expected_open}")
            ok_flag = False
        if abs(agg_high - expected_high) > 0.01:
            fail(f"BTC/USDT {tf_label} high mismatch: got {agg_high}, expected {expected_high}")
            ok_flag = False
        if abs(agg_low - expected_low) > 0.01:
            fail(f"BTC/USDT {tf_label} low mismatch: got {agg_low}, expected {expected_low}")
            ok_flag = False
        if abs(agg_close - expected_close) > 0.01:
            fail(f"BTC/USDT {tf_label} close mismatch: got {agg_close}, expected {expected_close}")
            ok_flag = False
        if abs(actual_volume - expected_volume) > 0.1:
            warn(f"BTC/USDT {tf_label} volume diff: db={actual_volume}, computed={expected_volume} (float rounding)")

        if ok_flag:
            ok(f"BTC/USDT {tf_label} aggregation is correct")

# ─── TEST 4 : BINANCE COMPARISON SINCE 11H UTC ───────────────────────────────

SINCE_UTC = "2026-04-06T11:00:00Z"

def test_binance_comparison(conn):
    print(f"\n── Test 4 : Binance comparison since {SINCE_UTC} (all closed candles) ──")

    exchange = ccxt.binance({'enableRateLimit': True})
    since_ms = int(exchange.parse8601(SINCE_UTC))

    assets = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'XRP/USDT']
    check_tfs = ['1m', '3m', '5m', '15m', '30m', '1h', '2h']

    for asset_name in assets:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM asset WHERE name = %s", (asset_name,))
            row = cur.fetchone()
            if not row:
                warn(f"{asset_name} not found in DB, skipping")
                continue
            asset_id = row[0]

        for tf_label in check_tfs:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM timeframe WHERE label = %s", (tf_label,))
                row = cur.fetchone()
                if not row:
                    continue
                tf_id = row[0]

                cur.execute("""
                    SELECT open_time, open, high, low, close, volume
                    FROM market_data
                    WHERE asset_id = %s AND timeframe_id = %s AND open_time >= %s
                    ORDER BY open_time ASC
                """, (asset_id, tf_id, SINCE_UTC))
                db_candles = cur.fetchall()

            if not db_candles:
                warn(f"{asset_name} {tf_label}: no candles in DB since {SINCE_UTC}")
                continue

            # fetch from binance since 11h
            binance_candles = exchange.fetch_ohlcv(asset_name, tf_label, since=since_ms, limit=1000)

            # build binance dict: ts -> candle (exclude last = in progress)
            binance_map = {c[0]: c for c in binance_candles[:-1]}

            mismatches = 0
            missing = 0
            checked = 0

            for db_row in db_candles:
                db_open_time, db_open, db_high, db_low, db_close, db_volume = db_row
                db_ts = int(db_open_time.replace(tzinfo=timezone.utc).timestamp() * 1000)

                if db_ts not in binance_map:
                    # candle too recent (still open on binance side) — skip
                    missing += 1
                    continue

                b = binance_map[db_ts]
                checked += 1

                if abs(db_open - b[1]) > 0.01:
                    fail(f"{asset_name} {tf_label} {db_open_time} open: db={db_open} binance={b[1]}")
                    mismatches += 1
                if abs(db_high - b[2]) > 0.01:
                    fail(f"{asset_name} {tf_label} {db_open_time} high: db={db_high} binance={b[2]}")
                    mismatches += 1
                if abs(db_low - b[3]) > 0.01:
                    fail(f"{asset_name} {tf_label} {db_open_time} low: db={db_low} binance={b[3]}")
                    mismatches += 1
                if abs(db_close - b[4]) > 0.01:
                    fail(f"{asset_name} {tf_label} {db_open_time} close: db={db_close} binance={b[4]}")
                    mismatches += 1
                if abs(db_volume - b[5]) > 0.5:
                    warn(f"{asset_name} {tf_label} {db_open_time} volume diff: db={db_volume} binance={b[5]}")

            if mismatches == 0:
                ok(f"{asset_name} {tf_label}: {checked} candles checked, all match Binance")
            else:
                fail(f"{asset_name} {tf_label}: {mismatches} mismatches found out of {checked} candles")

# ─── MAIN ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print("  Backtesting Platform — Data Integrity Tests")
    print("=" * 55)

    try:
        conn = get_conn()
    except Exception as e:
        print(f"{FAIL} Cannot connect to DB: {e}")
        sys.exit(1)

    test_ohlcv_consistency(conn)
    test_continuity(conn)
    test_aggregation(conn)
    test_binance_comparison(conn)

    conn.close()

    print("\n" + "=" * 55)
    if errors:
        print(f"{FAIL} {len(errors)} test(s) failed.")
        sys.exit(1)
    else:
        print(f"{PASS} All tests passed.")
