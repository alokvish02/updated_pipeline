import eventlet
import requests
from decimal import Decimal
from flask_cors import CORS
from flask import Flask, jsonify, request, render_template, Blueprint
from flask_socketio import SocketIO, emit
import psycopg2
from datetime import datetime, date, time, timedelta
import json
import pandas as pd
import redis
from services.config import redis_connection
from services.db_config import DB_NAME_2, DB_PORT, DB_USER, DB_PASS, DB_HOST, DB_SCHEMA
from psycopg2 import sql
from dateutil.parser import parse as parse_datetime
from services.backend_api.services.ltp_ws import fetch_ltp
eventlet.monkey_patch()
from flask import request
app = Flask(__name__)
CORS(app)

socketio = SocketIO(app, cors_allowed_origins=["http://127.0.0.1:5000", "http://localhost:5000", "http://192.168.1.6:5000"])

chart_bp = Blueprint('chart', __name__)

redis_client = redis_connection()

def get_db_connection():
    """Establish and return a PostgreSQL connection."""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME_2,
        user=DB_USER,
        password=DB_PASS
    )
    return conn

def format_timestamp(timestamp):
    """
    Return the timestamp as a string in the format 'YYYY-MM-DDTHH:MM:SS'.
    No timezone conversion is performed.
    """
    try:
        if isinstance(timestamp, datetime):
            return timestamp.strftime("%Y-%m-%dT%H:%M:%S")
        dt = parse_datetime(str(timestamp))
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception as e:
        print(f"Error formatting timestamp {timestamp}: {e}")
        return str(timestamp)

@chart_bp.route('/ohlcv')
def get_ohlcv():
    symbol = request.args.get('symbol')
    interval = request.args.get('interval', '1m')
    limit = int(request.args.get('limit', 500))
    offset = int(request.args.get('offset', 0))

    if not symbol:
        return jsonify({"error": "Symbol is required"}), 400

    # Determine table and db symbol
    if symbol.lower().startswith('nse_spreads_'):
        materialized_view = f"nse_spreads_{interval}"
        db_symbol = symbol.replace('NSE_SPREADS_', '').lower()
    elif symbol.lower().startswith('binance_'):
        materialized_view = f"binance_spreads_{interval}"
        db_symbol = symbol.replace('BINANCE_', '').lower()
    elif symbol.lower().startswith('snp_spreads_'):
        materialized_view = f"snp_spreads_{interval}"
        db_symbol = symbol.replace('SNP_SPREADS_', '').lower()
    elif symbol.lower().startswith('etf_spreads_'):
        materialized_view = f"etf_spreads_{interval}"
        db_symbol = symbol.replace('ETF_SPREADS_', '').lower()
    elif symbol.lower().startswith('nse_'):
        materialized_view = f"nse_stocks_{interval}"
        db_symbol = symbol.replace('NSE_', '')
    elif symbol.lower().startswith('snp_'):
        materialized_view = f"snp_stocks_{interval}"
        db_symbol = symbol.replace('SNP_', '')
    elif symbol.lower().startswith('etf_'):
        materialized_view = f"etf_stocks_{interval}"
        db_symbol = symbol.replace('ETF_', '')
    elif symbol.lower().startswith('crypto_'):
        materialized_view = f"binance_stocks_{interval}"
        db_symbol = symbol.replace('CRYPTO_', '')
    else:
        materialized_view = f"binance_stocks_{interval}"
        db_symbol = symbol

    # Validate interval
    valid_intervals = ["1m", "5m", "15m", "30m", "1d"]
    if interval not in valid_intervals:
        return jsonify({"error": "Invalid interval"}), 400

    # Fetch from PostgreSQL
    db_candles = []
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        query = sql.SQL("""
            SELECT bucket, open, high, low, close, volume
            FROM {view}
            WHERE symbol = %s
            ORDER BY bucket DESC
            LIMIT %s OFFSET %s
        """).format(view=sql.Identifier(materialized_view))

        cur.execute(query, (db_symbol, limit, offset))
        rows = cur.fetchall()
        for row in rows:
            db_candles.append({
                'timestamp': format_timestamp(row[0]),
                'open': float(row[1]),
                'high': float(row[2]),
                'low': float(row[3]),
                'close': float(row[4]),
                'volume': float(row[5])
            })
    except Exception as e:
        print(f"Database query failed: {e}")
        cur.close()
        conn.close()
        return jsonify({"error": f"Database query failed: {e}"}), 500
    finally:
        cur.close()
        conn.close()

    if not db_candles:
        print("No data found in database")
        return jsonify([])

    df = pd.DataFrame(db_candles)
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize('Asia/Kolkata')  # IST

    # Sort and drop duplicates
    df = df.sort_values('timestamp').drop_duplicates(subset=['timestamp'])

    # Convert to epoch seconds in IST
    df['timestamp'] = df['timestamp'].astype('int64') // 10 ** 9  # vectorized epoch conversion

    # Final output as list of dicts (no for loop)
    result = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].to_dict('records')

    return jsonify(result)


@chart_bp.route('/tables', methods=['GET'])
def get_tables():
    try:
        binance_raw = redis_client.smembers("BINANCE_SYMBOLS")
        fyers_raw = redis_client.smembers("FYERS_SYMBOLS")
        snp_raw = redis_client.smembers("SNP_SYMBOLS")
        etf_raw = redis_client.smembers("ETF_SYMBOLS")
        binance_symbols = [symbol.decode("utf-8") for symbol in binance_raw]
        fyers_symbols = [symbol.decode("utf-8") for symbol in fyers_raw]
        snp_symbols = [symbol.decode("utf-8") for symbol in snp_raw]
        etf_symbols = [symbol.decode("utf-8") for symbol in etf_raw]
        binance_spreads = [f"CRYPTO_{symbol}" for symbol in binance_symbols]
        nse_spreads = [f"NSE_{symbol}" for symbol in fyers_symbols]
        snp_spreads = [f"SNP_{symbol}" for symbol in snp_symbols]
        etf_spreads = [f"ETF_{symbol}" for symbol in etf_symbols]
        spreads = binance_spreads + nse_spreads + snp_spreads + etf_spreads
        return jsonify({"tables": spreads})
    except Exception as e:
        print(f"Error fetching tables: {e}")
        return jsonify({"error": str(e)}), 500

@chart_bp.route('/spreads', methods=['GET'])
def get_spreads():
    try:
        binance_raw = redis_client.smembers("spreads:binance_spreads_name")
        fyers_raw = redis_client.smembers("spreads:nse_spreads_name")
        snp_raw = redis_client.smembers("spreads:snp_spreads_name")
        etf_raw = redis_client.smembers("spreads:etf_spreads_name")
        binance_symbols = [symbol.decode("utf-8") for symbol in binance_raw]
        fyers_symbols = [symbol.decode("utf-8") for symbol in fyers_raw]
        snp_symbols = [symbol.decode("utf-8") for symbol in snp_raw]
        etf_symbols = [symbol.decode("utf-8") for symbol in etf_raw]
        binance_spreads = [f"BINANCE_{symbol}" for symbol in binance_symbols]
        nse_spreads = [f"NSE_SPREADS_{symbol}" for symbol in fyers_symbols]
        snp_spreads = [f"SNP_SPREADS_{symbol}" for symbol in snp_symbols]
        etf_spreads = [f"ETF_SPREADS_{symbol}" for symbol in etf_symbols]
        spreads = binance_spreads + nse_spreads + snp_spreads + etf_spreads
        return jsonify({"tables": spreads})
    except Exception as e:
        print(f"Error fetching spreads: {e}")
        return jsonify({"error": str(e)}), 500

@chart_bp.route('/chart')
def chart():
    return render_template('chart.html')

def init_socketio_events(socketio):
    active_subscriptions = {}
    last_emitted_ltp = {}
    polling_active = True

    @socketio.on('symbol_subscribed')
    def handle_symbol_subscribed(data):
        print("User subscribed to symbol:", data)
        handle_subscription(data)

    @socketio.on('client_message')
    def handle_client_message(data):
        # print("Received client message:", data)
        handle_subscription(data)

    def handle_subscription(data):
        sid = request.sid
        symbol = data.get('symbol')
        if symbol:
            if sid not in active_subscriptions:
                active_subscriptions[sid] = set()
            active_subscriptions[sid].add(symbol)
            try:
                ltp_data = fetch_ltp(symbol, redis_client)
                last_emitted_ltp[(sid, symbol)] = ltp_data['ltp']  # Store initial value
                socketio.emit('ltp_update', ltp_data, room=sid)
                print(f"Active subscriptions for {sid}: {active_subscriptions[sid]}")
            except Exception as e:
                pass

    @socketio.on('unsubscribe')
    def handle_unsubscribe(data=None):
        sid = request.sid
        if sid in active_subscriptions:
            symbols = active_subscriptions.pop(sid, set())
            for symbol in symbols:
                last_emitted_ltp.pop((sid, symbol), None)
            print(f"User unsubscribed (sid: {sid}): Removed symbols {symbols}")
        else:
            print(f"User unsubscribed (sid: {sid}): No active subscriptions found")

    @socketio.on('disconnect')
    def handle_disconnect():
        sid = request.sid
        if sid in active_subscriptions:
            symbols = active_subscriptions.pop(sid, set())
            for symbol in symbols:
                last_emitted_ltp.pop((sid, symbol), None)
            print(f"User disconnected (sid: {sid}): Removed symbols {symbols}")
        else:
            print(f"User disconnected (sid: {sid}): No active subscriptions found")

    def poll_redis():
        while polling_active:
            try:
                for sid, symbols in list(active_subscriptions.items()):
                    for symbol in symbols:
                        try:
                            ltp_data = fetch_ltp(symbol, redis_client)
                            key = (sid, symbol)
                            last_ltp = last_emitted_ltp.get(key)
                            current_ltp = ltp_data['ltp']

                            if current_ltp != last_ltp:
                                socketio.emit('ltp_update', ltp_data, room=sid)
                                last_emitted_ltp[key] = current_ltp
                        except Exception as e:
                            pass
                eventlet.sleep(1)
            except Exception as e:
                print(f"Polling error: {e}")
                eventlet.sleep(1) 

    eventlet.spawn(poll_redis)

    # Optional: Add a cleanup function to stop polling on shutdown
    def stop_polling():
        nonlocal polling_active
        polling_active = False
        print("Stopping Redis polling thread")


# Assuming app and socketio are defined elsewhere
app.register_blueprint(chart_bp)
