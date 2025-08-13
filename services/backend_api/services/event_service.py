# backend_api/services/trade_service.py
from flask import current_app, request
from extensions import db
from models import TradeModel, TradeHistoryModel, NotificationModel, SpreadsModel
import uuid
from datetime import datetime
from sqlalchemy import and_, or_
import json
import psycopg2
from services.config import redis_connection
redis_client = redis_connection()
from flask import Flask, render_template, jsonify, Response, stream_with_context, json
from sqlalchemy import text
from services.db_config import DB_NAME_2, DB_PORT, DB_USER, DB_PASS, DB_HOST, DB_SCHEMA
from services.config import redis_connection
from services.filter_manager import filter_manager  # Added filter manager
# filter_manager = FilterManager()
from datetime import datetime, timedelta
from collections import defaultdict
import itertools
from psycopg2.extras import RealDictCursor
from services.db_config import get_db_connection
import psycopg2.extras

radis_client = redis_connection()

class Notifications:
    """Service for handling notifications."""

    @staticmethod
    def Notifications_data(exchange_filter):
        """
        Retrieve the latest unseen notification and all notifications based on exchange mode filter.
        If an unseen notification exists, it is marked as seen.

        Returns:
            Dictionary containing:
            - `latest_notification`: The most recent unseen notification (if any).
            - `all_notifications`: A list of all notifications filtered by exchange mode.
        """
        try:
            # Fetch unseen notifications filtered by exchange_mode (latest first)
            unseen_notifs = NotificationModel.query.filter_by(seen=False, exchange_mode=exchange_filter).order_by(
                NotificationModel.created_at.desc()
            ).all()

            # Initialize the latest_notification as empty
            latest_notification = ""
            if unseen_notifs:
                latest_instance = unseen_notifs[0]  # Get latest unseen notification
                if not latest_instance.seen:
                    latest_instance.seen = True
                    db.session.commit()

                latest_notification = latest_instance.to_dict()

            # Fetch all notifications filtered by exchange_mode (latest first)
            # print("exchange_filter",exchange_filter)
            all_notifs = NotificationModel.query.filter_by(exchange_mode=exchange_filter).order_by(
                NotificationModel.created_at.desc()
            ).all()
            # print("all_notifs", all_notifs)
            # Return the response with the latest notification and all notifications
            return {
                "latest_notification": latest_notification,
                "all_notifications": [notif.to_dict() for notif in all_notifs] if all_notifs else [],
            }

        except Exception as e:
            db.session.rollback()
            current_app.logger.error(f"Error fetching notifications: {e}")
            return {
                "latest_notification": "",
                "all_notifications": []
            }


class AccountMatrixdata:
    @staticmethod
    def Mtarixdata():
        # Fetch all account matrix data from Redis
        account_matrix = redis_client.hgetall("account_matrix:account")
        # Decode the byte keys and values
        account_matrix = {k.decode('utf-8'): v.decode('utf-8') for k, v in account_matrix.items()}
        # Directly return the dictionary
        return jsonify(account_matrix)


class Get_SpreadData:
    @staticmethod
    def SpreadData():
        trades = SpreadsModel.query.all()
        return [trade.to_dict() for trade in trades]


class Get_count_ttp:
    @staticmethod
    def get_thread_data():
        # Retrieve all members with their scores from the sorted set.
        data = redis_client.zrange("threads_task_count", 0, -1, withscores=True)

        thread_data = []
        for member, score in data:
            try:
                # member is a bytes object; decode it and then load as JSON.
                record_dict = json.loads(member.decode('utf-8'))
            except Exception as e:
                # Fallback: if JSON fails, just decode the string.
                record_dict = member.decode('utf-8')
            thread_data.append({"data": record_dict})
        # print("thread_data",thread_data)
        return jsonify(thread_data)


# Helper: Get a PostgreSQL database connection.
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


class TradeService:
    """Service for handling trade-related operations."""

    @staticmethod
    def get_all_trades(exchange_filter, limit=None, offset=None, start_date=None):
        table_name = f"{exchange_filter}_utils_trade"
        combined_data = []

        conn = get_db_connection()
        cursor = conn.cursor()

        query = f"""
            SELECT id, symbol, candle_time, action, price, exit_price, pnl, status, executed_at, stop_loss, target_price, exchange_mode
            FROM {table_name}
        """

        # ✅ Date filter
        if start_date:
            if isinstance(start_date, str):
                try:
                    start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    print("⚠️ Invalid start_date format. Skipping date filter.")
                    start_date = None

        if start_date:
            start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
            query += f" WHERE candle_time >= '{start_date_str}'"

        query += " ORDER BY candle_time DESC"

        # ✅ Convert limit/offset to int
        if limit is not None:
            limit = int(limit)
        if offset is not None:
            offset = int(offset)

        if limit is not None and offset is not None:
            query += f" LIMIT {limit} OFFSET {offset}"
        elif limit is not None:
            query += f" LIMIT {limit}"

        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            data_dict = {
                "id": row[0],
                "symbol": row[1],
                "candle_time": row[2].isoformat() if row[2] else None,
                "action": row[3],
                "entry_price": float(row[4]) if row[4] is not None else None,
                "exit_price": float(row[5]) if row[5] is not None else None,
                "pnl": float(row[6]) if row[6] is not None else None,
                "status": row[7],
                "executed_at": row[8].isoformat() if row[8] else None,
                "stop_loss": float(row[9]) if row[9] is not None else None,
                "target_price": float(row[10]) if row[10] is not None else None,
                "exchange_mode": row[11] if row[11] is not None else None,
            }
            combined_data.append(data_dict)

        cursor.close()
        conn.close()

        # ✅ Update PnL from Redis for active trades
        try:
            redis_client = redis_connection()
            for trade in combined_data:
                if trade["status"] and trade["status"].lower() == "active":
                    redis_key = f"{exchange_filter}_spread_trade:{trade['symbol']}"
                    redis_data = redis_client.get(redis_key)
                    if redis_data:
                        try:
                            data = json.loads(redis_data)
                            if data.get("pnl") is not None:
                                trade["pnl"] = float(data["pnl"])
                        except json.JSONDecodeError:
                            pass
        except Exception as e:
            print(f"Error updating PnL from Redis: {e}")

        return combined_data


class TradeHistory:
    """Service for handling trade history operations."""

    @staticmethod
    def trades_history(exchange_filter, limit=None, offset=None, start_date=None):
        def parse_datetime(date_input):
            if isinstance(date_input, str):
                return datetime.strptime(date_input, "%Y-%m-%d %H:%M:%S")
            elif isinstance(date_input, datetime):
                return date_input
            return None

        table_name = f"{exchange_filter}_utils_trade_history"
        combined_data = []

        conn = get_db_connection()
        cursor = conn.cursor()

        query = f"""
                    SELECT id, symbol, candle_time, action, price, exit_price, pnl, status, executed_at, stop_loss, target_price, exchange_mode
                    FROM {table_name}
                """
        # Add filtering based on start_date
        # ✅ Skip date filtering if start_date is None
        if start_date:
            if isinstance(start_date, str):
                try:
                    start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    print("⚠️ Invalid start_date format. Skipping date filter.")
                    start_date = None

        conditions = []
        if start_date:
            start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
            conditions.append(f"candle_time >= '{start_date_str}'")

        # Add condition to exclude status = 'active'
        conditions.append("status <> 'active'")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY candle_time desc"

        # Add limit and offset if provided
        if limit is not None and offset is not None:
            query += f" LIMIT {limit} OFFSET {offset}"
        elif limit is not None:
            query += f" LIMIT {limit}"

        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            pnl = float(row[6]) if row[6] is not None else None
            # Skip trade if pnl is None, 0, or invalid
            if pnl in [None, 0]:
                continue
            data_dict = {
                "symbol": row[1],
                "candle_time": row[2].isoformat() if row[2] else None,
                "action": row[3],
                "entry_price": float(row[4]) if row[4] is not None else None,
                "exit_price": float(row[5]) if row[5] is not None else None,
                "pnl": float(row[6]) if row[6] is not None else None,
                "status": row[7],
                "executed_at": row[8].isoformat() if row[8] else None,
                "stop_loss": float(row[9]) if row[9] is not None else None,
                "target_price": float(row[10]) if row[10] is not None else None,
                "exchange_mode": row[11] if row[11] is not None else None,
            }
            combined_data.append(data_dict)

        return combined_data


class TradeAndTradeHistoryMetrics:
    @staticmethod
    def get_trade_and_trade_historymatrics():
        # try:
        # Mapping period to days
        period_days = {
            "1w": 10, "1m": 40, "3m": 105, "6m": 215, "1y": 380, "all": 9999
        }

        # Get period and exchange from the request
        period = request.args.get("period", "1w")
        exchange = request.args.get("exchange", "binance")
        view_period = period if period in period_days else "1w"
        days = period_days.get(view_period, 999999)

        # Determine the start date based on the period
        start_date = datetime(1970, 1, 1) if days >= 365 * 50 else datetime.now() - timedelta(days=days)
        start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
        print("start_date_str", start_date_str)
        # Construct the view name (you only have one table now)
        view_name1 = f"{exchange}_utils_trade_history_and_trade_period_all"

        # Fetch base capital from Redis
        base_capital = 0
        total_capital = redis_client.hget("account_matrix:account", "total_capital")
        if total_capital:
            base_capital = float(total_capital)

        # DB Query to fetch trade and trade history metrics
        conn = get_db_connection()
        cursor = conn.cursor()
        query = f"""
            SELECT 
                COALESCE(SUM(total_pnl), 0),
                COALESCE(SUM(CASE WHEN bucket >= %s THEN active_count ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN bucket >= %s THEN closed_count ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN bucket <= %s THEN total_pnl ELSE 0 END), 0),
                COALESCE(SUM(active_count + closed_count), 0),
                COALESCE(SUM(avg_deal_time_secs), 0)
            FROM {view_name1};
        """

        # Execute the query without the WHERE clause
        cursor.execute(query, (start_date_str, start_date_str, start_date_str))
        (
        total_pnl_sum, active_count, closed_count, pnl_before_period, total_trades, total_deal_time) = cursor.fetchone()

        cursor.close()
        conn.close()

        for_end_cap_pnl_sum = base_capital + float(total_pnl_sum)
        for_strt_cap_pnl_sum = base_capital + float(pnl_before_period)

        netcollection = for_end_cap_pnl_sum - for_strt_cap_pnl_sum
        roi = (netcollection / for_strt_cap_pnl_sum * 100) if for_strt_cap_pnl_sum else 0
        avg_deal_time_hours = (total_deal_time / total_trades / 3600) if total_trades else None

        # Optionally, set Redis with final capital metrics (if needed)
        redis_client.set("for_strt_cap_pnl_sum", for_strt_cap_pnl_sum)

        # Create the result for the response
        stats = {
            "netcollection": round(netcollection, 2),
            "roi": round(roi, 2),
            "base_capital": round(base_capital, 2),
            "running_capital": round(for_end_cap_pnl_sum, 2),
            "active_count": active_count,
            "closed_count": closed_count,
            "avg_deal_time_hours": round(avg_deal_time_hours, 2) if avg_deal_time_hours is not None else None,
        }

        calc_data = {
            "for_strt_cap_pnl_sum": round(for_strt_cap_pnl_sum, 2),
            "for_end_cap_pnl_sum": round(for_end_cap_pnl_sum, 2),
        }

        return Response(json.dumps({"calc_data": calc_data, "stats": stats}), mimetype="application/json")


class Trade_and_tradehistoryfor_dealbook:
    @staticmethod
    def get_trade_and_tradehistoryfor_dealbook():
        # Get filter parameters from request and set the filter.
        period = request.args.get('period', None)
        exchange = request.args.get('exchange', None)
        limit = request.args.get('limit', None)  # Default to 100 rows
        offset = request.args.get('offset', None)  # Default to start from the first row

        # print("period", period, "exchange", exchange, "limit", limit, "offset", offset)

        # Update filter based on provided arguments
        if period is None and exchange is None:
            period = "1w"
            exchange = ""
        filter_manager.set_filter(period or "1w", exchange or "", offset, limit)

        all_trades = TradeService.get_all_trades(exchange, limit, offset, )
        history_source = TradeHistory.trades_history(exchange, limit, offset)

        combined_trades = list(itertools.chain(all_trades, history_source))

        return {
            "status": "success",
            "data": combined_trades,
            'history_source': history_source,
        }


class Trade_and_historyfor_chart:
    @staticmethod
    def get_trade_and_historyfor_chart():
        # Fetch global filters from Redis (still using Redis for this part)
        filters = redis_client.hgetall("global_filter")
        filters = {k.decode(): v.decode() for k, v in filters.items()}

        period = request.args.get('period', None)
        exchange = request.args.get('exchange', None)

        for_strt_cap_pnl_sum = redis_client.get("for_strt_cap_pnl_sum")
        for_strt_cap_pnl_sum = float(for_strt_cap_pnl_sum.decode("utf-8")) if for_strt_cap_pnl_sum else 0

        # Map period to a start date
        now = datetime.now()
        period_map = {
            "1w": timedelta(days=10),
            "1m": timedelta(days=40),
            "3m": timedelta(days=105),
            "6m": timedelta(days=215),
            "1y": timedelta(days=380),
            "all": timedelta(days=9999),
        }
        start_date = now - period_map.get(period, timedelta(days=9999))

        # Establish PostgreSQL connection
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        # cur = global_connection.cursor(cursor_factory=RealDictCursor)
        # Updated query with filtering and formatting in SQL
        query = """
            SELECT 
                TO_CHAR(bucket, 'YYYY-MM-DD"T"HH24:MI:SS') AS time,  -- ISO 8601 format
                volume AS pnl_sum
            FROM {exchange}_utils_trade_history_1h
            WHERE bucket >= %s AND volume IS NOT NULL
            ORDER BY bucket ASC;
        """.format(exchange=exchange)  # Safely insert the exchange table

        # Execute the query with the start_date as a parameter
        cur.execute(query, (start_date,))
        data = cur.fetchall()

        # Close the connection
        cur.close()
        conn.close()

        # Return API response
        return {
            "status": "success",
            "base_capital": for_strt_cap_pnl_sum,
            "data": data,
        }


# this query wioll delte view data
# DELETE FROM nse_utils_trade_history_1h


class Trade_and_tradehistoryfor_sidebar:
    @staticmethod
    def get_trade_and_tradehistoryfor_sidebar():
        # Get filter parameters from request and set the filter.

        exchange = request.args.get('exchange', None)
        period = request.args.get('period', None)

        # filters = redis_client.hgetall("global_filter")
        # filters = dict(map(lambda item: (item[0].decode(), item[1].decode()), filters.items()))
        #
        # period = filters.get("period", "")

        # Update filter based on provided arguments
        # print("period", period, "exchange", exchange)
        now = datetime.now()
        period_map = {
            "1w": timedelta(days=10),
            # "1w": timedelta(days=10), this is the corect
            "1m": timedelta(days=40),
            "3m": timedelta(days=105),
            "6m": timedelta(days=215),
            "1y": timedelta(days=380),
            "all": timedelta(days=9999),
        }
        start_date = now - period_map.get(period, timedelta(days=9999))

        # filters = redis_client.hgetall("global_filter")
        # filters = {k.decode(): v.decode() for k, v in filters.items()}

        # current_filter_json = json.dumps(filters)
        # exchange = filters.get("exchange", "")
        # limit = filters.get("limit", "")

        # print(period, exchange, limit, offset)
        # Apply limit and offset while fetching trades and history
        # all_trades = TradeService.get_all_trades(exchange, limit, offset,)
        history_source = TradeHistory.trades_history(exchange_filter=exchange, start_date=start_date)

        # print("history_source", history_source)
        # Combine and paginate results
        # combined_trades = list(itertools.chain(history_source))

        return {
            "status": "success",
            'data': history_source,
        }