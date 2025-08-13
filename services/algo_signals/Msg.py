from datetime import datetime
import psycopg2
import pytz
from services.db_config import DB_HOST, DB_PORT,DB_NAME_2, DB_USER, DB_PASS
from services.loger import logger
from services.config import redis_client

def current_time():
    # Return the current time in IST
    return datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")

def get_db_connection():
    """
    Establish and return a database connection using the global configuration.
    """
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME_2,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        logger.error(f"[ERROR] Database Connection Failed: {e}")
        return None


def ensure_notifications_table():
    """
    Ensures that the 'notification' table exists in TimescaleDB with correct data types and is a hypertable.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS notification (
        symbol VARCHAR(50) NOT NULL,
        action VARCHAR(50) NOT NULL,
        price NUMERIC(12, 2) NOT NULL,
        stop_loss NUMERIC(12, 2) NOT NULL,
        target_price NUMERIC(12, 2) NOT NULL,
        execution_time TIMESTAMP NOT NULL,
        message TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        seen BOOLEAN NOT NULL DEFAULT FALSE,
        exit_price NUMERIC(12, 2) NOT NULL,
        pnl NUMERIC(12, 2) NOT NULL,
        status VARCHAR(20) NOT NULL,
        exchange_mode VARCHAR(10),
        PRIMARY KEY (action, symbol, created_at)
    );
    """
    create_hypertable_query = """
    SELECT create_hypertable(
        'notification',
        'execution_time',
        chunk_time_interval => INTERVAL '1 day',
        if_not_exists => TRUE
    );
    """

    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                # Step 1: Create the table if it doesn't exist
                cursor.execute(create_table_query)

                # Step 2: Convert the table into a hypertable
                cursor.execute(create_hypertable_query)

            conn.commit()
            # print("[INFO] 'notification' table ensured as a TimescaleDB hypertable.")
        except Exception as e:
            print(f"[ERROR] Failed to create or convert 'notification' table to hypertable: {e}")
        finally:
            conn.close()

def notify_order_execution(symbol, action, price, stop_loss, target_price, time,
                           exit_price=None, pnl=None, status=None):
    ensure_notifications_table()
    """
    Saves order execution/closure details to the database.
    For order execution (when status is not provided), default values are used:
      exit_price = 0.0, pnl = 0.0, status = "EXECUTED".
    """
    # Get the current time string
    execution_time_str = current_time()
    if status:
        message = (
            f"Order Closed:\n"
            f"Entry Price: {float(price):.2f}\n"
            f"Exit Price: {float(exit_price):.2f}\n"
            f"PnL: {float(pnl):.2f}\n"
            f"Status: {status.upper()}\n"
        )
    else:
        # Default values for an order executed (not closed) notification.
        exit_price = 0.0
        pnl = 0.0
        status = "EXECUTED"
        message = (
            f"Order Executed:\n"
            f"Price: {float(price):.2f}\n"
            f"Stop Loss: {float(stop_loss):.2f}\n"
            f"Target Price: {float(target_price):.2f}\n"
        )
    conn = get_db_connection()

    redis_key = "pipeline:exchange"
    data = redis_client.get(redis_key)
    exchange_mode = data.decode("utf-8")
    exchange_mode = exchange_mode.split('_')[-1]

    if conn:
        try:
            with conn.cursor() as cursor:
                insert_query = """
                    INSERT INTO notification (
                        symbol, action, price, stop_loss, target_price, 
                        execution_time, message, exit_price, pnl, status, exchange_mode
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                params = (
                    symbol,
                    action,
                    float(price),
                    float(stop_loss),
                    float(target_price),
                    execution_time_str,
                    message,
                    float(exit_price),
                    float(pnl),
                    status,
                    exchange_mode
                )
                cursor.execute(insert_query, params)
            conn.commit()
        except Exception as e:
            print(f"[ERROR] Failed to save notification: {e}")
        finally:
            conn.close()
# ensure_notifications_table()