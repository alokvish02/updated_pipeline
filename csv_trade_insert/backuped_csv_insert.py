import psycopg2
import csv
from datetime import datetime
import psycopg2
import pandas as pd
from datetime import datetime
import random

# Database credentials
db_config = {
    "dbname": "trading_system",
    "user": "postgres",
    "password": "onealpha12345",
    "host": "localhost",
    "port": 5432,
}

# PostgreSQL upsert query with RETURNING for debugging
upsert_query = """
    INSERT INTO public.nse_utils_trade_history (
        util_type, symbol, id, candle_time, action, price, stop_loss,
        target_price, current_price, exit_price, pnl, status, executed_at, exchange_mode
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (util_type, symbol, candle_time)
    DO UPDATE SET
        action = EXCLUDED.action,
        price = EXCLUDED.price,
        stop_loss = EXCLUDED.stop_loss,
        target_price = EXCLUDED.target_price,
        current_price = EXCLUDED.current_price,
        exit_price = EXCLUDED.exit_price,
        pnl = EXCLUDED.pnl,
        status = EXCLUDED.status,
        executed_at = EXCLUDED.executed_at,
        exchange_mode = EXCLUDED.exchange_mode
"""

# def parse_date(date_str):
#     formats = [
#         "%d/%m/%Y %H:%M",  # e.g., 9/4/2024 12:29
#         "%d-%m-%Y %H:%M",  # e.g., 15-04-2024 09:19
#         "%m/%d/%Y %H:%M"   # e.g., 6/27/2024 15:24
#     ]
#     for fmt in formats:
#         try:
#             return datetime.strptime(date_str, fmt)
#         except ValueError:
#             continue
#     raise ValueError(f"Date string '{date_str}' does not match any expected format")

def parse_date(date_str):
    formats = [
        "%d/%m/%Y %H:%M",  # e.g., 9/4/2024 12:29
        "%d-%m-%Y %H:%M",  # e.g., 15-04-2024 09:19
        "%m/%d/%Y %H:%M"   # e.g., 6/27/2024 15:24
    ]
    for fmt in formats:
        try:
            # Parse the input string with one of the formats
            parsed_date = datetime.strptime(date_str, fmt)
            # Convert to the desired output format: %d-%m-%Y %H:%M
            return datetime.strptime(parsed_date.strftime("%d-%m-%Y %H:%M"), "%d-%m-%Y %H:%M")
        except ValueError:
            continue
    raise ValueError(f"Date string '{date_str}' does not match any expected format")


def process_row(row):
    # Initialize list to hold processed data
    processed = []
    # print(f"Processing row: {row}")  # Debugging line
    # Iterate through each value in the row
    for idx, value in enumerate(row):
        value = value.strip() if value else None  # Strip spaces and handle empty values

        if idx in [3, 12]:  # candle_time and executed_at are date fields
            processed.append(parse_date(value) if value else None)
        elif idx in [5, 6, 7, 8, 9, 10]:  # Numeric fields (price, stop_loss, target_price, etc.)
            try:
                processed.append(float(value) if value else None)  # Convert to float or None
            except (ValueError, TypeError):
                processed.append(None)  # In case of conversion failure, append None
        elif idx == 2:  # id field
            try:
                processed.append(int(value) if value else 1)  # Convert to int or set default to 1
            except ValueError:
                processed.append(1)  # Default value for id in case of conversion failure
        elif idx == 4:  # action field (should not be empty)
            processed.append(value if value else 'UNKNOWN')  # Default to 'UNKNOWN' if missing
        else:
            processed.append(value)  # For other fields, just append the value (if valid)
    # print(f"Processed row: {processed}")  # Debugging line
    return processed



def active_data_insertion():
    # File path from uploaded file
    #change csv name if you insert active
    # csv_file_path = 'C:/Users/devel/Downloads/active.csv'
    csv_file_path = 'active.csv'

    # Start processing
    try:
        # Establish connection and set autocommit
        conn = psycopg2.connect(**db_config)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)  # Ensure immediate commit
        cursor = conn.cursor()

        # List to hold processed rows
        rows_to_insert = []

        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            # print("reader", reader)
            row_count = 1
            for row in reader:
                # print(f"Row {row_count}: {row}")
                # if len(row) != 14:
                #     continue

                # if not row[0].strip():
                #     continue
                print(f"Processing row {row_count}: {row}")
                processed = process_row(row)
                rows_to_insert.append(processed)
                row_count += 1


            upsert_active = """
                INSERT INTO public.nse_utils_trade (
                    util_type, symbol, id, candle_time, action, price, stop_loss,
                    target_price, current_price, exit_price, pnl, status, executed_at, exchange_mode
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (util_type, symbol, candle_time)
                DO UPDATE SET
                    action = EXCLUDED.action,
                    price = EXCLUDED.price,
                    stop_loss = EXCLUDED.stop_loss,
                    target_price = EXCLUDED.target_price,
                    current_price = EXCLUDED.current_price,
                    exit_price = EXCLUDED.exit_price,
                    pnl = EXCLUDED.pnl,
                    status = EXCLUDED.status,
                    executed_at = EXCLUDED.executed_at,
                    exchange_mode = EXCLUDED.exchange_mode
            """


        # Bulk insert all rows at once using executemany
        if rows_to_insert:
            print(f"Inserting {len(rows_to_insert)} rows into the database.")
            cursor.executemany(upsert_active, rows_to_insert)

            # Commit after insertion
            conn.commit()

    except Exception as e:
        print(f"❌ Error: {e}")

        # conn.rollback()

    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()


def History_data_insertion():
    # File path from uploaded file
    # change csv name if you insert active
    # csv_file_path = 'C:/Users/devel/Downloads/TRADE_HISTORY.csv'
    csv_file_path = 'trade_history.csv'

    # Start processing
    try:
        # Establish connection and set autocommit
        conn = psycopg2.connect(**db_config)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)  # Ensure immediate commit
        cursor = conn.cursor()

        # List to hold processed rows
        rows_to_insert = []

        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            print("reader", reader)
            row_count = 1
            for row in reader:
                print(f"Row {row_count}: {row}")
                if len(row) != 14:
                    continue

                if not row[0].strip():  # util_type is essential
                    continue

                processed = process_row(row)
                rows_to_insert.append(processed)
                row_count += 1

        # Bulk insert all rows at once using executemany
        if rows_to_insert:
            cursor.executemany(upsert_query, rows_to_insert)

            # Commit after insertion
            conn.commit()

    except Exception as e:
        print(f"❌ Error: {e}")

        # conn.rollback()

    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()



def csv_active_notification():
    # Load your CSV
    df = pd.read_csv("active.csv")

    # Connect to the database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Prepare the INSERT query
    # insert_query1 = """
    #     INSERT INTO public.notification (
    #         symbol, action, price, stop_loss, target_price,
    #         execution_time, message, created_at, seen,
    #         exit_price, pnl, status, exchange_mode
    #     )
    #     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    # """
    insert_query1 = """
        INSERT INTO public.notification (
            symbol, action, price, stop_loss, target_price,
            execution_time, message, created_at, seen,
            exit_price, pnl, status, exchange_mode
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (action, symbol, created_at) DO NOTHING;
    """
    # Loop through each row
    for index, row in df.iterrows():
        print(f"Processing row {index}...")

        # Skip completely empty rows
        if pd.isna(row['symbol']) or pd.isna(row['action']):
            print(f"Skipping empty row {index}")
            continue

        # Create a message field if needed
        if row['status'] in ('target_hit', 'stop_loss_hit'):
            message = (
                "Order Closed:\n"
                f"Entry Price: {row['price']}\n"
                f"Exit Price: {row['exit_price']}\n"
                f"PnL: {row['pnl']}\n"
                f"Status: {row['status'].upper()}"
            )
        else:
            message = (
                "Order Executed:\n"
                f"Price: {row['price']}\n"
                f"Stop Loss: {row['stop_loss']}\n"
                f"Target Price: {row['target_price']}"
            )


        # execution_time = pd.to_datetime(row['executed_at'], format="%d-%m-%Y %H:%M")
        # candle_time = pd.to_datetime(row['candle_time'], format="%d-%m-%Y %H:%M")
        execution_time = parse_date(row['executed_at'])
        candle_time = parse_date(row['candle_time'])

        # 🔥 Add random seconds (0 to 59)
        execution_time = execution_time + pd.to_timedelta(random.randint(0, 59), unit='s')
        candle_time = candle_time + pd.to_timedelta(random.randint(0, 59), unit='s')

        cursor.execute(insert_query1, (
            row['symbol'],
            row['action'],
            row['price'] if not pd.isna(row['price']) else None,
            row['stop_loss'] if not pd.isna(row['stop_loss']) else None,
            row['target_price'] if not pd.isna(row['target_price']) else None,
            datetime.now(),
            message,
            candle_time,
            False,  # Seen = False
            row['exit_price'] if not pd.isna(row['exit_price']) else None,
            None,
            row['status'],
            row['exchange_mode']
        ))
        print("insert_query1", insert_query1)

    # Commit changes and close
    conn.commit()
    cursor.close()
    conn.close()

    print("All data inserted successfully!")


def csv_history_notification():
    # Load your CSV
    # df = pd.read_csv(r"C:/Users/devel/Downloads/TRADE_HISTORY.csv")
    df = pd.read_csv("trade_history.csv")

    # Connect to the database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    insert_query2 = """
        INSERT INTO public.notification (
            symbol, action, price, stop_loss, target_price,
            execution_time, message, created_at, seen,
            exit_price, pnl, status, exchange_mode
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    # Loop through each row
    for index, row in df.iterrows():
        print(f"Processing row {index}...")

        # Skip completely empty rows
        if pd.isna(row['symbol']) or pd.isna(row['action']):
            print(f"Skipping empty row {index}")
            continue

        # Create a message field if needed
        if row['status'] in ('target_hit', 'stop_loss_hit'):
            message = (
                "Order Closed:\n"
                f"Entry Price: {row['price']}\n"
                f"Exit Price: {row['exit_price']}\n"
                f"PnL: {row['pnl']}\n"
                f"Status: {row['status'].upper()}"
            )
        else:
            message = (
                "Order Executed:\n"
                f"Price: {row['price']}\n"
                f"Stop Loss: {row['stop_loss']}\n"
                f"Target Price: {row['target_price']}"
            )


        execution_time = parse_date(row['executed_at'])
        candle_time = parse_date(row['candle_time'])

        # execution_time = pd.to_datetime(row['executed_at'], format="%d-%m-%Y %H:%M")
        # candle_time = pd.to_datetime(row['candle_time'], format="%d-%m-%Y %H:%M")

        # 🔥 Add random seconds (0 to 59)
        execution_time = execution_time + pd.to_timedelta(random.randint(0, 59), unit='s')
        candle_time = candle_time + pd.to_timedelta(random.randint(0, 59), unit='s')

        cursor.execute(insert_query2, (
            row['symbol'],
            row['status'],
            row['price'] if not pd.isna(row['price']) else None,
            row['stop_loss'] if not pd.isna(row['stop_loss']) else None,
            row['target_price'] if not pd.isna(row['target_price']) else None,
            None,
            message,
            execution_time,
            False,  # Seen = False
            row['exit_price'] if not pd.isna(row['exit_price']) else None,
            row['pnl'] if not pd.isna(row['pnl']) else None,
            row['status'],
            row['exchange_mode']
        ))

    # Commit changes and close
    conn.commit()
    cursor.close()
    conn.close()

    print("All data inserted successfully!")



# active_data_insertion()
csv_active_notification()
# History_data_insertion()
# csv_history_notification()