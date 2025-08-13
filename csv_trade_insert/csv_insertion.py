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
        "%m/%d/%Y %H:%M",  # e.g., 6/27/2024 15:24
        "%-m/%-d/%Y %H:%M",  # e.g., 3/27/2025 15:25 (Linux/Mac)
        "%#m/%#d/%Y %H:%M",  # e.g., 3/27/2025 15:25 (Windows)
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
    df = pd.read_csv("active.csv")
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Updated unique constraint to prevent duplicates more effectively
    cursor.execute("ALTER TABLE public.notification ALTER COLUMN exit_price DROP NOT NULL")
    cursor.execute("ALTER TABLE public.notification ALTER COLUMN pnl DROP NOT NULL")

    # Drop old constraint if exists and create new one
    try:
        cursor.execute("ALTER TABLE public.notification DROP CONSTRAINT IF EXISTS unique_trade")
    except:
        pass

    try:
        # More comprehensive unique constraint
        cursor.execute(
            "ALTER TABLE public.notification ADD CONSTRAINT unique_trade_active UNIQUE (symbol, action, price, stop_loss, target_price, status, exchange_mode)"
        )
    except:
        pass  # Constraint already exists
    conn.commit()

    # Modified insert query with better conflict handling
    insert_query1 = """
        INSERT INTO public.notification (
            symbol, action, price, stop_loss, target_price,
            execution_time, message, created_at, seen,
            exit_price, pnl, status, exchange_mode
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, action, price, stop_loss, target_price, status, exchange_mode) 
        DO UPDATE SET
            execution_time = EXCLUDED.execution_time,
            message = EXCLUDED.message,
            created_at = EXCLUDED.created_at
        RETURNING id;
    """

    inserted_count = 0
    skipped_count = 0

    for index, row in df.iterrows():
        print(f"Processing row {index}...")

        # Skip rows with missing essential data
        if pd.isna(row['symbol']) or pd.isna(row['action']):
            print(f"Row {index} skipped: missing symbol or action")
            skipped_count += 1
            continue

        try:
            cursor.execute("SAVEPOINT sp1")

            # Check if this exact record already exists
            check_query = """
                SELECT COUNT(*) FROM public.notification 
                WHERE symbol = %s AND action = %s AND price = %s 
                AND stop_loss = %s AND target_price = %s 
                AND status = %s AND exchange_mode = %s
            """

            cursor.execute(check_query, (
                row['symbol'], row['action'], float(row['price']) if not pd.isna(row['price']) else None,
                float(row['stop_loss']) if not pd.isna(row['stop_loss']) else None,
                float(row['target_price']) if not pd.isna(row['target_price']) else None,
                row['status'], row['exchange_mode']
            ))

            existing_count = cursor.fetchone()[0]

            if existing_count > 0:
                print(f"Row {index} skipped: duplicate record already exists")
                cursor.execute("ROLLBACK TO SAVEPOINT sp1")
                skipped_count += 1
                continue

            if row['status'] in ('target_hit', 'stop_loss_hit'):
                message = f"Order Closed: Entry:{row['price']}, Exit:{row.get('exit_price', 'N/A')}, PnL:{row.get('pnl', 'N/A')}"
            else:
                message = f"Order Executed: Price:{row['price']}, SL:{row['stop_loss']}, Target:{row['target_price']}"

            execution_time = parse_date(row['executed_at'])
            candle_time = parse_date(row['candle_time'])

            # Validate dates
            if execution_time is None or candle_time is None:
                print(f"Row {index} skipped: invalid executed_at or candle_time")
                cursor.execute("ROLLBACK TO SAVEPOINT sp1")
                skipped_count += 1
                continue

            # Add random offset to execution_time
            execution_time = execution_time + pd.to_timedelta(random.randint(0, 59), unit='s')

            # Validate other required fields
            required_fields = ['price', 'stop_loss', 'target_price', 'status', 'exchange_mode']
            if any(pd.isna(row[field]) for field in required_fields):
                print(f"Row {index} skipped: missing required fields")
                cursor.execute("ROLLBACK TO SAVEPOINT sp1")
                skipped_count += 1
                continue

            cursor.execute(insert_query1, (
                row['symbol'], row['action'],
                float(row['price']), float(row['stop_loss']), float(row['target_price']),
                execution_time, message, candle_time, False,
                None,  # Always use None for exit_price in active trades
                None,  # Always use None for pnl in active trades
                row['status'], row['exchange_mode']
            ))

            result = cursor.fetchone()
            if result:
                cursor.execute("RELEASE SAVEPOINT sp1")
                inserted_count += 1
                print(f"Row {index} inserted successfully")
            else:
                cursor.execute("ROLLBACK TO SAVEPOINT sp1")
                skipped_count += 1
                print(f"Row {index} was a duplicate, skipped")

        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT sp1")
            skipped_count += 1
            print(f"Row {index} failed: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Active trade Notification Done! Inserted: {inserted_count}, Skipped: {skipped_count}")


def csv_history_notification():
    df = pd.read_csv("trade_history.csv")
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Ensure table constraints
    cursor.execute("ALTER TABLE public.notification ALTER COLUMN exit_price DROP NOT NULL")
    cursor.execute("ALTER TABLE public.notification ALTER COLUMN pnl DROP NOT NULL")

    # Drop old constraint if exists and create new one
    try:
        cursor.execute("ALTER TABLE public.notification DROP CONSTRAINT IF EXISTS unique_trade")
        cursor.execute("ALTER TABLE public.notification DROP CONSTRAINT IF EXISTS unique_trade_active")
    except:
        pass

    try:
        # Comprehensive unique constraint for history
        cursor.execute(
            "ALTER TABLE public.notification ADD CONSTRAINT unique_trade_history UNIQUE (symbol, action, price, stop_loss, target_price, status, exchange_mode, exit_price, pnl)"
        )
    except:
        pass  # Constraint already exists
    conn.commit()

    insert_query2 = """
        INSERT INTO public.notification (
            symbol, action, price, stop_loss, target_price,
            execution_time, message, created_at, seen,
            exit_price, pnl, status, exchange_mode
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, action, price, stop_loss, target_price, status, exchange_mode, exit_price, pnl) 
        DO UPDATE SET
            execution_time = EXCLUDED.execution_time,
            message = EXCLUDED.message,
            created_at = EXCLUDED.created_at
        RETURNING id;
    """

    inserted_count = 0
    skipped_count = 0

    for index, row in df.iterrows():
        print(f"Processing row {index}...")

        # Skip rows with missing essential data
        if pd.isna(row['symbol']) or pd.isna(row['action']):
            print(f"Row {index} skipped: missing symbol or action")
            skipped_count += 1
            continue

        try:
            cursor.execute("SAVEPOINT sp1")

            # Check if this exact record already exists
            check_query = """
                SELECT COUNT(*) FROM public.notification 
                WHERE symbol = %s AND action = %s AND price = %s 
                AND stop_loss = %s AND target_price = %s 
                AND status = %s AND exchange_mode = %s
                AND exit_price = %s AND pnl = %s
            """

            cursor.execute(check_query, (
                row['symbol'], row['action'],
                float(row['price']) if not pd.isna(row['price']) else None,
                float(row['stop_loss']) if not pd.isna(row['stop_loss']) else None,
                float(row['target_price']) if not pd.isna(row['target_price']) else None,
                row['status'], row['exchange_mode'],
                float(row['exit_price']) if not pd.isna(row['exit_price']) else None,
                float(row['pnl']) if not pd.isna(row['pnl']) else None
            ))

            existing_count = cursor.fetchone()[0]

            if existing_count > 0:
                print(f"Row {index} skipped: duplicate record already exists")
                cursor.execute("ROLLBACK TO SAVEPOINT sp1")
                skipped_count += 1
                continue

            # Create message
            if row['status'] in ('target_hit', 'stop_loss_hit'):
                message = (
                    "Order Closed:\n"
                    f"Entry Price: {row['price']}\n"
                    f"Exit Price: {row.get('exit_price', 'N/A')}\n"
                    f"PnL: {row.get('pnl', 'N/A')}\n"
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

            if execution_time is None or candle_time is None:
                print(f"Row {index} skipped: invalid date")
                cursor.execute("ROLLBACK TO SAVEPOINT sp1")
                skipped_count += 1
                continue

            # Add random seconds to execution_time
            execution_time = execution_time + pd.to_timedelta(random.randint(0, 59), unit='s')

            cursor.execute(insert_query2, (
                row['symbol'], row['action'],
                float(row['price']) if not pd.isna(row['price']) else None,
                float(row['stop_loss']) if not pd.isna(row['stop_loss']) else None,
                float(row['target_price']) if not pd.isna(row['target_price']) else None,
                execution_time, message, candle_time, False,
                float(row['exit_price']) if not pd.isna(row['exit_price']) else None,
                float(row['pnl']) if not pd.isna(row['pnl']) else None,
                row['status'], row['exchange_mode']
            ))

            result = cursor.fetchone()
            if result:
                cursor.execute("RELEASE SAVEPOINT sp1")
                inserted_count += 1
                print(f"Row {index} inserted successfully")
            else:
                cursor.execute("ROLLBACK TO SAVEPOINT sp1")
                skipped_count += 1
                print(f"Row {index} was a duplicate, skipped")

        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT sp1")
            skipped_count += 1
            print(f"Row {index} failed: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"History Notification Done! Inserted: {inserted_count}, Skipped: {skipped_count}")


# Optional: Function to clean existing duplicates
def remove_existing_duplicates():
    """Remove existing duplicate records from notification table"""
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    try:
        # Remove duplicates keeping only the first occurrence
        cursor.execute("""
            DELETE FROM public.notification 
            WHERE id NOT IN (
                SELECT MIN(id) 
                FROM public.notification 
                GROUP BY symbol, action, price, stop_loss, target_price, status, exchange_mode
            )
        """)

        deleted_count = cursor.rowcount
        conn.commit()
        print(f"Removed {deleted_count} duplicate records")

    except Exception as e:
        conn.rollback()
        print(f"Error removing duplicates: {e}")
    finally:
        cursor.close()
        conn.close()

# remove_existing_duplicates()
# active_data_insertion()
# csv_active_notification()
# History_data_insertion()
# csv_history_notification()