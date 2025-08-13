import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.errors import DuplicateObject

TRADING_SYSTEM_CONN_PARAMS = {
    "dbname": "trading_system",
    "user": "postgres",
    "password": "onealpha12345",
    "host": "localhost",
    "port": 5432
}

tables = [
    "nse_utils_trade_history",
    "binance_utils_trade_history"
]

# Setting a fixed interval for 1 hour as per your request
intervals = {
    "1h": ("1 hour", "10 years", "1 hour")
}

conn = psycopg2.connect(**TRADING_SYSTEM_CONN_PARAMS)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()

# Function to check if a materialized view exists
def view_exists(view_name):
    cur.execute("""
    SELECT matviewname
    FROM pg_matviews
    WHERE schemaname = 'public' AND matviewname = %s;
    """, (view_name,))
    return cur.fetchone() is not None

# Function to check if the policy exists
def policy_exists(view_name):
    cur.execute("""
    SELECT *
    FROM timescaledb_information.continuous_aggregates
    WHERE view_name = %s;
    """, (view_name,))
    return cur.fetchone() is not None

# Function to remove continuous aggregate policy
def remove_policy(view_name):
    try:
        print(f"Dropping continuous aggregate policy for {view_name}")
        cur.execute(f"""
        SELECT remove_continuous_aggregate_policy('{view_name}');
        """)
    except Exception as e:
        print(f"Error removing policy for {view_name}: {e}")

# Function to drop materialized view if it exists
def drop_materialized_view(view_name):
    try:
        print(f"Dropping materialized view {view_name}")
        cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name};")
    except Exception as e:
        print(f"Error dropping view {view_name}: {e}")

# Iterate over the new tables and create views with 1 hour interval
for table in tables:
    for label, (bucket_size, start_offset, schedule) in intervals.items():
        view_name = f"{table}_{label}"
        print(f"Processing view: {view_name}")

        # Remove existing policy and materialized view if they exist
        if policy_exists(view_name):
            remove_policy(view_name)

        if view_exists(view_name):
            drop_materialized_view(view_name)

        # Use the provided column names
        time_column = "candle_time"  # Timestamp column
        entry_price_column = "price"  # Price (entry price) column
        exit_price_column = "exit_price"  # Exit price column
        pnl_column = "pnl"  # Profit and Loss column
        stop_loss_column = "stop_loss"  # Stop loss column
        target_price_column = "target_price"  # Target price column
        symbol_column = "symbol"  # Symbol column

        # Create materialized view
        try:
            print(f"Creating materialized view: {view_name}")
            cur.execute(f"""
            CREATE MATERIALIZED VIEW {view_name}
            WITH (timescaledb.continuous) AS
            SELECT
                {symbol_column},
                time_bucket('{bucket_size}', {time_column}) AS bucket,
                FIRST({entry_price_column}, {time_column}) AS open,
                MAX({exit_price_column}) AS high,
                MIN({stop_loss_column}) AS low,
                LAST({target_price_column}, {time_column}) AS close,
                SUM({pnl_column}) AS volume
            FROM {table}
            GROUP BY {symbol_column}, bucket;
            """)
        except Exception as e:
            print(f"Error creating view {view_name}: {e}")

        # Add continuous aggregate policy
        try:
            print(f"Adding continuous aggregate policy for {view_name}")
            cur.execute(f"""
                SELECT add_continuous_aggregate_policy('{view_name}',
                    start_offset => INTERVAL '{start_offset}',
                    end_offset => INTERVAL '1 minute',  -- You can change this as needed
                    schedule_interval => INTERVAL '30 minutes');
            """)
        except DuplicateObject:
            print(f"Policy already exists for {view_name}. Skipping.")
        except Exception as e:
            print(f"Error adding policy for {view_name}: {e}")

cur.close()
conn.close()

print("✅ All continuous aggregates processed.")
