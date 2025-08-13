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

    # "nse_stocks",
    # "nse_spreads"
    # "snp_spreads"
    # "etf_spreads"
    "binance_stocks",
    "binance_spreads"
]

intervals = {
    "1m":  ("1 minutes", "2 years",  "10 minutes"),
    "5m":  ("5 minutes", "2 years",  "30 minutes"),
    "15m": ("15 minutes","3 years",  "1 hour"),
    "30m": ("30 minutes","5 years",  "2 hours"),
    "1d":  ("1 day","10 years", "1 day")
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
        # print(f"Dropping continuous aggregate policy for {view_name}")
        cur.execute(f"""
        SELECT remove_continuous_aggregate_policy('{view_name}');
        """)

    except Exception as e:
        print(f"Error removing policy for {view_name}: {e}")

# Function to drop materialized view if it exists
def drop_materialized_view(view_name):
    try:
        # print(f"Dropping materialized view {view_name}")
        cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name};")
    except Exception as e:
        print(f"Error dropping view {view_name}: {e}")



# Iterate over tables and intervals
for table in tables:
    for label, (bucket_size, start_offset, schedule) in intervals.items():
        view_name = f"{table}_{label}"
        # print(f"Processing view: {view_name}")

        # Remove existing policy and materialized view if they exist
        if policy_exists(view_name):
            remove_policy(view_name)

        if view_exists(view_name):
            drop_materialized_view(view_name)

        # Create materialized view
        try:
            # print(f"Creating materialized view: {view_name}")
            cur.execute(f"""
            CREATE MATERIALIZED VIEW {view_name}
            WITH (timescaledb.continuous) AS
            SELECT
                symbol,
                time_bucket('{bucket_size}', timestamp) AS bucket,
                FIRST(open, timestamp) AS open,
                MAX(high) AS high,
                MIN(low) AS low,
                LAST(close, timestamp) AS close,
                SUM(volume) AS volume
            FROM {table}
            GROUP BY symbol, bucket;
            """)
        except Exception as e:
            pass
            # print(f"Error creating view {view_name}: {e}")

        # Add continuous aggregate policy
        try:
            # print(f"Adding continuous aggregate policy for {view_name}")
            cur.execute(f"""
            SELECT add_continuous_aggregate_policy('{view_name}',
                start_offset => INTERVAL '{start_offset}',
                end_offset => INTERVAL '1 minute',
                schedule_interval => INTERVAL '{schedule}');
            """)
        except DuplicateObject:
            pass
            # print(f"Policy already exists for {view_name}. Skipping.")
        except Exception as e:
            print(f"Error adding policy for {view_name}: {e}")

cur.close()
conn.close()

print("✅ All continuous aggregates processed.")


### change policy it is set policy for refresh every 1m
# import psycopg2
# from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
# from psycopg2.errors import DuplicateObject
#
# TRADING_SYSTEM_CONN_PARAMS = {
#     "dbname": "trading_system",
#     "user": "postgres",
#     "password": "onealpha12345",
#     "host": "localhost",
#     "port": 5432
# }
#
# tables = [
#
#     # "nse_stocks",
#     # "nse_spreads"
#     # "snp_spreads"
#     # "etf_spreads"
#     "binance_stocks",
#     "binance_spreads"
# ]
#
# intervals = {
#     "1m":  ("1 minute", "2 years"),
#     "5m":  ("5 minutes", "2 years"),
#     "15m": ("15 minutes", "3 years"),
#     "30m": ("30 minutes", "5 years"),
#     "1d":  ("1 day", "10 years")
# }
#
# conn = psycopg2.connect(**TRADING_SYSTEM_CONN_PARAMS)
# conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
# cur = conn.cursor()
#
# # ---------------------- Utility Functions ---------------------- #
#
# def view_exists(view_name):
#     cur.execute("""
#     SELECT matviewname FROM pg_matviews
#     WHERE schemaname = 'public' AND matviewname = %s;
#     """, (view_name,))
#     return cur.fetchone() is not None
#
# def policy_exists(view_name):
#     cur.execute("""
#     SELECT * FROM timescaledb_information.continuous_aggregates
#     WHERE view_name = %s;
#     """, (view_name,))
#     return cur.fetchone() is not None
#
# def remove_policy(view_name):
#     try:
#         print(f"🗑️ Removing policy for {view_name}")
#         cur.execute(f"SELECT remove_continuous_aggregate_policy('{view_name}');")
#     except Exception as e:
#         print(f"⚠️ Error removing policy for {view_name}: {e}")
#
# def drop_materialized_view(view_name):
#     try:
#         print(f"🗑️ Dropping view {view_name}")
#         cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name};")
#     except Exception as e:
#         print(f"⚠️ Error dropping view {view_name}: {e}")
#
# # ------------------ Create Continuous Aggregates ------------------ #
#
# for table in tables:
#     for label, (bucket_size, start_offset) in intervals.items():
#         view_name = f"{table}_{label}"
#         print(f"\n🔄 Processing: {view_name}")
#
#         # Cleanup existing view and policy
#         if policy_exists(view_name):
#             remove_policy(view_name)
#         if view_exists(view_name):
#             drop_materialized_view(view_name)
#
#         # Create view
#         try:
#             print(f"✅ Creating materialized view: {view_name}")
#             cur.execute(f"""
#             CREATE MATERIALIZED VIEW {view_name}
#             WITH (timescaledb.continuous) AS
#             SELECT
#                 symbol,
#                 time_bucket('{bucket_size}', timestamp) AS bucket,
#                 FIRST(open, timestamp) AS open,
#                 MAX(high) AS high,
#                 MIN(low) AS low,
#                 LAST(close, timestamp) AS close,
#                 SUM(volume) AS volume
#             FROM {table}
#             GROUP BY symbol, bucket;
#             """)
#         except Exception as e:
#             print(f"❌ Error creating view {view_name}: {e}")
#
#         # Add 1-minute refresh policy
#         try:
#             print(f"🕒 Adding 1-minute policy for {view_name}")
#             cur.execute(f"""
#             SELECT add_continuous_aggregate_policy('{view_name}',
#                 start_offset => INTERVAL '{start_offset}',
#                 end_offset => INTERVAL '1 minute',
#                 schedule_interval => INTERVAL '1 minute');
#             """)
#         except DuplicateObject:
#             print(f"⏩ Policy already exists for {view_name}. Skipping.")
#         except Exception as e:
#             print(f"❌ Error adding policy for {view_name}: {e}")
#
# cur.close()
# conn.close()
# print("\n✅ All continuous aggregates processed.")