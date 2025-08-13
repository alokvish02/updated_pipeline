import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.errors import DuplicateObject, UndefinedColumn, UndefinedTable

TRADING_SYSTEM_CONN_PARAMS = {
    "dbname": "trading_system",
    "user": "postgres",
    "password": "onealpha12345",
    "host": "localhost",
    "port": 5432
}



"""-- delete views for dasbord chart tile
DO $$
DECLARE
    view_name text;
    table_prefix text;
    period_label text;
    periods text[] := ARRAY['1w', '1m', '3m', '6m', '1y', 'all'];
    prefixes text[] := ARRAY['binance_utils', 'nse_utils'];
BEGIN
    -- Drop period-based materialized views
    FOREACH table_prefix IN ARRAY prefixes LOOP
        FOREACH period_label IN ARRAY periods LOOP
            view_name := format('%s_trade_history_and_trade_period_%s', table_prefix, period_label);
            EXECUTE format('DROP MATERIALIZED VIEW IF EXISTS %s CASCADE;', view_name);
            RAISE NOTICE 'Dropped materialized view: %', view_name;
        END LOOP;
    END LOOP;

    -- Drop combined materialized views
    FOREACH table_prefix IN ARRAY prefixes LOOP
        view_name := format('%s_combined_trades', table_prefix);
        EXECUTE format('DROP MATERIALIZED VIEW IF EXISTS %s CASCADE;', view_name);
        RAISE NOTICE 'Dropped materialized view: %', view_name;
    END LOOP;
END
$$;"""

# Base tables that need to be converted to hypertables
base_tables = [
    "binance_utils_trade_history",
    "binance_utils_trade",
    "nse_utils_trade_history",
    "nse_utils_trade"
]

period_days = {
    "1w": 7,
    "1m": 30,
    "3m": 90,
    "6m": 180,
    "1y": 364,
    "all": 999999999
}

conn = psycopg2.connect(**TRADING_SYSTEM_CONN_PARAMS)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()

def is_hypertable(table_name):
    try:
        cur.execute("""
        SELECT * FROM timescaledb_information.hypertables 
        WHERE hypertable_name = %s;
        """, (table_name,))
        return cur.fetchone() is not None
    except UndefinedColumn:
        cur.execute("""
        SELECT * FROM _timescaledb_catalog.hypertable 
        WHERE table_name = %s;
        """, (table_name,))
        return cur.fetchone() is not None

def materialized_view_exists(view_name):
    cur.execute("""
    SELECT matviewname FROM pg_matviews 
    WHERE schemaname = 'public' AND matviewname = %s;
    """, (view_name.lower(),))
    return cur.fetchone() is not None

def convert_to_hypertable(table_name):
    if not is_hypertable(table_name):
        print(f"Converting {table_name} to hypertable")
        try:
            cur.execute(f"""
            SELECT create_hypertable('{table_name}', 'candle_time',
                if_not_exists => TRUE);
            """)
            print(f"✅ Successfully converted {table_name} to hypertable")
        except Exception as e:
            print(f"❌ Error converting {table_name} to hypertable: {e}")

def drop_continuous_aggregate(view_name):
    try:
        print(f"Dropping continuous aggregate {view_name}")
        cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;")
    except Exception as e:
        print(f"Error dropping view {view_name}: {e}")

def create_combined_view(table_prefix):
    view_name = f"{table_prefix}_combined_trades"
    if materialized_view_exists(view_name):
        cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;")
    cur.execute(f"""
    CREATE MATERIALIZED VIEW {view_name} AS
    SELECT * FROM {table_prefix}_trade_history
    UNION ALL
    SELECT * FROM {table_prefix}_trade;
    """)
    cur.execute(f"""
    CREATE INDEX idx_{view_name}_time ON {view_name}(candle_time);
    """)
    print(f"✅ Created combined view: {view_name}")
    return view_name

# Step 1: Convert base tables to hypertables
print("\nConverting base tables to hypertables...")
for table in base_tables:
    convert_to_hypertable(table)

# Step 1.5: Drop existing combined views (and dependent views) with CASCADE
print("\nDropping existing combined views if they exist...")
drop_existing_views_sql = """
DO $$
BEGIN
    -- Drop binance_utils_combined_trades
    IF EXISTS (
        SELECT 1 FROM pg_matviews WHERE matviewname = 'binance_utils_combined_trades'
    ) THEN
        EXECUTE 'DROP MATERIALIZED VIEW binance_utils_combined_trades CASCADE';
    ELSIF EXISTS (
        SELECT 1 FROM pg_views WHERE viewname = 'binance_utils_combined_trades'
    ) THEN
        EXECUTE 'DROP VIEW binance_utils_combined_trades CASCADE';
    END IF;

    -- Drop nse_utils_combined_trades
    IF EXISTS (
        SELECT 1 FROM pg_matviews WHERE matviewname = 'nse_utils_combined_trades'
    ) THEN
        EXECUTE 'DROP MATERIALIZED VIEW nse_utils_combined_trades CASCADE';
    ELSIF EXISTS (
        SELECT 1 FROM pg_views WHERE viewname = 'nse_utils_combined_trades'
    ) THEN
        EXECUTE 'DROP VIEW nse_utils_combined_trades CASCADE';
    END IF;
END
$$;
"""
cur.execute(drop_existing_views_sql)

# Step 2: Create combined materialized views
print("\nCreating combined materialized views...")
binance_combined = create_combined_view("binance_utils")
nse_combined = create_combined_view("nse_utils")

# Step 3: Create period-based materialized views
print("\nCreating period-based materialized views...")
for view_name in [binance_combined, nse_combined]:
    table_prefix = "binance_utils" if "binance" in view_name else "nse_utils"

    for label, days in period_days.items():
        period_view_name = f"{table_prefix}_trade_history_and_trade_period_{label}"

        if materialized_view_exists(period_view_name):
            drop_continuous_aggregate(period_view_name)

        try:
            where_clause = "" if label == "all" else f"WHERE candle_time >= now() - INTERVAL '{days} days'"
            print(f"Creating materialized view: {period_view_name}")
            cur.execute(f"""
            CREATE MATERIALIZED VIEW {period_view_name} AS
            SELECT 
                symbol, 
                exchange_mode, 
                time_bucket('1 day', candle_time) AS bucket,
                SUM(pnl) AS total_pnl, 
                COUNT(*) FILTER (WHERE status = 'active') AS active_count,
                COUNT(*) FILTER (WHERE status != 'active') AS closed_count,
                AVG(EXTRACT(EPOCH FROM (executed_at - candle_time))) AS avg_deal_time_secs
            FROM {view_name}
            {where_clause}
            GROUP BY symbol, exchange_mode, bucket;
            """)
            cur.execute(f"""
            CREATE INDEX idx_{period_view_name}_bucket ON {period_view_name}(bucket);
            """)
            print(f"✅ Successfully created materialized view: {period_view_name}")
        except Exception as e:
            print(f"❌ Error creating view {period_view_name}: {e}")

cur.close()
conn.close()
print("\n✅ Completed creating period-based views")
