import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.errors import DuplicateObject, UndefinedColumn, UndefinedTable


class TradingSystemPeriodViewsManager:
    def __init__(self):
        self.TRADING_SYSTEM_CONN_PARAMS = {
            "dbname": "trading_system",
            "user": "postgres",
            "password": "onealpha12345",
            "host": "localhost",
            "port": 5432
        }

        # Base tables that need to be converted to hypertables
        self.base_tables = [
            "binance_utils_trade_history",
            "binance_utils_trade",
            "nse_utils_trade_history",
            "nse_utils_trade"
        ]

        self.period_days = {
            "1w": 7,
            "1m": 30,
            "3m": 90,
            "6m": 180,
            "1y": 364,
            "all": 999999999
        }

        self.conn = None
        self.cur = None

        # SQL for dropping existing views
        self.drop_existing_views_sql = """
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

    def _connect(self):
        """Establish database connection"""
        self.conn = psycopg2.connect(**self.TRADING_SYSTEM_CONN_PARAMS)
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.cur = self.conn.cursor()

    def _disconnect(self):
        """Close database connection"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    def is_hypertable(self, table_name):
        """Check if table is already a hypertable"""
        try:
            self.cur.execute("""
            SELECT * FROM timescaledb_information.hypertables 
            WHERE hypertable_name = %s;
            """, (table_name,))
            return self.cur.fetchone() is not None
        except UndefinedColumn:
            self.cur.execute("""
            SELECT * FROM _timescaledb_catalog.hypertable 
            WHERE table_name = %s;
            """, (table_name,))
            return self.cur.fetchone() is not None

    def materialized_view_exists(self, view_name):
        """Check if materialized view exists"""
        self.cur.execute("""
        SELECT matviewname FROM pg_matviews 
        WHERE schemaname = 'public' AND matviewname = %s;
        """, (view_name.lower(),))
        return self.cur.fetchone() is not None

    def convert_to_hypertable(self, table_name):
        """Convert table to hypertable if not already converted"""
        if not self.is_hypertable(table_name):
            print(f"Converting {table_name} to hypertable")
            try:
                self.cur.execute(f"""
                SELECT create_hypertable('{table_name}', 'candle_time',
                    if_not_exists => TRUE);
                """)
                print(f"✅ Successfully converted {table_name} to hypertable")
            except Exception as e:
                print(f"❌ Error converting {table_name} to hypertable: {e}")

    def drop_continuous_aggregate(self, view_name):
        """Drop continuous aggregate materialized view"""
        try:
            print(f"Dropping continuous aggregate {view_name}")
            self.cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;")
        except Exception as e:
            print(f"Error dropping view {view_name}: {e}")

    def create_combined_view(self, table_prefix):
        """Create combined materialized view from trade_history and trade tables"""
        view_name = f"{table_prefix}_combined_trades"
        if self.materialized_view_exists(view_name):
            self.cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;")

        # self.cur.execute(f"""
        # CREATE MATERIALIZED VIEW {view_name} AS
        # SELECT * FROM {table_prefix}_trade_history
        # UNION ALL
        # SELECT * FROM {table_prefix}_trade;
        # """)
        self.cur.execute(f"""
        CREATE MATERIALIZED VIEW {view_name} AS
        SELECT * FROM {table_prefix}_trade_history
        """)

        self.cur.execute(f"""
        CREATE INDEX idx_{view_name}_time ON {view_name}(candle_time);
        """)

        print(f"✅ Created combined view: {view_name}")
        return view_name

    def convert_base_tables_to_hypertables(self):
        """Convert all base tables to hypertables"""
        print("\nConverting base tables to hypertables...")
        for table in self.base_tables:
            self.convert_to_hypertable(table)

    # def drop_existing_combined_views(self):
    #     """Drop existing combined views with CASCADE"""
    #     print("\nDropping existing combined views if they exist...")
    #     self.cur.execute(self.drop_existing_views_sql)

    def create_combined_views(self):
        """Create combined materialized views for both exchanges"""
        print("\nCreating combined materialized views...")
        binance_combined = self.create_combined_view("binance_utils")
        nse_combined = self.create_combined_view("nse_utils")
        return binance_combined, nse_combined

    def create_period_based_views(self, combined_views):
        """Create period-based materialized views"""
        print("\nCreating period-based materialized views...")

        for view_name in combined_views:
            table_prefix = "binance_utils" if "binance" in view_name else "nse_utils"

            for label, days in self.period_days.items():
                period_view_name = f"{table_prefix}_trade_history_and_trade_period_{label}"

                if self.materialized_view_exists(period_view_name):
                    self.drop_continuous_aggregate(period_view_name)

                try:
                    where_clause = "" if label == "all" else f"WHERE candle_time >= now() - INTERVAL '{days} days'"
                    print(f"Creating materialized view: {period_view_name}")

                    self.cur.execute(f"""
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

                    self.cur.execute(f"""
                    CREATE INDEX idx_{period_view_name}_bucket ON {period_view_name}(bucket);
                    """)

                    print(f"✅ Successfully created materialized view: {period_view_name}")
                except Exception as e:
                    print(f"❌ Error creating view {period_view_name}: {e}")

    def create_period_views(self):
        """Main function to create all period-based views"""
        try:
            self._connect()

            # Step 1: Convert base tables to hypertables
            self.convert_base_tables_to_hypertables()

            # Step 1.5: Drop existing combined views (and dependent views) with CASCADE
            # self.drop_existing_combined_views()

            # Step 2: Create combined materialized views
            combined_views = self.create_combined_views()

            # Step 3: Create period-based materialized views
            self.create_period_based_views(combined_views)

            print("\n✅ Completed creating period-based views")

        finally:
            self._disconnect()


# For external usage - single function call
def create_tiles_views():
    """External function to create all period-based views"""
    manager = TradingSystemPeriodViewsManager()
    manager.create_period_views()

# create_tiles_views()