import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.errors import DuplicateObject


class TradingSystemAggregatesManager:
    def __init__(self):
        self.TRADING_SYSTEM_CONN_PARAMS = {
            "dbname": "trading_system",
            "user": "postgres",
            "password": "onealpha12345",
            "host": "localhost",
            "port": 5432
        }

        self.tables = [
            "nse_utils_trade_history",
            "binance_utils_trade_history"
        ]

        # Setting a fixed interval for 1 hour as per your request
        self.intervals = {
            "1h": ("1 hour", "10 years", "1 hour")
        }

        self.conn = None
        self.cur = None

        # Column mappings
        self.time_column = "candle_time"
        self.entry_price_column = "price"
        self.exit_price_column = "exit_price"
        self.pnl_column = "pnl"
        self.stop_loss_column = "stop_loss"
        self.target_price_column = "target_price"
        self.symbol_column = "symbol"

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

    def view_exists(self, view_name):
        """Function to check if a materialized view exists"""
        self.cur.execute("""
        SELECT matviewname
        FROM pg_matviews
        WHERE schemaname = 'public' AND matviewname = %s;
        """, (view_name,))
        return self.cur.fetchone() is not None

    def policy_exists(self, view_name):
        """Function to check if the policy exists"""
        self.cur.execute("""
        SELECT *
        FROM timescaledb_information.continuous_aggregates
        WHERE view_name = %s;
        """, (view_name,))
        return self.cur.fetchone() is not None

    def remove_policy(self, view_name):
        """Function to remove continuous aggregate policy"""
        try:
            print(f"Dropping continuous aggregate policy for {view_name}")
            self.cur.execute(f"""
            SELECT remove_continuous_aggregate_policy('{view_name}');
            """)
        except Exception as e:
            print(f"Error removing policy for {view_name}: {e}")

    def drop_materialized_view(self, view_name):
        """Function to drop materialized view if it exists"""
        try:
            print(f"Dropping materialized view {view_name}")
            self.cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name};")
        except Exception as e:
            print(f"Error dropping view {view_name}: {e}")

    def create_materialized_view(self, view_name, table, bucket_size):
        """Create materialized view"""
        try:
            print(f"Creating materialized view: {view_name}")
            self.cur.execute(f"""
            CREATE MATERIALIZED VIEW {view_name}
            WITH (timescaledb.continuous) AS
            SELECT
                {self.symbol_column},
                time_bucket('{bucket_size}', {self.time_column}) AS bucket,
                FIRST({self.entry_price_column}, {self.time_column}) AS open,
                MAX({self.exit_price_column}) AS high,
                MIN({self.stop_loss_column}) AS low,
                LAST({self.target_price_column}, {self.time_column}) AS close,
                SUM({self.pnl_column}) AS volume
            FROM {table}
            GROUP BY {self.symbol_column}, bucket;
            """)
        except Exception as e:
            print(f"Error creating view {view_name}: {e}")

    def add_aggregate_policy(self, view_name, start_offset):
        """Add continuous aggregate policy"""
        try:
            print(f"Adding continuous aggregate policy for {view_name}")
            self.cur.execute(f"""
                SELECT add_continuous_aggregate_policy('{view_name}',
                    start_offset => INTERVAL '{start_offset}',
                    end_offset => INTERVAL '1 minute',  -- You can change this as needed
                    schedule_interval => INTERVAL '30 minutes');
            """)
        except DuplicateObject:
            print(f"Policy already exists for {view_name}. Skipping.")
        except Exception as e:
            print(f"Error adding policy for {view_name}: {e}")

    def create_ds_chart_view(self):
        """Main function to create all continuous aggregates and policies"""
        try:
            self._connect()

            # Iterate over the new tables and create views with 1 hour interval
            for table in self.tables:
                for label, (bucket_size, start_offset, schedule) in self.intervals.items():
                    view_name = f"{table}_{label}"
                    print(f"Processing view: {view_name}")

                    # Remove existing policy and materialized view if they exist
                    if self.policy_exists(view_name):
                        self.remove_policy(view_name)

                    if self.view_exists(view_name):
                        self.drop_materialized_view(view_name)

                    # Create materialized view
                    self.create_materialized_view(view_name, table, bucket_size)

                    # Add continuous aggregate policy
                    self.add_aggregate_policy(view_name, start_offset)

            print("✅ All continuous aggregates processed.")

        finally:
            self._disconnect()


# For external usage - single function call
def create_ds_chart_view():
    """External function to create all DS chart views"""
    manager = TradingSystemAggregatesManager()
    manager.create_ds_chart_view()