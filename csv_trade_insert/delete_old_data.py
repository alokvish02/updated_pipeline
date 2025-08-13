import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

TRADING_SYSTEM_CONN_PARAMS = {
    "dbname": "trading_system",
    "user": "postgres",
    "password": "onealpha12345",
    "host": "localhost",
    "port": 5432
}

def run_custom_query(query: str):
    try:
        with psycopg2.connect(**TRADING_SYSTEM_CONN_PARAMS) as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                cur.execute(query)
                # print("Query executed successfully.")
    except Exception as e:
        print(f"Error executing query: {e}")

def truncate_ds_chart():
    query = "TRUNCATE TABLE public.nse_utils_trade_history_1h;"
    run_custom_query(query)

# ────────────────────────────────────────────────────────────
# NEW: clear / “truncate” *all* period materialized views
# ────────────────────────────────────────────────────────────
MATERIALIZED_PERIOD_VIEWS = [
    "public.nse_utils_trade_history_and_trade_period_1w",
    "public.nse_utils_trade_history_and_trade_period_1m",
    "public.nse_utils_trade_history_and_trade_period_3m",
    "public.nse_utils_trade_history_and_trade_period_6m",
    "public.nse_utils_trade_history_and_trade_period_1y",
    "public.nse_utils_trade_history_and_trade_period_all",
]

def truncate_tile_views():
    """
    Empties every materialized view listed in MATERIALIZED_PERIOD_VIEWS
    by running REFRESH MATERIALIZED VIEW … WITH NO DATA;
    """
    for view in MATERIALIZED_PERIOD_VIEWS:
        query = f"REFRESH MATERIALIZED VIEW {view} WITH NO DATA;"
        run_custom_query(query)

# ────────────────────────────────────────────────────────────
# 🧪  USAGE EXAMPLES
# ────────────────────────────────────────────────────────────
# if __name__ == "__main__":
#     # 1️⃣  Empty (“truncate”) every period view
#     truncate_tile_views()
#     truncate_ds_chart()

    # 2️⃣  Or run something custom
    # run_custom_query("SELECT COUNT(*) FROM public.nse_utils_trade_history_and_trade_period_1w;")