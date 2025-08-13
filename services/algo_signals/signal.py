import pandas as pd
import json
from datetime import datetime
from services.config import redis_connection, config
from services.db_config import get_db_connection
from services.algo_signals.Strategy import TradingStrategyEngine
from services.algo_signals.monitor import start_monitor

class SignalProcessor:
    def __init__(self, exchange):
        self.exchange = exchange
        self.redis_client = redis_connection()
        self.strategy_engine = TradingStrategyEngine()
        self.fpt = []   
        redis_key = f"account_matrix:account"
        account_matrix = self.redis_client.hgetall(redis_key)
        self.account_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in account_matrix.items()}
    
    def fetch_spread_data(self, symbol_pairs, exchange):
        try:
            symbol_pairs = [symbol_pairs] if isinstance(symbol_pairs, str) else symbol_pairs
            lookback = int(self.account_data.get("lookback")) + 10
            table = f"public.{exchange}_spreads"
            placeholders = ','.join(['%s'] * len(symbol_pairs))
            
            query = f"""
                WITH ranked AS (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) rn
                    FROM {table} WHERE symbol IN ({placeholders})
                )
                SELECT symbol, timestamp, open, high, low, close, volume, slope
                FROM ranked WHERE rn <= %s ORDER BY symbol, timestamp DESC
            """

            with get_db_connection() as conn, conn.cursor() as cur:
                cur.execute(query, symbol_pairs + [lookback])
                rows = cur.fetchall()
                if not rows:
                    return pd.DataFrame()
                cols = [desc[0] for desc in cur.description]

            df = pd.DataFrame(rows, columns=cols)
            numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'slope']
            
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            df = df.sort_values('timestamp', ascending=True).reset_index(drop=True)
            return df
        except Exception as e:
            print(f"fetch_spread_data error: {e}")
            return pd.DataFrame()

    def get_stock_ltp(self, symbols, exchange):
        try:
            redis_key = f"{exchange}_ltp:stocks"
            pipe = self.redis_client.pipeline()
            for symbol in symbols:
                pipe.hget(redis_key, symbol.lower())
            results = pipe.execute()
            return {symbol.lower(): float(json.loads(res)['price']) if res else None for symbol, res in zip(symbols, results)}
        except Exception as e:
            print(f"get_stock_ltp error: {e}")
            return {}

    def get_spread_live_data(self, symbol_pair, exchange):
        try:
            result = self.redis_client.hget(config['redis_keys']['spreads_live_data'], symbol_pair)
            if result:
                data = json.loads(result)
                return {'close': float(data['close']), 'slope': float(data['slope']), 'timestamp': data['timestamp']}
            return None
        except Exception as e:
            print(f"get_spread_live_data error: {e}")
            return None

    def check_trade_exists(self, symbol, signal, exchange):
        try:
            action = "BUY" if signal == 1 else "SELL"
            cache_key = f"trade_check:{exchange}:{symbol}:{action}"
            if self.redis_client.exists(cache_key):
                return True
            
            with get_db_connection() as conn, conn.cursor() as cursor:
                cursor.execute(f"SELECT 1 FROM public.{exchange}_utils_trade WHERE symbol = %s AND action = %s LIMIT 1", (symbol, action))
                exists = cursor.fetchone() is not None
            
            if exists:
                self.redis_client.setex(cache_key, config['cache_params']['trade_check_ttl'], "1")
            return exists
        except Exception as e:
            print(f"check_trade_exists error: {e}")
            return False

    def calculate_trade_data(self, symbol_pair, latest_row, signal, spread_data, stock_ltps, exchange):
        try:
            sym1, sym2 = symbol_pair.split("_")
            total_capital = float(self.account_data.get("total_capital"))
            pos_val = float(self.account_data.get("pos_val"))
            target_distance = float(latest_row.get('mean'))
            long_band = float(latest_row.get('long_band'))
            short_band = float(latest_row.get('short_band'))
            fund_per_trade = total_capital / pos_val
            self.fpt.append(fund_per_trade)
            spread_price = spread_data['close']
            
            quantity_sym1 = round(fund_per_trade / stock_ltps[sym1.lower()])
            quantity_sym2 = round(fund_per_trade / stock_ltps[sym2.lower()])

            if signal == 1:
                sym1_quantity = -quantity_sym1
                sym2_quantity = quantity_sym2
                
                # print("this is the tp", target_distance)
                spread_target = target_distance
                spread_stop_loss = spread_price - (target_distance - spread_price)
            else: 
                sym1_quantity = quantity_sym1
                sym2_quantity = -quantity_sym2
                spread_target = target_distance
                spread_stop_loss = spread_price + (target_distance - spread_price)

            entry_price_sym1 = stock_ltps[sym1.lower()]
            entry_price_sym2 = stock_ltps[sym2.lower()]
            pnl_sym1 = (stock_ltps[sym1.lower()] - entry_price_sym1) * sym1_quantity
            pnl_sym2 = (stock_ltps[sym2.lower()] - entry_price_sym2) * sym2_quantity

            total_pnl = pnl_sym1 + pnl_sym2

            return {
                "symbol_pair": symbol_pair,
                "candle_time": str(latest_row.get('timestamp', datetime.now())),
                "executed_at": str(datetime.now()),
                "signal": signal,
                "action": "BUY" if signal == 1 else "SELL",
                "entry_price": spread_price,
                "stop_loss": spread_stop_loss,
                "target": spread_target,
                "quantity": 1,
                "sym1_quantity": sym1_quantity,
                "sym2_quantity": sym2_quantity,
                "pnl": total_pnl,
                "sym1_entry_price": stock_ltps[sym1.lower()],
                "sym2_entry_price": stock_ltps[sym2.lower()],
                "sym1": sym1,
                "sym2": sym2
            }
        except Exception as e:
            print(f"calculate_trade_data error: {e}")
            return None

    def insert_trade_to_db(self, trade_data, exchange):
        try:
            table = f"public.{exchange}_utils_trade"
            query = f"""
                INSERT INTO {table} (util_type, symbol, candle_time, action, price, stop_loss, target_price, current_price, pnl, status, executed_at, exchange_mode)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            with get_db_connection() as conn, conn.cursor() as cursor:
                cursor.execute(query, (
                    'spread',
                    trade_data['symbol_pair'],
                    trade_data['candle_time'],
                    trade_data['action'],
                    trade_data['entry_price'],
                    trade_data['stop_loss'],
                    trade_data['target'],
                    trade_data['entry_price'],
                    trade_data['pnl'],
                    'active',
                    trade_data['executed_at'],
                    exchange
                ))
                conn.commit()
        except Exception as e:
            print(f"insert_trade_to_db error: {e}")

    def store_trade(self, trade_data, exchange):
        print(f"Storing trade data for {trade_data['symbol_pair']} on {exchange}")
        try:
            from services.live_order.cptord_live import execute_live_order
            spread_key = f"{exchange}_spread_trade:{trade_data['symbol_pair']}"
            self.redis_client.set(spread_key, json.dumps(trade_data))
            ftp = self.fpt[-1] if self.fpt else 0
            # if exchange == 'binance':
            #     execute_live_order(trade_data['sym1'], trade_data['sym2'], trade_data['signal'], ftp, trade_data['symbol_pair'])
            start_monitor(exchange)
            self.insert_trade_to_db(trade_data, exchange)
        except Exception as e:
            print(f"store_trade error: {e}")

    def process_symbol(self, symbol_pair, exchange):
        try:
            spread_df = self.fetch_spread_data(symbol_pair, exchange)
            if spread_df.empty:
                print(f"No spread data found for {symbol_pair} on {exchange}")
                return None
            
            df_with_signals = self.strategy_engine.generate_signals(spread_df, exchange)
            # df_with_signals.to_csv(f'signal_data.csv', index=False)
            if df_with_signals.empty or 'signal' not in df_with_signals.columns:
                print(f"No signals generated for {symbol_pair} on {exchange}")
                return None
            
            latest = df_with_signals.iloc[-1]
            print("Latest row:\n", latest)
            signal = int(latest['signal'])
            start_monitor(exchange)
            if signal == 0 or self.check_trade_exists(symbol_pair, signal, exchange):
                print(f"No valid signal or trade already exists for {symbol_pair} on {exchange}.")
                return None
            
            spread_live_data = self.get_spread_live_data(symbol_pair, exchange)
            if not spread_live_data:
                print(f"No live data available for {symbol_pair} on {exchange}.")
                return None
            
            sym1, sym2 = symbol_pair.split("_")
            stock_ltps = self.get_stock_ltp([sym1, sym2], exchange)
            if not all(stock_ltps.values()):
                print(f"Missing LTP data for {symbol_pair} on {exchange}.")
                return None
            
            trade_data = self.calculate_trade_data(symbol_pair, latest, signal, spread_live_data, stock_ltps, exchange)
            if not trade_data:
                print(f"Failed to calculate trade data for {symbol_pair} on {exchange}")
                return None
                
            self.store_trade(trade_data, exchange)
            return trade_data
        except Exception as e:
            print(f"process_symbol error for {symbol_pair} on {exchange}: {e}")
            return None


def process_symbol_signal(symbol_pair, exchange):
    try:
        processor = SignalProcessor(exchange)
        return processor.process_symbol(symbol_pair, exchange)
    except Exception as e:
        print(f"process_symbol_signal error for {symbol_pair} on {exchange}: {e}")
        return None