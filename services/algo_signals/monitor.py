import json
import time
import threading
from datetime import datetime
from services.config import redis_connection, config
from services.live_order.cptord_live import execute_live_order, close_live_order
from services.db_config import get_db_connection

class TradeMonitor:
    def __init__(self, exchange):
        self.exchange = exchange
        self.redis = redis_connection()
        self.running = False
        self.trades = {}
        self.last_ltps = {}
        self.ltp_subscriber = None
    
    def get_ltps(self):
        if not self.trades:
            return {}
        symbols = set()
        for t in self.trades.values():
            symbols.update([t['sym1'], t['sym2']])
        
        pipe = self.redis.pipeline()
        for s in symbols:
            pipe.hget(f"{self.exchange}_ltp:stocks", s.lower())
        results = pipe.execute()
        
        ltps = {}
        for s, r in zip(symbols, results):
            if r:
                data = json.loads(r)
                ltps[s.lower()] = float(data['price'])
        return ltps
    
    def calc_pnl(self, trade, ltps):
        p1, p2 = ltps.get(trade['sym1'].lower()), ltps.get(trade['sym2'].lower())
        if not p1 or not p2:
            return None
        e1, e2 = trade['sym1_entry_price'], trade['sym2_entry_price']
        q1, q2 = trade['sym1_quantity'], trade['sym2_quantity']
        pnl = (p1 - e1) * q1 + (p2 - e2) * q2
        return {'pnl': pnl, 'sym1_price': p1, 'sym2_price': p2}

    
    def check_exit(self, trade):
        spread_data = self.redis.hget(config['redis_keys']['spreads_live_data'], trade['symbol_pair'])
        if not spread_data:
            return False, None
        spread_price = float(json.loads(spread_data)['close'])
        
        if trade['signal'] == 1:
            if spread_price <= trade['stop_loss']:
                return True, "SL"
            elif spread_price >= trade['target']:
                return True, "TP"
        else:
            if spread_price >= trade['stop_loss']:
                return True, "SL"
            elif spread_price <= trade['target']:
                return True, "TP"
        return False, None
    
    def insert_trade_history(self, trade_data):
        """Insert trade data into PostgreSQL history table"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            insert_query = """
                INSERT INTO public.{}_utils_trade_history 
                (util_type, symbol, candle_time, action, price, stop_loss, target_price, 
                 current_price, exit_price, pnl, status, executed_at, exchange_mode)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """.format(self.exchange.lower())
            
            cursor.execute(insert_query, (
                trade_data.get('util_type', 'TRADE_HISTORY'),
                trade_data.get('symbol_pair', ''),
                trade_data.get('candle_time', datetime.now()),
                trade_data.get('action', 'BUY'),
                trade_data.get('entry_price', 0),
                trade_data.get('stop_loss', 0),
                trade_data.get('target', 0),
                trade_data.get('current_price', 0),
                trade_data.get('exit_price', 0),
                trade_data.get('final_pnl', 0),
                trade_data.get('exit_reason', ''),
                trade_data.get('closed_at', datetime.now()),
                self.exchange
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
        except Exception as e:
            print(f"Database insert error: {e}")
    
    def delete_active_trade(self, symbol, action):
        """Delete trade from active trades table"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            delete_query = """
                DELETE FROM public.{}_utils_trade 
                WHERE symbol = %s AND action = %s
            """.format(self.exchange.lower())
            
            cursor.execute(delete_query, (symbol, action))
            conn.commit()
            cursor.close()
            conn.close()
            
        except Exception as e:
            print(f"Database delete error: {e}")
    
    def close_trade(self, pair, trade, reason, pnl_data):
        close_data = {
            **trade, 
            'closed_at': str(datetime.now()), 
            'exit_reason': reason, 
            'final_pnl': pnl_data['pnl'],
            'current_price': pnl_data.get('sym1_price', 0),
            'exit_price': pnl_data.get('sym2_price', 0)
        }
        
        # Insert into history table
        self.insert_trade_history(close_data)
        
        # Delete from active trades table
        self.delete_active_trade(trade.get('symbol_pair', ''), trade.get('action', 'BUY'))
        
        close_live_order(pair)
        pipe = self.redis.pipeline()
        pipe.set(f"{self.exchange}_trade_history:{pair}:{int(time.time())}", json.dumps(close_data))
        pipe.delete(f"{self.exchange}_spread_trade:{pair}")
        pipe.execute()
        
        del self.trades[pair]
        print(f"Closed {pair}: {reason} PnL:{pnl_data['pnl']:.2f}")
    
    def load_trades(self):
        keys = self.redis.keys(f"{self.exchange}_spread_trade:*")
        if not keys:
            return
        
        pipe = self.redis.pipeline()
        for k in keys:
            pipe.get(k)
        results = pipe.execute()
        
        for k, r in zip(keys, results):
            if r:
                pair = k.decode('utf-8').split(':')[1]
                self.trades[pair] = json.loads(r)
        print(f"Loaded {len(self.trades)} trades for monitoring")
    
 
    def update_pnl(self, ltps):
        """Update PnL in Redis"""
        for pair, trade in self.trades.items():
            pnl_data = self.calc_pnl(trade, ltps)
            if pnl_data:
                try:
                    trade_key = f"{self.exchange}_spread_trade:{pair}"
                    trade_data = self.redis.get(trade_key)
                    if not trade_data:
                        continue
                    trade_data = json.loads(trade_data)
                    trade_data['pnl'] = round(pnl_data['pnl'], 2)
                    self.redis.set(trade_key, json.dumps(trade_data))
                    self.trades[pair]['pnl'] = trade_data['pnl']
                    
                except Exception as e:
                    print(f"Redis update error for {pair}: {e}")
    
    def monitor(self):
        while self.running:
            try:
                if not self.trades:
                    self.load_trades()

                if not self.trades:
                    print(f"No trades found, stopping monitor: {self.exchange}")
                    self.running = False
                    if self.exchange in monitors:
                        del monitors[self.exchange]
                    break
                
                ltps = self.get_ltps()
                if ltps != self.last_ltps and ltps:
                    self.last_ltps = ltps.copy()
                    self.update_pnl(ltps)
                    to_close = []
                    for pair, trade in self.trades.items():
                        pnl_data = self.calc_pnl(trade, ltps)
                        if pnl_data:
                            should_close, reason = self.check_exit(trade)
                            if should_close:
                                to_close.append((pair, trade, reason, pnl_data))
                    for pair, trade, reason, pnl_data in to_close:
                        self.close_trade(pair, trade, reason, pnl_data)
                
                time.sleep(1)
                    
            except Exception as e:
                print(f"Monitor error: {e}")
                time.sleep(1)

monitors = {}

def start_monitor(exchange):
    if exchange in monitors:
        return
    
    monitor = TradeMonitor(exchange)
    monitor.running = True
    monitor.load_trades()
    
    thread = threading.Thread(target=monitor.monitor, daemon=True)
    thread.start()
    
    monitors[exchange] = monitor
    print(f"Monitor started: {exchange}")

def stop_monitor(exchange):
    if exchange in monitors:
        monitors[exchange].running = False
        print(f"Monitor stoped: {exchange}")
        del monitors[exchange]