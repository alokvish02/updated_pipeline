import os
import json
from binance.client import Client
from binance.enums import *
from dotenv import load_dotenv

# Load API Keys
load_dotenv()
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")

# Binance client
client = Client(API_KEY, API_SECRET)
client.FUTURES_URL = 'https://testnet.binancefuture.com/fapi'

class WebSocketTrader:
    def __init__(self):
        self.client = client
        self.active_trades = {}
    
    def get_live_price(self, symbol):
        try:
            ticker = self.client.futures_symbol_ticker(symbol=symbol)
            return float(ticker['price'])
        except:
            return None
    
    def calculate_quantity(self, symbol, fund_amount):
        price = self.get_live_price(symbol)
        if not price:
            return 0
        return round(fund_amount / price, 3)
    
    def check_active_position(self, symbol):
        try:
            positions = self.client.futures_position_information(symbol=symbol)
            for pos in positions:
                if float(pos['positionAmt']) != 0:
                    return True
            return False
        except:
            return False
    
    def execute_trade(self, data):
        try:
            sym1, sym2, signal, fund_per_trade, symbol_pair = data['sym1'], data['sym2'], data['signal'], data['fund_per_trade'], data['symbol_pair']
            
            price1 = self.get_live_price(sym1)
            price2 = self.get_live_price(sym2)
            
            if not price1 or not price2:
                return {'status': 'error', 'message': f'Failed to get prices for {sym1} or {sym2}'}
            
            qty1 = self.calculate_quantity(sym1, fund_per_trade)
            qty2 = self.calculate_quantity(sym2, fund_per_trade)
            
            if qty1 <= 0 or qty2 <= 0:
                return {'status': 'error', 'message': f'Invalid quantities: {qty1}, {qty2}'}
            
            if signal == 1:
                side1, side2 = SIDE_SELL, SIDE_BUY
            else:
                side1, side2 = SIDE_BUY, SIDE_SELL
            
            order1 = self.client.futures_create_order(
                symbol=sym1, side=side1, type=ORDER_TYPE_MARKET, quantity=qty1
            )
            
            order2 = self.client.futures_create_order(
                symbol=sym2, side=side2, type=ORDER_TYPE_MARKET, quantity=qty2
            )
            
            print(f"Order 1 Response: {order1}")
            print(f"Order 2 Response: {order2}")
            
            self.active_trades[symbol_pair] = {
                'sym1': sym1, 'sym2': sym2, 'qty1': qty1, 'qty2': qty2,
                'side1': side1, 'side2': side2, 'signal': signal
            }
            
            return {'status': 'success', 'message': f'Trade executed: {sym1} {side1} {qty1}, {sym2} {side2} {qty2}'}
            
        except Exception as e:
            print(f"EXECUTION ERROR: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def close_trade(self, data):
        try:
            symbol_pair = data['symbol_pair']
            
            # Extract symbols from symbol_pair (e.g., "BTCUSDT_ETHUSDT" -> "BTCUSDT", "ETHUSDT")
            symbols = symbol_pair.split('_')
            if len(symbols) != 2:
                return {'status': 'error', 'message': f'Invalid symbol pair format: {symbol_pair}'}
            
            sym1, sym2 = symbols[0], symbols[1]
            
            # Check positions directly from Binance
            pos1_exists = self.check_active_position(sym1)
            pos2_exists = self.check_active_position(sym2)
            
            if not pos1_exists and not pos2_exists:
                return {'status': 'success', 'message': f'No active positions found for {symbol_pair}'}
            
            # Close positions that exist
            if pos1_exists:
                # Get position details to determine close side and quantity
                positions = self.client.futures_position_information(symbol=sym1)
                for pos in positions:
                    if float(pos['positionAmt']) != 0:
                        position_amt = float(pos['positionAmt'])
                        close_side = SIDE_SELL if position_amt > 0 else SIDE_BUY
                        close_qty = abs(position_amt)
                        
                        close_order1 = self.client.futures_create_order(
                            symbol=sym1, side=close_side, 
                            type=ORDER_TYPE_MARKET, quantity=close_qty
                        )
                        print(f"Close Order 1 Response: {close_order1}")
                        break
            
            if pos2_exists:
                # Get position details to determine close side and quantity
                positions = self.client.futures_position_information(symbol=sym2)
                for pos in positions:
                    if float(pos['positionAmt']) != 0:
                        position_amt = float(pos['positionAmt'])
                        close_side = SIDE_SELL if position_amt > 0 else SIDE_BUY
                        close_qty = abs(position_amt)
                        
                        close_order2 = self.client.futures_create_order(
                            symbol=sym2, side=close_side, 
                            type=ORDER_TYPE_MARKET, quantity=close_qty
                        )
                        print(f"Close Order 2 Response: {close_order2}")
                        break
            
            # Clean up local storage if exists
            if symbol_pair in self.active_trades:
                del self.active_trades[symbol_pair]
            
            return {'status': 'success', 'message': f'Trade closed: {symbol_pair}'}
            
        except Exception as e:
            print(f"CLOSE ERROR: {e}")
            return {'status': 'error', 'message': str(e)}

trader = WebSocketTrader()

def execute_live_order(sym1, sym2, signal, fund_per_trade, symbol_pair):
    data = {
        'sym1': sym1,
        'sym2': sym2, 
        'signal': signal,
        'fund_per_trade': fund_per_trade,
        'symbol_pair': symbol_pair
    }
    
    result = trader.execute_trade(data)
    return result['status'] == 'success'

def close_live_order(symbol_pair):
    data = {'symbol_pair': symbol_pair}
    result = trader.close_trade(data)
    return result['status'] == 'success'

# execute_live_order("BTCUSDT", "ETHUSDT", 1, 100, "BTCUSDT_ETHUSDT")
# close_live_order("BTCUSDT_ETHUSDT")