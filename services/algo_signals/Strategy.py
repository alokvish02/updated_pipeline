import pandas as pd
import numpy as np
from numba import jit
from services.config import redis_connection, config

@jit(nopython=True)
def bollinger_signals_fast(close_prices, std_vals, num_std):
    n = len(close_prices)
    signals = np.zeros(n)
    # signals = np.ones(n)
    long_bands = std_vals - (num_std * std_vals)
    short_bands = std_vals + (num_std * std_vals)
    
    for i in range(n):
        if close_prices[i] < long_bands[i]: 
            signals[i] = 1
        elif close_prices[i] > short_bands[i]: 
            signals[i] = -1
    
    return signals, long_bands, short_bands, std_vals

class TradingStrategyEngine:
    def __init__(self):
        self.redis_client = redis_connection()
        self.strategy_params = config['params']
    
    def generate_signals(self, df, exchange):
        try:
            if df.empty or 'close' not in df.columns:
                print(f"generate_signals error: Empty dataframe or missing 'close' column")
                return df
                
            strategy_name = self.strategy_params['strategy'].lower()
            result_df = df.copy()
            bands_data = {}
            
            if strategy_name == 'bollinger':
                std = df['close'].rolling(self.strategy_params['window']).std()
                signals, long_bands, short_bands, mean_vals = bollinger_signals_fast(df['close'].values, std.values, self.strategy_params['std'])
                bands_data = {'long_band': long_bands, 'short_band': short_bands, 'mean': mean_vals}
                result_df['signal'] = signals
            else:
                print(f"generate_signals error: Unknown strategy '{strategy_name}'")
                return df
            
            bands_df = pd.DataFrame(bands_data, index=df.index)
            final_df = pd.concat([result_df, bands_df], axis=1)
            # print(f"Final DataFrame shape: {final_df.shape}, columns: {list(final_df.columns)}")
            
            return final_df
        except Exception as e:
            print(f"generate_signals error: {e}")
            return df