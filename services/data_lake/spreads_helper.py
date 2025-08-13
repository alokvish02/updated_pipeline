import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from services.config import config
import warnings
warnings.filterwarnings('ignore')

def vectorized_ols(y, x, window):
    print("Vectorized OLS started")
    n = len(y)
    hedge_ratio = np.full(n, np.nan)
    
    y_vals = y.values
    x_vals = x.values
    
    for i in range(window-1, n):
        y_window = y_vals[i-window+1:i+1]
        x_window = x_vals[i-window+1:i+1]
        denominator = np.sum(x_window ** 2)
        if denominator != 0:
            hedge_ratio[i] = np.sum(x_window * y_window) / denominator
    
    return hedge_ratio

class SpreadCalculator:
    def __init__(self, exchange: str):
        self.exchange = exchange.lower()
        self.config = config
        
        # Extract parameters for easy access
        self.signal_params = self.config['params']
        self.window = self.signal_params['window']

    def generate_pair_name(self, sym1: str, sym2: str) -> str:
        clean = lambda s: s.replace(':', '').lower()
        return f"{clean(sym1)}_{clean(sym2)}"

    def _calculate_ols_spread(self, merged_df: pd.DataFrame, window: int) -> pd.DataFrame:
        lookback = self.window
        min_periods = max(1, window // 2) #need to add min_periods from congfig if needed
        
        y, x = merged_df["close_1"], merged_df["close_2"]
        
        if len(y) < min_periods:
            return pd.DataFrame()
        
        hedge = vectorized_ols(y, x, lookback)
        hedge_series = pd.Series(hedge, index=merged_df.index)
        
        try:
            spread_data = pd.DataFrame({
                "timestamp": merged_df["timestamp"],
                "symbol": merged_df["symbol_x"] + "_" + merged_df["symbol_y"],
                "open": merged_df["open_1"] - (hedge_series * merged_df["open_2"]),
                "high": merged_df["high_1"] - (hedge_series * merged_df["high_2"]),
                "low": merged_df["low_1"] - (hedge_series * merged_df["low_2"]),
                "close": merged_df["close_1"] - (hedge_series * merged_df["close_2"]),
                "volume": 0,
                "slope": hedge_series
            })
            return spread_data.dropna()
        except Exception as e:
            print("[ERROR in spread_data creation]", e)
            return pd.DataFrame()

    def _merge_dataframes(self, df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        df1_renamed = df1.rename(columns={
            'open': 'open_1', 'high': 'high_1', 'low': 'low_1', 
            'close': 'close_1', 'volume': 'volume_1'
        })
        df2_renamed = df2.rename(columns={
            'open': 'open_2', 'high': 'high_2', 'low': 'low_2',
            'close': 'close_2', 'volume': 'volume_2'
        })
        
        merged = pd.merge_asof(
            df1_renamed.sort_values('timestamp'),
            df2_renamed.sort_values('timestamp'),
            on='timestamp', direction='backward',
            tolerance=pd.Timedelta(minutes=5),
            suffixes=('_x', '_y')
        )
        return merged.ffill().dropna()

    def calculate_historical_spread(self, df1: pd.DataFrame, df2: pd.DataFrame, window: int) -> pd.DataFrame:
        if df1.empty or df2.empty:
            return pd.DataFrame()

        merged = self._merge_dataframes(df1, df2)
        if merged.empty:
            return pd.DataFrame()

        return self._calculate_ols_spread(merged, window)