import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Load data
df = pd.read_csv('signal_data.csv')
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Create subplot with secondary y-axis for volume
fig = make_subplots(rows=2, cols=1, 
                   shared_xaxes=True,
                   vertical_spacing=0.1,
                   row_heights=[0.7, 0.3],
                   subplot_titles=('BTCUSDT/ETHUSDT Spread with Bollinger Bands', 'Volume'))

# Candlestick chart
fig.add_trace(go.Candlestick(x=df['timestamp'],
                           open=df['open'],
                           high=df['high'], 
                           low=df['low'],
                           close=df['close'],
                           name='Spread',
                           increasing_line_color='green',
                           decreasing_line_color='red'),
             row=1, col=1)

# Bollinger Bands
fig.add_trace(go.Scatter(x=df['timestamp'], y=df['short_band'],
                        mode='lines', name='Upper Band',
                        line=dict(color='blue', width=1)), row=1, col=1)

fig.add_trace(go.Scatter(x=df['timestamp'], y=df['long_band'],
                        mode='lines', name='Lower Band',
                        line=dict(color='blue', width=1)), row=1, col=1)

fig.add_trace(go.Scatter(x=df['timestamp'], y=df['mean'],
                        mode='lines', name='Mean',
                        line=dict(color='orange', width=2)), row=1, col=1)

# Signal markers
buy_signals = df[df['signal'] == 1]
sell_signals = df[df['signal'] == -1]

if not buy_signals.empty:
    fig.add_trace(go.Scatter(x=buy_signals['timestamp'], y=buy_signals['close'],
                            mode='markers', name='Buy Signal',
                            marker=dict(symbol='triangle-up', size=15, color='green')), row=1, col=1)

if not sell_signals.empty:
    fig.add_trace(go.Scatter(x=sell_signals['timestamp'], y=sell_signals['close'],
                            mode='markers', name='Sell Signal', 
                            marker=dict(symbol='triangle-down', size=15, color='red')), row=1, col=1)

# Volume bars
fig.add_trace(go.Bar(x=df['timestamp'], y=df['volume'],
                    name='Volume', marker_color='lightblue'), row=2, col=1)

# Update layout
fig.update_layout(
    title='BTCUSDT/ETHUSDT Spread Trading with Bollinger Bands',
    xaxis_title='Time',
    yaxis_title='Spread Price',
    height=800,
    showlegend=True,
    xaxis_rangeslider_visible=False
)

fig.update_yaxes(title_text="Volume", row=2, col=1)

# Show chart
fig.show()

# Print signal statistics
total_signals = len(df[df['signal'] != 0])
buy_count = len(buy_signals)
sell_count = len(sell_signals)

print(f"Total data points: {len(df)}")
print(f"Total signals: {total_signals}")
print(f"Buy signals: {buy_count}")
print(f"Sell signals: {sell_count}")
print(f"Signal frequency: {total_signals/len(df)*100:.2f}%")