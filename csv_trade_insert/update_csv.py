import pandas as pd

def process_test_csv():
    """
    Read test.csv and create active.csv and history.csv based on status with specific format
    """
    try:
        df = pd.read_csv('test.csv')
        print("test.csv loaded successfully!")
        print(f"Total records: {len(df)}")

        active_columns = [
            'util_type', 'symbol', 'id', 'candle_time', 'action', 'price',
            'stop_loss', 'target_price', 'current_price', 'exit_price',
            'pnl', 'status', 'executed_at', 'exchange_mode'
        ]

        history_columns = [
            'util_type', 'symbol', 'id', 'candle_time', 'action', 'price',
            'stop_loss', 'target_price', 'current_price', 'exit_price',
            'pnl', 'status', 'executed_at', 'exchange_mode'
        ]

        active_data = df[df['status'] == 'active']
        closed_data = df[df['status'] == 'closed']

        print(f"\nActive records: {len(active_data)}")
        print(f"Closed records: {len(closed_data)}")

        # Create active.csv with specific format
        if len(active_data) > 0:
            # print("active_data.shape", active_data.shape)
            # print("active_data\n", active_data.to_string(index=False))

            def get_action_from_position(position):
                position = position.lower()
                if 'long' in position:
                    return 'BUY'
                elif 'short' in position:
                    return 'SELL'
                else:
                    return 'BUY'

            # Initialize DataFrame with correct index
            active_formatted = pd.DataFrame(index=range(len(active_data)))
            active_formatted['util_type'] = 'ACTIVE'
            active_formatted['symbol'] = active_data['Pair'].values
            active_formatted['id'] = range(1, len(active_data) + 1)
            active_formatted['candle_time'] = active_data['Entry Date'].values
            active_formatted['action'] = active_data['Position'].apply(get_action_from_position).values
            active_formatted['price'] = active_data['entry_spread'].values
            active_formatted['stop_loss'] = active_data['stoploss'].values
            active_formatted['target_price'] = active_data['target'].values
            active_formatted['current_price'] = active_data['exit_spread'].values
            active_formatted['exit_price'] = ''
            active_formatted['pnl'] = active_data['P&L'].values
            active_formatted['status'] = 'active'
            active_formatted['executed_at'] = active_data['Entry Date'].values
            active_formatted['exchange_mode'] = 'nse'

            # Debug: Print DataFrame before saving
            print("active_formatted before saving:\n", active_formatted.head().to_string(index=False))
            active_formatted.to_csv('active.csv', index=False, encoding='utf-8')
            print("active.csv created successfully!")
        else:
            print("No active records found, active.csv not created")

        # Create trade_history.csv with specific format
        if len(closed_data) > 0:
            def determine_status(row):
                position = row['Position'].lower()
                entry_spread = row['entry_spread']
                exit_spread = row['exit_spread']
                stoploss = row['stoploss']
                target = row['target']

                if position == 'long':
                    if exit_spread >= target:
                        return 'target_hit'
                    elif exit_spread <= stoploss:
                        return 'stop_loss_hit'
                    else:
                        return 'target_hit' if exit_spread > entry_spread else 'stop_loss_hit'
                elif position == 'short':
                    if exit_spread <= target:
                        return 'target_hit'
                    elif exit_spread >= stoploss:
                        return 'stop_loss_hit'
                    else:
                        return 'target_hit' if exit_spread < entry_spread else 'stop_loss_hit'
                return 'target_hit'

            def get_action_from_position(position):
                position = position.lower()
                if 'long' in position:
                    return 'BUY'
                elif 'short' in position:
                    return 'SELL'
                else:
                    return 'BUY'

            # Initialize DataFrame with correct index
            history_formatted = pd.DataFrame(index=range(len(closed_data)))
            history_formatted['util_type'] = 'TRADE_HISTORY'
            history_formatted['symbol'] = closed_data['Pair'].values
            history_formatted['id'] = range(1, len(closed_data) + 1)
            history_formatted['candle_time'] = closed_data['Entry Date'].values
            history_formatted['action'] = closed_data['Position'].apply(get_action_from_position).values
            history_formatted['price'] = closed_data['entry_spread'].values
            history_formatted['stop_loss'] = closed_data['stoploss'].values
            history_formatted['target_price'] = closed_data['target'].values
            history_formatted['current_price'] = ''
            history_formatted['exit_price'] = closed_data['exit_spread'].values
            history_formatted['pnl'] = closed_data['P&L'].values
            history_formatted['status'] = closed_data.apply(determine_status, axis=1).values
            history_formatted['executed_at'] = closed_data['Exit Date'].values
            history_formatted['exchange_mode'] = 'nse'

            # Debug: Print DataFrame before saving
            print("history_formatted before saving:\n", history_formatted.head().to_string(index=False))
            history_formatted.to_csv('trade_history.csv', index=False, encoding='utf-8')
            print("trade_history.csv created successfully!")
        else:
            print("No closed records found, trade_history.csv not created")

        return active_formatted if len(active_data) > 0 else None, history_formatted if len(closed_data) > 0 else None

    except FileNotFoundError:
        print("Error: test.csv file not found!")
        return None, None
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return None, None

def update_csv():
    active_df, history_df = process_test_csv()

    if active_df is not None and len(active_df) > 0:
        print("\nSample active data:")
        print(active_df.head().to_string(index=False))

    if history_df is not None and len(history_df) > 0:
        print("\nSample history data:")
        print(history_df.head().to_string(index=False))

# if __name__ == "__main__":
#     update_csv()


# active_formatted = pd.DataFrame(index=range(len(active_data)))
# history_formatted = pd.DataFrame(index=range(len(closed_data)))