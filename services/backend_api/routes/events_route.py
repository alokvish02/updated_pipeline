# backend_api/routes/trade_routes.py

from flask import Blueprint, request, jsonify
from services.backend_api.services.event_service import (
    TradeService,
    TradeHistory,
    Notifications,
    Get_SpreadData,
    Trade_and_tradehistoryfor_dealbook,
    Trade_and_historyfor_chart,
    TradeAndTradeHistoryMetrics,
    Trade_and_tradehistoryfor_sidebar
)
from services.backend_api.socketio_init import socketio

# Define the blueprint
trade_bp = Blueprint('backend_api', __name__)


@trade_bp.route('/trades', methods=['GET'])
def list_trades():
    """
    List all trades.
    """
    trades = TradeService.get_all_trades()
    # print("trades", trades)
    return jsonify(trades), 200


@trade_bp.route('/tradehistory', methods=['GET'])
def get_trades_history():
    """
    List all trades.
    """
    trades = TradeHistory.trades_history()
    # print("trades", trades)
    return jsonify(trades), 200


@trade_bp.route('/spreadsdata', methods=['GET'])
def get_spreads_data():
    """
    List all trades.
    """
    spreads = Get_SpreadData.SpreadData()
    # print("trades", trades)
    return jsonify(spreads), 200


@trade_bp.route('/trades/notify', methods=['GET'])
def notify_trades():
    # Assume you want to notify all clients about the current trades
    notify = Notifications.Notifications_data()
    # print(trades,trades)
    # Emit the trades event to all connected clients

    socketio.emit('notify_update', notify, broadcast=True)
    return jsonify({"message": "notify updates emitted"}), 200


# backend_api/routes/trade_routes.py
@trade_bp.route('/trades_and_history', methods=['GET'], endpoint='get_trades_and_history')
def get_trades_and_history():
    """
    Endpoint to get trade and trade history for the dealbook.
    """
    try:
        return Trade_and_tradehistoryfor_dealbook.get_trade_and_tradehistoryfor_dealbook()
    except Exception as e:
        import traceback
        traceback.print_exc()  # Log traceback for debugging
        return jsonify({"error": f"Failed to fetch trades and history: {str(e)}"}), 500


@trade_bp.route('/trade_and_history_for_chart', methods=['GET'], endpoint='get_trade_and_history_for_chart')
def get_trade_and_history_for_chart():
    """
    Endpoint to get trade and trade history for the chart.
    """
    try:
        return Trade_and_historyfor_chart.get_trade_and_historyfor_chart()
    except Exception as e:
        import traceback
        traceback.print_exc()  # Log traceback for debugging
        return jsonify({"error": f"Failed to fetch chart data: {str(e)}"}), 500


@trade_bp.route('/trade_and_history_metrics', methods=['GET'], endpoint='get_trade_and_history_metrics')
def get_trade_and_history_metrics():
    """
    Endpoint to get trade and trade history metrics.
    """
    try:
        return TradeAndTradeHistoryMetrics.get_trade_and_trade_historymatrics()
    except Exception as e:
        import traceback
        traceback.print_exc()  # Log traceback for debugging
        return jsonify({"error": f"Failed to fetch metrics: {str(e)}"}), 500


@trade_bp.route('/trades_and_historyforsidebar', methods=['GET'], endpoint='trades_and_historyforsidebar')
def trades_and_historyforsidebar():
    """
    Endpoint to get trade and trade history metrics.
    """
    try:
        return Trade_and_tradehistoryfor_sidebar.get_trade_and_tradehistoryfor_sidebar()
    except Exception as e:
        import traceback
        traceback.print_exc()  # Log traceback for debugging
        return jsonify({"error": f"Failed to fetch metrics: {str(e)}"}), 500