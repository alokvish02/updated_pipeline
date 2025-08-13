import eventlet
eventlet.monkey_patch()

import os
import sys
import json
import hashlib
import time
import psutil
import platform
from decimal import Decimal
from threading import Thread
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
from flask_socketio import SocketIO
from extensions import db
import redis

# Adjust sys.path to include Pipeline directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Try multiple import paths for Config
try:
    from services.config import Config
except ImportError:
    try:
        from services.backend_api.config import Config
    except ImportError:
        try:
            from config import Config
        except ImportError as e:
            raise ImportError("Could not find config.py in services/, services/backend_api/, or Pipeline/") from e

from services.config import redis_connection
from routes.events_route import trade_bp
from routes.admin_route import admin_bp
from chart import chart_bp, init_socketio_events
from services.backend_api.services.event_service import (
    TradeService, 
    TradeHistory, 
    Notifications, 
    Get_count_ttp
)

eventlet.monkey_patch()
push, redis_client = [], redis_connection()

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj): return float(obj) if isinstance(obj, Decimal) else super().default(obj)

def check_data_pull():
    try:
        data = redis_client.hget("ui_alert", "data_pull")
        if data and json.loads(data).get("data_pull"):
            push.append(True); return json.loads(data)
    except: pass
    return False

def get_live_system_metrics():
    try:
        mem, disk = psutil.virtual_memory(), psutil.disk_usage('/')
        load_avg = "0,0,0" if platform.system()=="Windows" else ",".join(f"{x:.2f}" for x in psutil.getloadavg())
        return {'cpu_percent': round(psutil.cpu_percent(0.1), 2),
                'memory_percent': round(mem.percent, 2),
                'memory_used': round(mem.used/(1024**3), 2),
                'memory_total': round(mem.total/(1024**3), 2),
                'disk_percent': round(disk.percent, 2),
                'load_avg': load_avg, 'platform': platform.system()}
    except: return {k:0 for k in ['cpu_percent','memory_percent','memory_used','disk_percent']}|{'memory_total':1,'load_avg':'0,0,0','platform':'unknown'}

def background_data_emitter(app, socketio):
    from datetime import datetime, timedelta
    previous_data, push[:] = {}, []
    previous_notifications = {}  # Track previous notifications per exchange

    @socketio.on('connect')
    def on_connect():
        # Get current exchange from global filter or default
        current_exchange = redis_client.hget("global_filter", "exchange") or "binance"
        if isinstance(current_exchange, bytes):
            current_exchange = current_exchange.decode('utf-8')
        
        initial_data = get_comprehensive_data(current_exchange)
        socketio.emit('data_update', initial_data, room=request.sid)
        print(f"Client connected, sent initial data for {current_exchange}")

    @socketio.on('disconnect')
    def on_disconnect(): 
        print("Client disconnected")

    @socketio.on('client_message')
    def on_client_msg(data):
        if data.get("message") == "DataPulled" and True in push:
            redis_client.hset("ui_alert","data_pull",json.dumps({"data_pull":False})); push.clear()

    def get_comprehensive_data(exchange_filter):
        try:
            r = redis.Redis(host=Config.REDIS_HOST, port=Config.REDIS_PORT, password=Config.REDIS_PASSWORD, db=0, decode_responses=True)
            
            # Get ONLY active trades for this specific exchange
            active_trades_only = TradeService.get_all_trades(exchange_filter)
            
            # Get notifications for this specific exchange
            notifications_data = Notifications.Notifications_data(exchange_filter)
            
            return {
                'trades': active_trades_only,  # Only active trades for the current exchange
                'notifications': notifications_data,
                'status': r.get('pipeline:status') or 'stopped',
                'exchange': exchange_filter,  # Send current exchange
                'thread_data': getattr(Get_count_ttp.get_thread_data(),'get_json',lambda:None)(),
                'hardware': get_live_system_metrics(),
                'pipeline': {'status': r.get('pipeline:status') or 'stopped','exchange': exchange_filter},
                'data_pull': check_data_pull()
            }
        except Exception as e:
            print(f"Error getting comprehensive data for {exchange_filter}: {e}")
            return {}

    def get_changed_values(old, new, exchange_filter):
        changed = {}
        
        # Always include trades for real-time updates (only active trades)
        if 'trades' in new:
            changed['trades'] = new['trades']
        
        # Check for new notifications
        if 'notifications' in new:
            old_notifications = old.get('notifications', {})
            new_notifications = new['notifications']
            
            # Check if latest notification changed
            old_latest = old_notifications.get('latest_notification', '')
            new_latest = new_notifications.get('latest_notification', '')
            
            if new_latest and (not old_latest or old_latest != new_latest):
                changed['notifications'] = new_notifications
                print(f"New notification detected for {exchange_filter}")
        
        # Check other fields
        for k, v in new.items():
            if k not in ['trades', 'notifications']:
                try:
                    if json.dumps(v, sort_keys=True, default=str) != json.dumps(old.get(k), sort_keys=True, default=str):
                        changed[k] = v
                except:
                    if old.get(k) != v:
                        changed[k] = v
        
        return changed

    with app.app_context():
        while True:
            try:
                # Get current exchange from Redis
                current_exchange = redis_client.hget("global_filter", "exchange")
                if isinstance(current_exchange, bytes):
                    current_exchange = current_exchange.decode('utf-8')
                if not current_exchange:
                    current_exchange = "nse"  # Default exchange
                
                # Get data for current exchange only
                data = get_comprehensive_data(current_exchange)
                
                if data:
                    # Get previous data for this exchange
                    exchange_previous_data = previous_data.get(current_exchange, {})
                    
                    # Check for changes
                    changed = get_changed_values(exchange_previous_data, data, current_exchange)
                    
                    if changed:
                        # Update previous data for this exchange
                        previous_data[current_exchange] = data
                        
                        # Emit only to clients interested in this exchange
                        socketio.emit('data_update', changed)
                        
                        # Log what we're sending
                        if 'notifications' in changed:
                            print(f"Emitting notifications for {current_exchange}")
                
            except Exception as e:
                print(f"Error in background_data_emitter: {e}")
            
            eventlet.sleep(0.5)

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)
    CORS(app)
    db.init_app(app)
    app.redis_client = redis.Redis(host=Config.REDIS_HOST, port=Config.REDIS_PORT, db=0, password=Config.REDIS_PASSWORD, decode_responses=True)
    app.register_blueprint(trade_bp, url_prefix='/api')
    app.register_blueprint(chart_bp)
    app.register_blueprint(admin_bp)

    @app.route('/')
    def home(): 
        return render_template('index.html')

    @app.route('/metrics/thread_data')
    def get_thread_data():
        try:
            data = redis_client.zrange("threads_task_count", 0, -1, withscores=True)
            return jsonify([{"data":json.loads(m.decode()) if isinstance(m,bytes) else m} for m,_ in data])
        except: 
            return jsonify([])
    
    return app

def main():
    app = create_app()
    socketio = SocketIO(app, cors_allowed_origins="*")
    init_socketio_events(socketio)
    Thread(target=background_data_emitter, args=(app,socketio), daemon=True).start()
    socketio.run(app, host='0.0.0.0', port=5001, debug=True, use_reloader=False)

if __name__ == '__main__': 
    main()