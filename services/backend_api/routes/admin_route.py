from flask import Blueprint, jsonify, request, make_response, render_template
import subprocess, os, json, psutil
from datetime import datetime
from services.config import redis_client
from services.loger import logger
from services.backend_api.services.event_service import (
    AccountMatrixdata
)
admin_bp = Blueprint('admin', __name__, url_prefix='/admin')

@admin_bp.route('/dashboard')
def admin_dashboard():
    status = redis_client.get('pipeline:status')
    pipeline_status = status.decode('utf-8') == 'running' if status else False

    current_exchange_value = redis_client.get('pipeline:exchange')
    current_exchange = current_exchange_value.decode('utf-8') if current_exchange_value else 'binance'

    # Get hardware metrics
    try:
        # Use HGETALL to fetch the hash data
        metrics = redis_client.hgetall('system:metrics')
        # Convert byte keys and values to strings
        hardware_data = {key.decode(): value.decode() for key, value in metrics.items()}
    except Exception as e:
        hardware_data = {}
        logger.error(f"Failed to fetch system metrics: {e}")

    # Get process data
    process_keys = redis_client.keys('process:*')
    processes = []
    for key in process_keys:
        # Skip keys for system processes
        if b'system:' in key:
            continue
        process_data = redis_client.hgetall(key)
        processes.append({
            'name': key.decode().split(':')[1],
            'status': process_data.get(b'status', b'unknown').decode(),
            'pid': process_data.get(b'pid', b'0').decode(),
            'last_ping': process_data.get(b'last_ping', b'0').decode()
        })

    response = make_response(render_template(
        'admin.html',
        pipeline_status=pipeline_status,
        current_exchange=current_exchange,
        hardware_data=hardware_data,
        processes=processes
    ))
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

@admin_bp.route('/hardware/status')
def hardware_status():
    try:
        metrics = redis_client.get('system:metrics')
        if metrics:
            # Decode and load the JSON data
            metrics_data = json.loads(metrics.decode())
            # Return the full set of metrics as stored in Redis
            return jsonify(metrics_data)
        return jsonify({"error": "No metrics found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@admin_bp.route('/user_pairs', methods=['POST'])
def update_user_pairs():
    try:
        if not request.is_json:
            return jsonify({"success": False, "error": "Request must be JSON"}), 400

        data = request.get_json()
        symbol_type = data.get('symbol_type')
        user_pairs = data.get('user_pairs')

        if not symbol_type or symbol_type not in ['nse', 'binance', 'snp', 'etf']:
            return jsonify({"success": False, "error": "Invalid or missing 'symbol_type'"}), 400
        if not user_pairs or not isinstance(user_pairs, list):
            return jsonify({"success": False, "error": "Missing or invalid 'user_pairs'"}), 400

        redis_key = f"manual_symbols:user_pairs_{symbol_type}"

        # Delete previous record before saving new one
        redis_client.delete(redis_key)

        # Save new record
        redis_client.set(redis_key, json.dumps(user_pairs))

        return jsonify({
            "success": True,
            "message": f"user_pairs for {symbol_type} updated successfully",
            "data": user_pairs
        }), 200

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/capital', methods=['POST'])
def update_total_capital():
    try:
        if not request.is_json:
            return jsonify({"error": "Request must be JSON"}), 400

        data = request.get_json()
        #print("Received data:", data)

        if not data or not isinstance(data, dict):
            return jsonify({"error": "Invalid request data, expected JSON object"}), 400

        total_capital = data.get('total_capital')
        if total_capital is None:
            return jsonify({"error": "Missing 'total_capital' in request"}), 400

        key = "account_matrix:account"

        # Ensure Redis connection
        if not redis_client.ping():
            return jsonify({"error": "Redis connection failed"}), 500

        # Check if the key exists
        if not redis_client.exists(key):
            print(f"Creating new key: {key}")
        else:
            pass
            #print(f"Updating existing key: {key}")

        # Convert all values to strings
        redis_data = {str(k): str(v) for k, v in data.items()}

        # FIX: Use ** unpacking instead of mapping argument
        for field, value in redis_data.items():
            redis_client.hset(key, field, value)

        # Verify insertion
        stored_data = redis_client.hgetall(key)
        print(f"Stored Data in Redis: {stored_data}")

        return jsonify({"message": f"Data inserted successfully using key '{key}'."}), 200

    except Exception as e:
        import traceback
        logger.error(f"Error updating total capital: {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


# @admin_bp.route('/hardware/status')
# def hardware_status():
#     try:
#         key_type = redis_client.type('system:metrics')
#         metrics = redis_client.get('system:metrics')
#         if metrics:
#             metrics_data = json.loads(metrics.decode())
#             cpu_val = metrics_data.get('cpu_percent', 0)
#             if not cpu_val or cpu_val == 0:
#                 cpu_val = psutil.cpu_percent(interval=0.1)
#
#             mem_used = metrics_data.get('memory_used', 0)
#             mem_total = metrics_data.get('memory_total', 1)  # avoid division by zero
#
#             return jsonify({
#                 'cpu_percent': cpu_val,
#                 'memory_percent': (float(mem_used) / float(mem_total) * 100),
#                 'load_avg': metrics_data.get('load_avg', '0,0,0')
#             })
#
#         return jsonify({})
#     except Exception as e:
#         return jsonify(error=str(e)), 500


@admin_bp.route('/threads')
def get_threads():
    try:
        process_keys = redis_client.keys('process:*')
        threads = []
        for key in process_keys:
            if b'system:' in key:
                continue
            process_data = redis_client.hgetall(key)
            decoded_data = {
                'name': key.decode().split(':')[1],
                'status': process_data.get(b'status', b'unknown').decode(),
                'start_time': process_data.get(b'start_time', b'0').decode(),
                'pid': process_data.get(b'pid', b'0').decode(),
                'last_ping': process_data.get(b'last_ping', b'0').decode(),
                'host': process_data.get(b'host', b'unknown').decode()
            }
            threads.append(decoded_data)
        return jsonify(threads)
    except Exception as e:
        return jsonify(error=str(e)), 500


@admin_bp.route('/control/process', methods=['POST'])
def control_process():
    try:
        data = request.json
        process_name = data.get('name')
        action = data.get('action')
        # Set the control field in Redis to signal the process
        redis_client.hset(f'process:{process_name}', 'control', action)
        return jsonify(success=True)
    except Exception as e:
        return jsonify(error=str(e)), 500


def get_manage_path():
    """Get absolute path to manage.py from project root"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(current_dir, '..', '..', '..', 'manage.py'))


@admin_bp.route('/pipeline/<action>', methods=['POST'])
def control_pipeline(action):
    data = request.json
    exchange = data.get('exchange', 'binance')
    mode = data.get('mode', 'automatic')  # <-- Get symbol mode from request

    manage_script = get_manage_path()
    if not os.path.exists(manage_script):
        return jsonify(error="Manage.py not found!"), 500

    try:
        if action == 'start':
            # Stop any existing pipeline
            pid = redis_client.get('pipeline:pid')
            if pid:
                try:
                    os.kill(int(pid), 9)
                except Exception:
                    pass

            # Start new pipeline process
            process = subprocess.Popen([
                'python', manage_script,
                '--exchange', exchange
            ], cwd=os.path.dirname(manage_script))

            # Store pipeline state
            redis_client.set('pipeline:pid', process.pid)
            redis_client.set('pipeline:status', 'running')
            redis_client.set('pipeline:exchange', exchange)
            redis_client.set('pipeline:symbol_mode', mode)  # <-- Store selection mode

        elif action == 'stop':
            pid = redis_client.get('pipeline:pid')
            if pid:
                try:
                    os.kill(int(pid), 9)
                except Exception:
                    pass
            redis_client.set('pipeline:status', 'stopped')

        # Retrieve current pipeline state
        status = redis_client.get('pipeline:status') or b'stopped'
        current_exchange = redis_client.get('pipeline:exchange') or b'binance'
        current_mode = redis_client.get('pipeline:mode') or b'automatic'

        return jsonify({
            'success': True,
            'status': status.decode('utf-8') if isinstance(status, bytes) else status,
            'exchange': current_exchange.decode('utf-8') if isinstance(current_exchange, bytes) else current_exchange,
            'mode': current_mode.decode('utf-8') if isinstance(current_mode, bytes) else current_mode
        })

    except Exception as e:
        return jsonify(error=str(e)), 500



@admin_bp.route('/system/config', methods=['GET', 'POST'])
def system_config():
    try:
        if request.method == 'POST':
            print("Received POST /system/config")
            data = request.get_json(force=True)
            print("Data received:", data)
            config = {
                'max_cpu_cores': int(data.get('max_cpu_cores', 8)),
                'memory_limit_gb': int(data.get('memory_limit_gb', 8)),
                'process_ttl': int(data.get('process_ttl', 300)),
                'system_monitor_interval': int(data.get('system_monitor_interval', 5)),
                'max_restart_attempts': int(data.get('max_restart_attempts', 3))
            }
            redis_client.set('system:config', json.dumps(config))
            print("Config saved:", config)
            return jsonify(success=True, config=config)
        else:
            config = redis_client.get('system:config')
            if config:
                config = json.loads(config.decode())
            else:
                config = {
                    'max_cpu_cores': 8,
                    'memory_limit_gb': 8,
                    'process_ttl': 300,
                    'system_monitor_interval': 5,
                    'max_restart_attempts': 3
                }
            return jsonify(config)
    except Exception as e:
        print("Error:", e)
        return jsonify(error=str(e)), 500


@admin_bp.route('/account_matrix_data', methods=['GET'], endpoint='get_account_matrix_data')
def get_account_matrix_data():
    """
    Endpoint to get account matrix data.
    """
    try:
        return AccountMatrixdata.Mtarixdata()
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Failed to fetch matrix: {str(e)}"}), 500