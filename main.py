import subprocess
import os
import sys

def run_app():
    app_path = "services/backend_api/app.py"
    abs_path = os.path.abspath(app_path)

    print("➡ Trying to run:", abs_path)

    if not os.path.exists(app_path):
        print(f"❌ Error: {app_path} not found.")
        return

    try:
        result = subprocess.run([sys.executable, app_path])
        print("✅ Subprocess completed.")
    except Exception as e:
        print("❌ Exception occurred while running app.py:")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    run_app()