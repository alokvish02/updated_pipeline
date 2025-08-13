import subprocess
import os
import sys
from pathlib import Path

def run_app():
    # Get the project root directory (where tp.py is located)
    project_root = Path(__file__).parent.absolute()
    
    # Add project root to Python path
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    
    # Set PYTHONPATH environment variable
    env = os.environ.copy()
    env['PYTHONPATH'] = str(project_root)
    
    # Change to project directory
    os.chdir(project_root)
    
    app_path = "services/algo_signals/backtester_spreads.py"
    abs_path = project_root / app_path
    
    print(f"➡ Project Root: {project_root}")
    print(f"➡ PYTHONPATH: {env.get('PYTHONPATH', 'Not set')}")
    print(f"➡ Current Directory: {os.getcwd()}")
    print(f"➡ Trying to run: {abs_path}")
    
    if not abs_path.exists():
        print(f"❌ Error: {abs_path} not found.")
        print("Available files in services/algo_signals/:")
        algo_signals_dir = project_root / "services" / "algo_signals"
        if algo_signals_dir.exists():
            for file in algo_signals_dir.iterdir():
                print(f"  - {file.name}")
        return
    
    try:
        # Run with proper environment and working directory
        result = subprocess.run(
            [sys.executable, str(abs_path)],
            env=env,
            cwd=str(project_root),
            capture_output=True,
            text=True
        )
        
        print("STDOUT:")
        print(result.stdout)
        
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        
        print(f"✅ Process completed with return code: {result.returncode}")
        
    except Exception as e:
        print("❌ Exception occurred while running backtester:")
        import traceback
        traceback.print_exc()

def run_direct():
    """Alternative: Run the backtester directly in the same process"""
    print("➡ Running backtester directly...")
    
    # Get the project root directory
    project_root = Path(__file__).parent.absolute()
    
    # Add project root to Python path
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    
    # Change to project directory
    os.chdir(project_root)
    
    try:
        # Import and run the backtester directly
        from services.algo_signals.backtester_spreads import run_backtest
        
        print("✅ Successfully imported backtester")
        print("🚀 Starting backtest...")
        
        # Run backtest
        results= run_backtest(
            symbol_pair='btcusdt_ethusdt',
            start_date='2025-01-01',
            exchange='binance'
        )
        
        print("✅ Backtest completed successfully!")
        
    except ImportError as e:
        print(f"❌ Import Error: {e}")
        print("Make sure all dependencies are installed and paths are correct")
    except Exception as e:
        print(f"❌ Error running backtester: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    print("=" * 50)
    print("🔥 SPREAD TRADING BACKTESTER RUNNER")
    print("=" * 50)
    
    # Try direct import first (recommended)
    print("\n1️⃣ Attempting direct import method...")
    try:
        run_direct()
    except Exception as e:
        print(f"❌ Direct method failed: {e}")
        print("\n2️⃣ Falling back to subprocess method...")
        run_app()