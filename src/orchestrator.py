# Save this file as: src/orchestrator.py

import subprocess
import sys
import os

# --- 1. Define the Pipeline ---
# List of scripts to run in the exact order of execution.
# Paths are relative to the *root* of your project directory.
SCRIPT_PIPELINE = [
    'src/data_acquisition.py',
    'src/add_raster_features.py',
    'src/add_weather_features.py',
    'src/train_model.py'
]

def run_script(script_path):
    """
    Executes a Python script as a subprocess.
    Uses the same Python executable that is running this orchestrator.
    """
    print(f"\n--- 🚀 Starting: {script_path} ---")
    
    # Use sys.executable to ensure we use the same Python interpreter
    command = [sys.executable, script_path]
    
    try:
        # Execute the script
        # check=True: Raises an error if the script returns a non-zero exit code
        # text=True: Decodes stdout/stderr as text (requires Python 3.7+)
        subprocess.run(command, check=True, text=True)
        
        print(f"--- ✅ Finished: {script_path} ---")
        return True
        
    except FileNotFoundError:
        print(f"--- ❌ FAILED: Script not found at {script_path} ---")
        return False
    except subprocess.CalledProcessError as e:
        print(f"--- ❌ FAILED: {script_path} ---")
        print(f"   An error occurred. Return code: {e.returncode}")
        # Optionally print stdout/stderr from the failed script
        # print(f"   STDOUT: {e.stdout}")
        # print(f"   STDERR: {e.stderr}")
        return False
    except Exception as e:
        print(f"--- ❌ FAILED: An unexpected error occurred with {script_path} ---")
        print(f"   Error: {e}")
        return False

def main():
    """
    Runs the full data processing and modeling pipeline.
    """
    print("===  orchestrator.py: Starting the NPK Prediction Pipeline ===")
    
    # Get the directory of this orchestrator script
    # and go one level up to get the project root.
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Change the current working directory to the project root
    # This ensures all relative paths (e.g., 'data/raw/') in your
    # scripts work correctly.
    os.chdir(project_root)
    print(f"Working Directory set to: {os.getcwd()}")
    
    for script_path in SCRIPT_PIPELINE:
        if not run_script(script_path):
            print(f"\n=== 🛑 Pipeline HALTED at {script_path} ===")
            sys.exit(1) # Exit with an error code to signal failure
            
    print("\n=== 🎉 Pipeline Completed Successfully ===")

if __name__ == "__main__":
    main()