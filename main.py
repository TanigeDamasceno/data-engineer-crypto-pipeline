import subprocess
import sys

def run_step(script_name):
    print(f"---Executing {script_name}---")
    result = subprocess.run([sys.executable, script_name], capture_output=True, text=True)
    if result.returncode == 0:
        print(result.stdout)
    else:
        print(f"Error in {script_name}:")
        print(result.stderr)
        
if __name__ == "__main__":
    print("Starting Medallion Architecture Pipeline")
    run_step('extract.py') # Bronze
    run_step('transform.py') # Silver 
    run_step('gold_analytics.py') # Gold 