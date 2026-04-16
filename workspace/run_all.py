import subprocess
import time
import sys

scripts = [
    "openaq_producer.py",
    "noaa_producer.py",
    "purpleair_producer.py", 
    "traffic_producer.py"    
]

processes = []

print("🚀 Starting all Big Data ingestion streams...")

# Launch each script as a separate background process
for script in scripts:
    print(f"--> Booting {script}")
    # Popen runs the script without pausing the main program
    p = subprocess.Popen([sys.executable, script])
    processes.append(p)

try:
    # Keep the main script alive to watch the background processes
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\n🛑 Shutting down all producers safely...")
    for p in processes:
        p.terminate()
    print("Done.")
    
#docker exec -it -w /home/jovyan/work jupyter-pyspark python run_all.py