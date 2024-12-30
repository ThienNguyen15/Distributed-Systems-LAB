import threading
import subprocess
import os

current_directory = os.path.dirname(os.path.abspath(__file__))
python_interpreter = r".\Scripts\python.exe"
scripts = [
    '.\Producer\producer.py',
    '.\Consumer\consumer_partition 0.py',
    '.\Consumer\consumer_partition 1.py'
]

def run_script(script):
    script_path = os.path.join(current_directory, script)
    try:
        subprocess.run([python_interpreter, script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error")

threads = []
for script in scripts:
    thread = threading.Thread(target=run_script, args=(script,))
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()
