# keep_cpu_busy.py
import time

print("Keeping the CPU busy for a bit...")

end_time = time.time() + 60 * 10  # Run for 10 minutes

while time.time() < end_time:
    # Do some basic math in a loop to keep CPU busy
    for i in range(1000000):
        _ = i ** 2
    time.sleep(0.1)  # small sleep to avoid overheating

