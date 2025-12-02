#!/usr/bin/env python3

import time
import subprocess
import sys
from datetime import datetime, timedelta


def run_ingest():
    """Run store_data.py as a subprocess and print its output."""
    try:
        result = subprocess.run(
            [sys.executable, "gtfs_ingest.py"],
            capture_output=True,
            text=True,
            check=False
        )
        print("=== store_data.py output ===")
        print(result.stdout)
        print(result.stderr)
        print("============================")

    except Exception as e:
        print("Error running store_data.py:", e)


def main():
    INTERVAL = 30              # seconds
    DURATION = 2 * 60 * 60     # 2 hours (in seconds)

    print("Starting 2-hour execution loop...")
    end_time = datetime.now() + timedelta(seconds=DURATION)

    while datetime.now() < end_time:
        now = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{now}] Running gtfs_ingest.py ...")

        run_ingest()

        time.sleep(INTERVAL)

    print("\nFinished running for 2 hours.")


if __name__ == "__main__":
    main()
