#!/usr/bin/env python3

import requests
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import os

# Load vehicle IDs
with open("vehicle_IDs.txt", "r") as f:
    vehicle_ids = [line.strip() for line in f if line.strip()]

# Get current time
now = datetime.now(ZoneInfo("America/Los_Angeles"))
today = now.strftime("%Y-%m-%d")

# Create output directory with current date
output_dir = f"/opt/shared/mov-data-pipeline/bus_data/{today}"
os.makedirs(output_dir, exist_ok=True)

# Fetch all bus data per vehicle and add it to a file
for vehicle_id in vehicle_ids:
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
    filename = os.path.join(output_dir, f"{vehicle_id}.json")

    try:
        response = requests.get(url)
        if response.status_code == 404:
            print(f"Failed to fetch data for {vehicle_id}")
        else:
            records = response.json()
            if records:  # Only write file if there is data
                with open(filename, "w") as outfile:
                    json.dump(records, outfile, indent=2)
                print(f"Wrote {len(records)} records for vehicle {vehicle_id}")
            else:
                print(f"No data for vehicle {vehicle_id}, file not created.")
    except Exception as e:
        print(f"Error for {vehicle_id}: {e}")

print(f"Finished gathering breadcrumbs for {today}")
