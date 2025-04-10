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

# Create file with current date 
output_dir = "/opt/shared/bus_data"
os.makedirs(output_dir, exist_ok=True)
filename = os.path.join(output_dir, f"breadcrumbs_{today}.jsonl")

# Fetch all bus data and add it to a file
with open(filename, "a") as outfile:
    for vehicle_id in vehicle_ids:
        url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
        try:
            response = requests.get(url)
            if response.status_code == 404:
                print(f"Failed to fetch data for {vehicle_id}")
            else:
                records = response.json()
                # records.dumps("file.json")
                for record in records:
                    outfile.write(json.dumps(record) + "\n")
                print(f"Wrote data for vehicle {vehicle_id}")
        except Exception as e:
            print(f"Error for {vehicle_id}: {e}")
