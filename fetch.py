#!/usr/bin/env python3

from google.cloud import pubsub_v1
import requests
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import os

#new 4/16 create pub/sub publisher 
publisher = pubsub_v1.PublisherClient()
project_id = "mov-data-eng"
topic_path = publisher.topic_path(project_id, "bus-breadcrumbs")

# Load vehicle IDs
with open("vehicle_IDs.txt", "r") as f:
    vehicle_ids = [line.strip() for line in f if line.strip()]

# Get current time
now = datetime.now(ZoneInfo("America/Los_Angeles"))
today = now.strftime("%Y-%m-%d")

# Create file with current date 
output_dir = f"/opt/shared/mov-data-pipeline/bus_data/{today}"
os.makedirs(output_dir, exist_ok=True)

#filename = os.path.join(output_dir, f"breadcrumbs_{today}.json")

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

            #save to file
            #with open(filename, "w") as outfile:
            with open(filename, "w") as f:
                #json.dump(records, outfile, indent=2)  
                json.dump(records, f, indent=2)

            #publish pub/sub
            for record in records:
                message = json.dumps(record).encode("utf-8")
                publisher.publish(topic_path, data=message)

            print(f"Published {len(records)} messages for vehicle {vehicle_id}")

            print(f"Wrote {len(records)} records for vehicle {vehicle_id}")
    except Exception as e:
        print(f"Error for {vehicle_id}: {e}")
            
print(f"Finished gathering breadcrumbs for {today}")
