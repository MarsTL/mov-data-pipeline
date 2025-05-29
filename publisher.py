
from google.cloud import pubsub_v1
import requests
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import os
from dotenv import load_dotenv
load_dotenv() 
from concurrent import futures
from google.oauth2 import service_account


SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")

project_id = "mov-data-eng"
topic_id = "bus-breadcrumbs"

pubsub_creds = (service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE))
publisher = pubsub_v1.PublisherClient(credentials=pubsub_creds)
topic_path = publisher.topic_path(project_id, topic_id)

with open("vehicle_IDs.txt", "r") as f:
    vehicle_ids = [line.strip() for line in f if line.strip()]

now = datetime.now(ZoneInfo("America/Los_Angeles"))
today = now.strftime("%Y-%m-%d")

 
def future_callback(future):
  try:
    future.result()
  except Exception as e:
    print(f"Error publishing message: {e}")


count = 0
future_list =[]

for vehicle_id in vehicle_ids:
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"

    try:
        response = requests.get(url)
        if response.status_code == 404:
            print(f"Failed to fetch data for {vehicle_id}")
        else:
            records = response.json()


            for record in records:
                message = json.dumps(record).encode("utf-8")
                future = publisher.publish(topic_path, data=message)
                future.add_done_callback(future_callback)
                future_list.append(future)
                count += 1
                if count % 50000 == 0:
                    print(f"Published {count} messages.")
            print(f"Published {len(records)} messages for vehicle {vehicle_id}")
            print(f"Wrote {len(records)} records for vehicle {vehicle_id}")
    except Exception as e:
        print(f"Error for {vehicle_id}: {e}")

for future in futures.as_completed(future_list):
        continue
            
print(f"Finished gathering breadcrumbs for {today}")
