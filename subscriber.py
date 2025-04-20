from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import json
from datetime import datetime
from google.oauth2 import service_account

project_id = "mov-data-eng"
subscription_id = "bus-breadcrumbs-sub"
SERVICE_ACCOUNT_FILE = "/opt/shared/mov-data-pipeline/service-account.json"

pubsub_creds2 = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
subscriber = pubsub_v1.SubscriberClient(credentials=pubsub_creds2)
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):
    try:
        data = json.loads(message.data.decode('utf-8'))
        if 'timestamp' not in data:
            print(f"Skipping message with missing timestamp: {data}")
            message.ack()
            return
        timestamp = datetime.fromtimestamp(data['timestamp'])
        filename = timestamp.strftime('%Y-%m-%d') + '.json'
        with open(filename, 'a') as f:
            json.dump(data, f)
            f.write('\n')
        message.ack()
    except Exception as e:
        print(f"Error processing message: {e}")
        message.nack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")
with subscriber:
    try:
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()
        print("Timeout occurred, exiting.")
        exit(0)
