from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import json
from datetime import datetime
from google.oauth2 import service_account
import pandas as pd
import psycopg2
import os
import io
import csv

#config
project_id = "mov-data-eng"
subscription_id = "bus-breadcrumbs-sub"
SERVICE_ACCOUNT_FILE = "/opt/shared/mov-data-pipeline/service-account.json"

#pub/sub setup
pubsub_creds2 = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
subscriber = pubsub_v1.SubscriberClient(credentials=pubsub_creds2)
subscription_path = subscriber.subscription_path(project_id, subscription_id)

#directory
OUTPUT_DIR = '/opt/shared/mov-data-pipeline/output'
os.makedirs(OUTPUT_DIR, exist_ok=True)
def db_connect():
    return psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="mov_engine_data",
            host="localhost"
    )


json_data = []
TableName = 'breadcrumb'
TableName2 = 'trip'



def validate_event_no_stop(df):
    try:
        df['EVENT_NO_STOP'] =  pd.to_numeric(df['EVENT_NO_STOP'], errors='coerce')
        correct = df['EVENT_NO_STOP'].notnull().all()
        if not correct:
            print("EVENT_NO_STOP contains values that are not numeric. converting to nan")
        return df
    except Exception as e:
        print(f"Error validating column event no stop: {e}")
        return df
def validate_event_no_trip(df):
  try:
    df['EVENT_NO_TRIP'] = pd.to_numeric(df['EVENT_NO_TRIP'], errors='coerce')
    correct = df['EVENT_NO_TRIP'].notnull().all() 
    if not correct:
      print("EVENT_NO_TRIP contains values that are not numeric. converting to nan")
    return df
  except Exception as e:
    print(f"Error validating column event no trip: {e}")
    return df

def convert_to_datetime(df):
  try:
    if 'OPD_DATE' in df.columns:
      df['OPD_DATE'] = df['OPD_DATE'].astype(str).str.strip()
      df['OPD_DATE'] = pd.to_datetime(df['OPD_DATE'], format='%d%b%Y:%H:%M:%S', errors='coerce')
        #validate datetime
      # wrong datetime values
      invalid_datetime_rows = df['OPD_DATE'].isnull().sum()
      if invalid_datetime_rows > 0:
        print(f"{invalid_datetime_rows} with invalid datetime values.converting to nan")
      return df
  except Exception as e:
    print(f"Error validating OPD_DATE: {e}")
    return df

def validate_gps_hdop(df):
  try:
    df['GPS_HDOP'] = pd.to_numeric(df['GPS_HDOP'], errors='coerce')
    #invalid gps out of range between 0 and 40
    invalid_gps_values = (~df['GPS_HDOP'].between(0, 40)& df['GPS_HDOP'].notnull()).sum()
    if invalid_gps_values > 0:
      print(f"{invalid_gps_values} with invalid gps values.converting to nan")
      df.loc[~df['GPS_HDOP'].between(0, 40), 'GPS_HDOP'] = float('nan')
    return df
  except Exception as e:
    print(f"Error validating GPS_HDOP: {e}")
    return df


def validate_gps_satellites(df):
  try:
    df['GPS_SATELLITES'] = pd.to_numeric(df['GPS_SATELLITES'], errors='coerce')
    invalid_gps_satellite_values = (~df['GPS_SATELLITES'].between(0, 24)& df['GPS_SATELLITES'].notnull()).sum()
    if invalid_gps_satellite_values > 0:
      print(f"{invalid_gps_satellite_values} with invalid gps values.converting to nan")
      df.loc[~df['GPS_SATELLITES'].between(0, 24), 'GPS_SATELLITES'] = float('nan')
    return df
  except Exception as e:
    print(f"Error validating GPS_SATELLITES: {e}")
    return df


def validate_latitude(df):
  try:
    df['GPS_LATITUDE'] = pd.to_numeric(df['GPS_LATITUDE'], errors='coerce')
    #invalid
    invalid_latitude_values = (~df['GPS_LATITUDE'].between(45,46)&df['GPS_LATITUDE'].notnull()).sum()
    if invalid_latitude_values > 0:
      print(f"{invalid_latitude_values} with invalid latitude values.converting to nan")
      df.loc[~df['GPS_LATITUDE'].between(45,46), 'GPS_LATITUDE'] = float('nan')
    return df
  except Exception as e:
    print(f"Error validating LATITUDE: {e}")
    return df

def validate_longitude(df):
  try:
    df['GPS_LONGITUDE'] = pd.to_numeric(df['GPS_LONGITUDE'], errors='coerce')
    invalid_longitude_values = (~df['GPS_LONGITUDE'].between(-124, -122)&df['GPS_LONGITUDE'].notnull()).sum()
    if invalid_longitude_values > 0:
      print(f"{invalid_longitude_values} with invalid longitude values.converting to nan")
      df.loc[~df['GPS_LONGITUDE'].between(-124, -122), 'GPS_LONGITUDE'] = float('nan')
    return df
  except Exception as e:
    print(f"Error validating LONGITUDE: {e}")
    return df

def validate_meters(df):
  try:
    df['METERS'] = pd.to_numeric(df['METERS'], errors='coerce')
    correct = df['METERS'].notnull().all() 
    if not correct:
      print("METERS contains values that are not numeric. converting to nan")
    return df
  except Exception as e:
    print(f"Error validating column meters: {e}")
    return df
def validate_act_time(df):
  try:
    df['ACT_TIME'] = pd.to_numeric(df['ACT_TIME'], errors='coerce')
    correct = df['ACT_TIME'].notnull().all() 
    if not correct:
      print("ACT_TIME contains values that are not numeric. converting to nan")
    return df
  except Exception as e:
    print(f"Error validating column act_time: {e}")
    return df
def validate_vehicle_id(df):
  try:
    df['VEHICLE_ID'] = pd.to_numeric(df['VEHICLE_ID'], errors='coerce')
    correct = df['VEHICLE_ID'].notnull().all() 
    if not correct:
      print("VEHICLE_ID contains values that are not numeric. converting to nan")
    return df
  except Exception as e:
    print(f"Error validating column vehicle_id: {e}")
    return df
  
def validate_data(df):
  df = validate_event_no_stop(df)
  df = validate_event_no_trip(df)
  df = convert_to_datetime(df)
  df = validate_gps_hdop(df)
  df = validate_gps_satellites(df)
  df = validate_latitude(df)
  df = validate_longitude(df)
  df = validate_meters(df)
  df = validate_act_time(df)
  df = validate_vehicle_id(df)
  return df

def transform_data(df):
  try:
    #check for missing required columns
    required_columns = ['OPD_DATE', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'ACT_TIME','METERS']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
      print(f"Missing required columns: {', '.join(missing_columns)}")
      return None
    df = df.copy()
    #drop nan rows in important columns
    important = ['OPD_DATE', 'GPS_LATITUDE', 'GPS_LONGITUDE','EVENT_NO_TRIP']
    df = df.dropna(subset=important)

    if df.empty:
      print("No valid data to transform")
      return None
    #convert to datetime

    #time delta value for act_time
    df['ACT_TIME'] = pd.to_timedelta(df['ACT_TIME'].astype(float), unit ='seconds')

    df['TIMESTAMP'] = df.apply(lambda y: y['OPD_DATE'] + y['ACT_TIME'], axis=1)

    df = df.drop(['OPD_DATE', 'ACT_TIME'], axis=1)

    df = df.sort_values(['EVENT_NO_TRIP', 'TIMESTAMP'])

    # dMETERS and dTIMESTAMP
    df['dMETERS'] = df.groupby('EVENT_NO_TRIP')['METERS'].diff()
    df['dTIMESTAMP'] = df.groupby('EVENT_NO_TRIP')['TIMESTAMP'].diff()

    df['SPEED'] = df.apply(lambda x:x['dMETERS'] / x['dTIMESTAMP'].total_seconds() 
    if pd.notnull(x['dTIMESTAMP']) and x['dTIMESTAMP'].total_seconds() > 0.1 else 0, axis=1)

    # df['SPEED'] = df.apply(lambda x:x['dMETERS'] / x['dTIMESTAMP'].total_seconds(), axis=1)
    df['SPEED'] = df.apply(lambda x:x['dMETERS'] / x['dTIMESTAMP'].total_seconds() if pd.notnull(x['dTIMESTAMP']) and x['dTIMESTAMP'].total_seconds() > 0 else 0, axis=1)

    # df = df.drop(['dMETERS', 'dTIMESTAMP'], axis=1)
    # range for speed
    invalid_range_for_speed = (df['SPEED'] < 0) | (df['SPEED'] > 95)
    if invalid_range_for_speed.any():
      print(f"{invalid_range_for_speed.sum()} invalid range for speed:converting to 0 ")
      df.loc[invalid_range_for_speed, 'SPEED'] = 0
    #validate nan
    if df['SPEED'].isnull().any():
      print("SPEED contains null values. converting to 0")
      df['SPEED'] = df['SPEED'].fillna(0)
    return df
  except Exception as e:
    print(f"Error transforming data: {e}")
    return None
#function to get existing trip ids from the trip table
def get_existing_trip_ids():
  conn = None
  try:
      conn = db_connect()
      with conn.cursor() as cursor:
          cursor.execute(f"SELECT trip_id FROM {TableName2}")
          existing_trip_ids = [row[0] for row in cursor.fetchall()]
          return existing_trip_ids
  except Exception as e:
      print(f"Error getting existing trip ids: {e}")
      return []
  finally:
      if conn:
          conn.close()
#function to insert trip_id into trip table
def insert_trip_id(trip_ids):
  conn = None
  try:
    conn = db_connect()
    with conn.cursor() as cursor:
      cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'trip'")

      columns = [col[0] for col in cursor.fetchall()]
      print(f'trip columns: {columns}')
      for trip_id in trip_ids:
        try:
          cursor.execute(f"INSERT INTO {TableName2} (trip_id) VALUES (%s) ON CONFLICT (trip_id) DO NOTHING", (trip_id,))
        except Exception as e:
          print(f"Error inserting trip id {trip_id}: {e}")
          continue
      conn.commit()
      print(f"Inserted {len(trip_ids)} trip ids into the database")
      return True
  except Exception as e:
    print(f"Error inserting trip ids: {e}")
    if conn:
      conn.rollback()
    return False
  finally:
    if conn:
      conn.close()

#store both trip and everthing in the database
def store_database(df):
  conn = None
  #handle nan values for required columns
  try:
    df_new = df.copy()
    required_columns = ['TIMESTAMP', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'SPEED', 'EVENT_NO_TRIP']
    for col in required_columns:
      if col not in df_new.columns:
        print(f"Column {col} not found in DataFrame")
        return False
    #check for existing trip ids
    trip_ids = df_new['EVENT_NO_TRIP'].unique().tolist()
    print(f"trip ids: {len(trip_ids)} unique")
    existing_trip_ids = get_existing_trip_ids()
    new_trip_ids = [trip_id for trip_id in trip_ids if trip_id not in existing_trip_ids]
    if new_trip_ids:
      print(f"Inserting {len(new_trip_ids)} new trip ids into the database")
      if not insert_trip_id(new_trip_ids):
        print("Error inserting trip ids into the database")
        return False
      
      df_new = df_new.rename(columns={'TIMESTAMP': 'tstamp', 'GPS_LATITUDE': 'latitude', 'GPS_LONGITUDE': 'longitude', 'SPEED': 'speed', 'EVENT_NO_TRIP': 'trip_id'})
      dataframe_data =  df_new[['tstamp', 'latitude', 'longitude','speed', 'trip_id']]
  
      f = io.StringIO()
      dataframe_data.to_csv(f, header=False, index=False, sep ='\t')
      f.seek(0)
      conn = db_connect()
      with conn.cursor() as cursor:
          cursor.copy_from(f, TableName, sep='\t')
          conn.commit()
          print(f'stored {len(dataframe_data)} records in database')
          return True
  except Exception as e:
      print(f"error storing in database{e}")  
      if conn:  
          conn.rollback()
      return False
  finally:
      if conn:
        conn.close()

def callback(message):
  try:
     data = json.loads(message.data.decode('utf-8'))
     json_data.append(data)
     message.ack() 
  except Exception as e:
    print(f"Error processing message: {e}")
    message.nack()
 


def other_process(json_data):
  if not json_data:
    print("No data to process")
    return None

  try:
    df = pd.DataFrame(json_data)
    df = validate_data(df)
    print("data validation complete")
    df = transform_data(df)
    print("data transform complete")

    if df is not None and not df.empty:
      save_db = store_database(df)
      if save_db:
        json_data = df.to_dict(orient='records')
        timestamp = datetime.now()
        filename = os.path.join(OUTPUT_DIR, timestamp.strftime('%Y-%m-%d') + '.json')
        try:
          with open(filename, 'a') as f:
            for data in json_data:
              json.dump(data, f)
              f.write('\n')
          print(f"Saved {len(json_data)} records to {filename}")
        except Exception as e:
          print(f"Error saving data to file: {e}")
        json_data.clear()
        return df
      else:
        print("Error storing data in database")
    else:
      print("No valid data to process")
    json_data.clear()
    return None
  except Exception as e:
    print(f"Error processing data: {e}")
    json_data.clear()
    return None


print(f"Listening for messages on {subscription_path}..\n")
while True:
  try:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result(timeout = 20)
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()
        print("Timeout occurred, exiting.")
    pro_df = other_process(json_data)
    if pro_df is not None:
      print(f"successfully processed  {len(pro_df)} records")
    else:
      print("no data to process")
  except KeyboardInterrupt:
    print('interrupted by keyboard')
    break
  except Exception as e:
    print(f"Error processing loop: {e}")
    continue
