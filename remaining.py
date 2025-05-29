import json
from datetime import datetime
import pandas as pd
import psycopg2
import os
import io
import csv
import shutil
import glob
from dotenv import load_dotenv 
load_dotenv()


INPUT_DIR = '/opt/shared/mov-data-pipeline/bus_data/2025-05-27'
OUTPUT_DIR = '/opt/shared/mov-data-pipeline/output2/2025-05-27'
os.makedirs(OUTPUT_DIR, exist_ok=True)
def db_connect():
    return psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST")
    )


TableName = 'breadcrumb'




def validate_event_no_stop(df):
  if df is None:
    return None

  try:
      var =  pd.to_numeric(df['EVENT_NO_STOP'], errors='coerce')
      df = df[var.notna()]
      var = var[var.notna()]
      assert var.notna().all(), "EVENT_NO_STOP contains values that are not numeric."
      df.loc[:,'EVENT_NO_STOP'] = var
      return df
  except Exception as e:
      print(f"Error validating column event no stop: {e}")
      return None

def validate_event_no_trip(df):
  if df is None:
    return None
  try:
    var = pd.to_numeric(df['EVENT_NO_TRIP'], errors='coerce')
    df = df[var.notna()]
    var = var[var.notna()]
    assert var.notna().all(), "EVENT_NO_TRIP contains values that are not numeric."
    df.loc[:,'EVENT_NO_TRIP'] = var
    return df
  except Exception as e:
    print(f"Error validating column event no trip: {e}")
    return None


def convert_to_datetime(df):
  if df is None:
    return None
  try:
    if 'OPD_DATE' in df.columns:
      df['OPD_DATE'] = df['OPD_DATE'].astype(str).str.strip()
      var= pd.to_datetime(df['OPD_DATE'], format='%d%b%Y:%H:%M:%S', errors='coerce')
      assert var.notna().all(), "OPD_DATE contains values that are not datetime."
      df.loc[:,'OPD_DATE'] = var
    return df
  except Exception as e:
    print(f"Error validating column OPD_DATE: {e}")
    return None

def validate_gps_hdop(df):
  if df is None:
    return None
  try:
    var = pd.to_numeric(df['GPS_HDOP'], errors='coerce')
    df = df[var.notna()]
    var = var[var.notna()]
    assert var.notna().all(), "GPS_HDOP contains values that are not numeric."
    assert var.between(0, 40).all(), "GPS_HDOP contains values that are not between 0 and 40."
    df.loc[:,'GPS_HDOP'] = var
    return df
  except Exception as e:
    print(f"Error validating column GPS_HDOP: {e}")
    return None



def validate_gps_satellites(df):
  if df is None:
    return None
  try:
    var = pd.to_numeric(df['GPS_SATELLITES'], errors='coerce')
    df = df[var.notna()]
    var = var[var.notna()]
    assert var.notna().all(), "GPS_SATELLITES contains values that are not numeric."
    assert var.between(0, 24).all(), "GPS_SATELLITES contains values that are not between 0 and 24."
    df.loc[:,'GPS_SATELLITES'] = var
    return df
  except Exception as e:
    print(f"Error validating column GPS_SATELLITES: {e}")
    return None


def validate_latitude(df):
  if df is None:
    return None
  try:
    var= pd.to_numeric(df['GPS_LATITUDE'], errors='coerce')
    df = df[var.notna()]
    var = var[var.notna()]
    assert var.notna().all(), "GPS_LATITUDE contains values that are not numeric."
    assert var.between(45, 46).all(), "GPS_LATITUDE contains values that are not between 45 and 46."
    df.loc[:,'GPS_LATITUDE'] = var
    return df
  except Exception as e:
    print(f"Error validating column GPS_LATITUDE: {e}")
    return None

def validate_longitude(df):
  if df is None:
    return None
  try:
    var = pd.to_numeric(df['GPS_LONGITUDE'], errors='coerce')
    df = df[var.notna()]
    var = var[var.notna()]
    assert var.notna().all(), "GPS_LONGITUDE contains values that are not numeric."
    assert var.between(-124, -122).all(), "GPS_LONGITUDE contains values that are not between -124 and -122."
    df.loc[:,'GPS_LONGITUDE'] = var
    return df
  except Exception as e:
    print(f"Error validating column GPS_LONGITUDE: {e}")
    return None


def validate_meters(df):
  if df is None:
    return None
  try:
    var = pd.to_numeric(df['METERS'], errors='coerce')
    df = df[var.notna()]
    var = var[var.notna()]
    assert var.notna().all(), "METERS contains values that are not numeric."
    df.loc[:,'METERS'] = var
    return df
  except Exception as e:
    print(f"Error validating column meters: {e}")
    return None

def validate_act_time(df):
  if df is None:
    return None
  try:
    var = pd.to_numeric(df['ACT_TIME'], errors='coerce')
    df = df[var.notna()]
    var = var[var.notna()]
    assert var.notna().all(), "ACT_TIME contains values that are not numeric."
    df.loc[:,'ACT_TIME'] = var
    return df
  except Exception as e:
    print(f"Error validating column act_time: {e}")
    return None

def validate_vehicle_id(df):
  if df is None:
    return None
  try:
    var = pd.to_numeric(df['VEHICLE_ID'], errors='coerce')
    df = df[var.notna()]
    var = var[var.notna()]
    assert var.notna().all(), "VEHICLE_ID contains values that are not numeric."
    df.loc[:,'VEHICLE_ID'] = var
    return df
  except Exception as e:
    print(f"Error validating column vehicle_id: {e}")
    return None


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
    required_columns = ['OPD_DATE', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'ACT_TIME','METERS']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
      print(f"Missing required columns: {', '.join(missing_columns)}")
      return None
    df = df.copy()
    important = ['OPD_DATE', 'GPS_LATITUDE', 'GPS_LONGITUDE','EVENT_NO_TRIP']
    df = df.dropna(subset=important)

    if df.empty:
      print("No valid data to transform")
      return None

    df['ACT_TIME'] = pd.to_timedelta(df['ACT_TIME'].astype(float), unit ='seconds')

    df['TIMESTAMP'] = df.apply(lambda y: y['OPD_DATE'] + y['ACT_TIME'], axis=1)

    df = df.drop(['OPD_DATE', 'ACT_TIME'], axis=1)

    df = df.sort_values(['EVENT_NO_TRIP', 'TIMESTAMP'])

    df['dMETERS'] = df.groupby('EVENT_NO_TRIP')['METERS'].diff()
    df['dTIMESTAMP'] = df.groupby('EVENT_NO_TRIP')['TIMESTAMP'].diff()

    df['SPEED'] = df.apply(lambda x:x['dMETERS'] / x['dTIMESTAMP'].total_seconds() 
    if pd.notnull(x['dTIMESTAMP']) and x['dTIMESTAMP'].total_seconds() > 0.1 else 0, axis=1)

    
    invalid_range_for_speed = (df['SPEED'] < 0) | (df['SPEED'] > 95)
    try:
      assert not invalid_range_for_speed.any(), "Invalid range for speed"
    except AssertionError:
      print(f"{invalid_range_for_speed.sum()} invalid range for speed:converting to 0 ")
      df.loc[invalid_range_for_speed, 'SPEED'] = 0

    try:
      assert df['SPEED'].notna().all(), "SPEED contains null values."
    except AssertionError:
      print("SPEED contains null values. converting to 0")
      df['SPEED'] = df['SPEED'].fillna(0)
    return df
  except Exception as e:
    print(f"Error transforming data: {e}")
    return None


def store_database(df):
  conn = None
  try:
    df_new = df.copy()
    required_columns = ['TIMESTAMP', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'SPEED', 'EVENT_NO_TRIP']
    for col in required_columns:
      if col not in df_new.columns:
        print(f"Column {col} not found in DataFrame")
        return False

      
    df_new = df_new.rename(columns={'TIMESTAMP': 'tstamp', 'GPS_LATITUDE': 'latitude', 'GPS_LONGITUDE': 'longitude', 'SPEED': 'speed', 'EVENT_NO_TRIP': 'trip_id'})
    dataframe_data =  df_new[['tstamp', 'latitude', 'longitude','speed', 'trip_id']]

    csv_data = dataframe_data.to_csv(index=False,header = False, sep ='\t')
    f = io.StringIO(csv_data)
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



def other_process(file):
  try:
    json_data = []
    with open(file, 'r', encoding='utf-8') as f:
      json_data = json.load(f)
      print(f"loaded {len(json_data)} records from {file}")
      if not json_data:
        print(f"No data to process")
        return False
    df = pd.DataFrame(json_data)
    df = df.drop_duplicates()

    df = validate_data(df)
    print("data validation complete")
    if df is None:
      print("No valid data to process")
    else:
      df = transform_data(df)
      print("data transform complete")

    if df is not None and not df.empty:
      save_db = store_database(df)
      if save_db:
        path = os.path.join(OUTPUT_DIR, os.path.basename(file))
        shutil.copy(file, path)
        print(f"copied {file} to {path}")
        return True
      else:
        print("Error storing data in database")
        return False
    else:
      print("No valid data to process")
      return False
  except Exception as e:
    print(f"Error processing data: {e}")
    return False

def main():
  gz = glob.glob(os.path.join(INPUT_DIR, '*.json'))
  if not gz:
    print("No files found")
    return 
  print(f"found {len(gz)} files")
  for file in gz:
    print(f"processing {file}")
    other_process(file)
  print("done")

if __name__ == '__main__':
  main()
