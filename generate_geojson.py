import psycopg2
import json
import sys
import os

# Usage check
if len(sys.argv) != 2:
    print("Usage: python generate_geojson.py <trip_id>")
    sys.exit(1)

trip_id = sys.argv[1]

# Connect to postgres sql
conn = psycopg2.connect(
    dbname="postgres",
    user="hidden",
    password="hidden",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Query latitude, longitude, and speed
cur.execute("""
    SELECT latitude, longitude, speed
    FROM BreadCrumb
    WHERE trip_id = %s
""", (trip_id,))

data = cur.fetchall()

# Convert to GeoJSON
geojson = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat]
            },
            "properties": {
                "speed": speed
            }
        } for lat, lon, speed in data
    ]
}

# Ensure viz_data directory exists
os.makedirs("viz_data", exist_ok=True)

# Define file paths
custom_filename = f"viz_data/trip_{trip_id}.geojson"
default_filename = "viz_data/output.geojson"

# Write both files
with open(custom_filename, "w") as f:
    json.dump(geojson, f, indent=2)

with open(default_filename, "w") as f:
    json.dump(geojson, f, indent=2)

print(f"GeoJSON saved to {custom_filename} and {default_filename}")

# Cleanup
cur.close()
conn.close()

