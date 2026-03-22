import requests
import pandas as pd
import zipfile
import io
from sqlalchemy import text
import sys
sys.path.append('C:/Projects/meridian')
from database.db import get_engine

CITY_ID = 1
GTFS_URL = "https://transitfeeds.com/p/madison-metro-transit/1061/latest/download"
GTFS_BACKUP_URL = "https://transitdata.cityofmadison.com/GTFS/mmt_gtfs.zip"

def download_gtfs():
    print("Downloading Madison Metro Transit GTFS feed...")
    for url in [GTFS_BACKUP_URL, GTFS_URL]:
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                print(f"Downloaded GTFS from {url}")
                return zipfile.ZipFile(io.BytesIO(response.content))
        except Exception as e:
            print(f"Failed {url}: {e}")
    return None

def ingest_stops(z):
    print("Ingesting transit stops...")
    with z.open('stops.txt') as f:
        stops = pd.read_csv(f)

    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM transit_stops WHERE city_id = :city_id"), {"city_id": CITY_ID})
        conn.commit()

    with engine.connect() as conn:
        for _, row in stops.iterrows():
            try:
                conn.execute(text("""
                    INSERT INTO transit_stops
                    (city_id, stop_id, stop_name, stop_lat, stop_lon, wheelchair_boarding, geometry)
                    VALUES (:city_id, :stop_id, :stop_name, :stop_lat, :stop_lon, :wheelchair_boarding,
                            ST_SetSRID(ST_MakePoint(:lon, :lat), 4326))
                """), {
                    'city_id': CITY_ID,
                    'stop_id': str(row['stop_id']),
                    'stop_name': str(row.get('stop_name', ''))[:200],
                    'stop_lat': float(row['stop_lat']),
                    'stop_lon': float(row['stop_lon']),
                    'wheelchair_boarding': int(row['wheelchair_boarding']) if pd.notna(row.get('wheelchair_boarding')) else 0,
                    'lat': float(row['stop_lat']),
                    'lon': float(row['stop_lon'])
                })
            except Exception as e:
                continue
        conn.commit()

    print(f"Inserted {len(stops)} transit stops")
    return stops

def ingest_routes(z):
    print("Ingesting transit routes...")
    with z.open('routes.txt') as f:
        routes = pd.read_csv(f)

    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM transit_routes WHERE city_id = :city_id"), {"city_id": CITY_ID})
        conn.commit()

    with engine.connect() as conn:
        for _, row in routes.iterrows():
            try:
                conn.execute(text("""
                    INSERT INTO transit_routes
                    (city_id, route_id, route_name, route_type)
                    VALUES (:city_id, :route_id, :route_name, :route_type)
                """), {
                    'city_id': CITY_ID,
                    'route_id': str(row['route_id']),
                    'route_name': str(row.get('route_long_name', row.get('route_short_name', '')))[:200],
                    'route_type': int(row.get('route_type', 3))
                })
            except Exception as e:
                continue
        conn.commit()

    print(f"Inserted {len(routes)} transit routes")

if __name__ == "__main__":
    z = download_gtfs()
    if z:
        ingest_stops(z)
        ingest_routes(z)
        print("GTFS ingestion complete for Madison Metro Transit!")
    else:
        print("Could not download GTFS feed.")