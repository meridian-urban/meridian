import requests
import os
from sqlalchemy import text
from datetime import datetime
import sys
sys.path.append('C:/Projects/meridian')
from database.db import get_engine
from dotenv import load_dotenv
load_dotenv('C:/Projects/meridian/.env')

HERE_API_KEY = os.getenv('HERE_API_KEY')
CITY_ID = 1

MADISON_CORRIDORS = [
    {"id": "east-wash-1", "name": "East Washington Ave", "bbox": "-89.3793,43.0731,-89.3501,43.0765"},
    {"id": "university-1", "name": "University Ave", "bbox": "-89.4195,43.0731,-89.3987,43.0752"},
    {"id": "park-1", "name": "Park Street", "bbox": "-89.3960,43.0525,-89.3920,43.0731"},
    {"id": "john-nolen-1", "name": "John Nolen Drive", "bbox": "-89.3793,43.0589,-89.3697,43.0672"},
    {"id": "washington-1", "name": "W Washington Ave", "bbox": "-89.4100,43.0650,-89.3900,43.0720"},
]

def fetch_and_store_corridor(corridor):
    url = "https://data.traffic.hereapi.com/v7/flow"
    params = {
        'in': f"bbox:{corridor['bbox']}",
        'locationReferencing': 'shape',
        'apiKey': HERE_API_KEY
    }
    try:
        response = requests.get(url, params=params, timeout=15)
        if response.status_code != 200:
            print(f"  API error {response.status_code} for {corridor['name']}")
            return 0
        data = response.json()
    except Exception as e:
        print(f"  Request failed: {e}")
        return 0

    results = data.get('results', [])
    if not results:
        print(f"  No results for {corridor['name']}")
        return 0

    engine = get_engine()
    count = 0
    with engine.connect() as conn:
        for result in results:
            try:
                current = result.get('currentFlow', {})
                speed_kmh = current.get('speed', 0)
                freeflow_kmh = current.get('freeFlow', speed_kmh)

                if speed_kmh <= 0:
                    continue

                speed_mph = float(speed_kmh) * 0.621371
                freeflow_mph = float(freeflow_kmh) * 0.621371

                ratio = speed_kmh / freeflow_kmh if freeflow_kmh > 0 else 1
                if ratio < 0.5:
                    congestion = 'heavy'
                elif ratio < 0.75:
                    congestion = 'moderate'
                elif ratio < 0.9:
                    congestion = 'light'
                else:
                    congestion = 'free'

                loc = result.get('location', {})
                shape = loc.get('shape', {})
                links = shape.get('links', [])

                all_points = []
                for link in links:
                    pts = link.get('points', [])
                    all_points.extend(pts)

                if len(all_points) < 2:
                    continue

                coords = ', '.join([f"{p['lng']} {p['lat']}" for p in all_points])
                geom_wkt = f"LINESTRING({coords})"

                conn.execute(text("""
                    INSERT INTO traffic_readings
                    (city_id, segment_id, speed_mph, free_flow_speed, congestion_level, recorded_at, geometry)
                    VALUES (:city_id, :segment_id, :speed, :free_flow, :congestion, :recorded_at,
                            ST_GeomFromText(:geometry, 4326))
                """), {
                    'city_id': CITY_ID,
                    'segment_id': corridor['id'],
                    'speed': speed_mph,
                    'free_flow': freeflow_mph,
                    'congestion': congestion,
                    'recorded_at': datetime.now(),
                    'geometry': geom_wkt
                })
                count += 1
            except Exception as e:
                print(f"    Row error: {e}")
                continue
        conn.commit()
    return count

if __name__ == "__main__":
    print(f"Fetching live traffic for {len(MADISON_CORRIDORS)} Madison corridors...")
    total = 0
    for corridor in MADISON_CORRIDORS:
        print(f"  Fetching {corridor['name']}...")
        count = fetch_and_store_corridor(corridor)
        print(f"  Stored {count} readings for {corridor['name']}")
        total += count
    print(f"Traffic ingestion complete — {total} total readings stored")