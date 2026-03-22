import osmnx as ox
import geopandas as gpd
from sqlalchemy import text
import sys
sys.path.append('C:/Projects/meridian')
from database.db import get_engine

CITY = "Madison, Wisconsin, USA"
CITY_ID = 1

def ingest_road_network():
    print("Downloading Madison road network from OpenStreetMap...")
    G = ox.graph_from_place(CITY, network_type='drive')
    nodes, edges = ox.graph_to_gdfs(G)
    edges = edges.reset_index()
    print(f"Downloaded {len(edges)} road segments")

    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM road_network WHERE city_id = :city_id"), {"city_id": CITY_ID})
        conn.commit()

    road_data = []
    for _, row in edges.iterrows():
        osm_id = row.get('osmid', 0)
        if isinstance(osm_id, list):
            osm_id = osm_id[0]
        osm_id = int(osm_id)

        highway = row.get('highway', '')
        if isinstance(highway, list):
            highway = highway[0]
        highway = str(highway)[:50]

        lanes_raw = row.get('lanes', None)
        if lanes_raw is None or (isinstance(lanes_raw, float) and str(lanes_raw) == 'nan'):
            lanes = 1
        elif isinstance(lanes_raw, list):
            lanes = 1
        else:
            try:
                lanes = int(float(lanes_raw))
            except:
                lanes = 1

        road_data.append({
            'city_id': CITY_ID,
            'osm_id': osm_id,
            'name': str(row.get('name', ''))[:200] if row.get('name') and str(row.get('name')) != 'nan' else None,
            'highway_type': highway,
            'length_m': float(row.get('length', 0)),
            'lanes': lanes,
            'maxspeed': None,
            'geometry': row.geometry.wkt
        })

    engine = get_engine()
    with engine.connect() as conn:
        for chunk_start in range(0, len(road_data), 500):
            chunk = road_data[chunk_start:chunk_start+500]
            for road in chunk:
                conn.execute(text("""
                    INSERT INTO road_network (city_id, osm_id, name, highway_type, length_m, lanes, maxspeed, geometry)
                    VALUES (:city_id, :osm_id, :name, :highway_type, :length_m, :lanes, :maxspeed, ST_GeomFromText(:geometry, 4326))
                """), road)
            conn.commit()
            print(f"  Inserted {min(chunk_start+500, len(road_data))}/{len(road_data)} road segments")

    print("Road network ingestion complete!")

def ingest_pois():
    print("Downloading Points of Interest...")
    tags = {'amenity': True, 'shop': True, 'leisure': True}
    try:
        pois = ox.features_from_place(CITY, tags=tags)
        print(f"Downloaded {len(pois)} POIs")
        return pois
    except Exception as e:
        print(f"POI download failed: {e}")
        return None

if __name__ == "__main__":
    ingest_road_network()
    ingest_pois()
    print("OSM ingestion complete for Madison, WI")