import requests
import geopandas as gpd
import pandas as pd
from sqlalchemy import text
from shapely.geometry import MultiPolygon
import sys
import os
sys.path.append('C:/Projects/meridian')
from database.db import get_engine
from dotenv import load_dotenv
load_dotenv('C:/Projects/meridian/.env')

CENSUS_KEY = os.getenv('CENSUS_API_KEY')
STATE_FIPS = '55'
COUNTY_FIPS = '025'
CITY_ID = 1

def fetch_census_data():
    print("Fetching Census ACS5 data for Madison (Dane County, WI)...")
    url = "https://api.census.gov/data/2022/acs/acs5"
    params = {
        'get': 'NAME,B01003_001E,B19013_001E,B08201_002E,B08301_010E,B08301_019E',
        'for': f'tract:*',
        'in': f'state:{STATE_FIPS} county:{COUNTY_FIPS}',
        'key': CENSUS_KEY
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"Census API error: {response.status_code}")
        return None
    data = response.json()
    headers = data[0]
    rows = data[1:]
    df = pd.DataFrame(rows, columns=headers)
    df.columns = ['name','population','median_income','no_vehicle_hh',
                  'transit_commute','walk_commute','state','county','tract']
    for col in ['population','median_income','no_vehicle_hh','transit_commute','walk_commute']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['geoid'] = df['state'] + df['county'] + df['tract']
    print(f"Fetched {len(df)} census tracts")
    return df

def fetch_tract_geometries():
    print("Fetching tract geometries...")
    url = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/Tracts_Blocks/MapServer/0/query"
    params = {
        'where': f"STATE='{STATE_FIPS}' AND COUNTY='{COUNTY_FIPS}'",
        'outFields': 'GEOID,NAME',
        'returnGeometry': 'true',
        'outSR': '4326',
        'f': 'geojson'
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"Geometry API error: {response.status_code}")
        return None
    gdf = gpd.read_file(response.text)
    print(f"Fetched {len(gdf)} tract geometries")
    return gdf

def ingest_census():
    df = fetch_census_data()
    if df is None:
        return
    gdf = fetch_tract_geometries()
    if gdf is None:
        return

    gdf['GEOID'] = gdf['GEOID'].astype(str)
    df['geoid'] = df['geoid'].astype(str)
    merged = gdf.merge(df, left_on='GEOID', right_on='geoid', how='inner')
    print(f"Merged {len(merged)} tracts with geometry")

    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM census_tracts WHERE city_id = :city_id"), {"city_id": CITY_ID})
        conn.commit()

    with engine.connect() as conn:
        for _, row in merged.iterrows():
            pop = int(row['population']) if pd.notna(row['population']) else 0
            income = float(row['median_income']) if pd.notna(row['median_income']) else 0
            no_veh = float(row['no_vehicle_hh']) if pd.notna(row['no_vehicle_hh']) else 0
            transit = float(row['transit_commute']) if pd.notna(row['transit_commute']) else 0
            walk = float(row['walk_commute']) if pd.notna(row['walk_commute']) else 0

            geom = row.geometry
            if geom and geom.geom_type == 'Polygon':
                geom = MultiPolygon([geom])
            geom_wkt = geom.wkt if geom else None
            if geom_wkt is None:
                continue

            conn.execute(text("""
                INSERT INTO census_tracts
                (city_id, geoid, tract_name, population, median_income,
                 pct_no_vehicle, pct_transit_commute, pct_walk_commute, geometry)
                VALUES (:city_id, :geoid, :tract_name, :population, :median_income,
                        :pct_no_vehicle, :pct_transit_commute, :pct_walk_commute,
                        ST_GeomFromText(:geometry, 4326))
            """), {
                'city_id': CITY_ID,
                'geoid': str(row['GEOID']),
                'tract_name': str(row.get('NAME_x', ''))[:100],
                'population': pop,
                'median_income': income,
                'pct_no_vehicle': no_veh,
                'pct_transit_commute': transit,
                'pct_walk_commute': walk,
                'geometry': geom_wkt
            })
        conn.commit()

    print(f"Census ingestion complete — {len(merged)} tracts loaded for Madison!")

if __name__ == "__main__":
    ingest_census()