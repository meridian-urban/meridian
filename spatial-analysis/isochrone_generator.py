import osmnx as ox
import networkx as nx
import geopandas as gpd
from shapely.geometry import MultiPolygon, Polygon
from sqlalchemy import text
import pandas as pd
import sys
sys.path.append('C:/Projects/meridian')
from database.db import get_engine

CITY_ID = 1
CITY = "Madison, Wisconsin, USA"
WALK_SPEEDS = {5: 400, 10: 800, 15: 1200}  # minutes: meters

def get_walk_graph():
    print("Loading Madison walking network...")
    G = ox.graph_from_place(CITY, network_type='walk')
    G = ox.add_edge_speeds(G)
    G = ox.add_edge_travel_times(G)
    print(f"Walk network loaded: {len(G.nodes)} nodes")
    return G

def get_transit_stops(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT stop_id, stop_name, stop_lat, stop_lon
            FROM transit_stops
            WHERE city_id = :city_id
            LIMIT 50
        """), {"city_id": CITY_ID})
        return result.fetchall()

def generate_isochrone(G, lat, lon, max_dist_m):
    try:
        center_node = ox.nearest_nodes(G, lon, lat)
        subgraph = nx.ego_graph(G, center_node, radius=max_dist_m, distance='length')
        node_points = [
            (G.nodes[n]['x'], G.nodes[n]['y'])
            for n in subgraph.nodes
        ]
        if len(node_points) < 3:
            return None
        from shapely.geometry import MultiPoint
        mp = MultiPoint(node_points)
        return mp.convex_hull
    except Exception as e:
        return None

def calculate_population_coverage(engine, isochrone_geom):
    if isochrone_geom is None:
        return 0
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COALESCE(SUM(population), 0)
                FROM census_tracts
                WHERE city_id = :city_id
                AND ST_Intersects(
                    geometry,
                    ST_GeomFromText(:geom, 4326)
                )
            """), {"city_id": CITY_ID, "geom": isochrone_geom.wkt})
            return result.fetchone()[0] or 0
    except:
        return 0

def run_isochrone_analysis():
    engine = get_engine()
    print("Starting isochrone analysis for Madison transit stops...")

    G = get_walk_graph()
    stops = get_transit_stops(engine)
    print(f"Analyzing {len(stops)} transit stops...")

    results = {5: [], 10: [], 15: []}

    for i, stop in enumerate(stops):
        stop_id, stop_name, lat, lon = stop
        if (i + 1) % 10 == 0:
            print(f"  Processed {i+1}/{len(stops)} stops...")

        for minutes, meters in WALK_SPEEDS.items():
            iso = generate_isochrone(G, lat, lon, meters)
            if iso:
                pop = calculate_population_coverage(engine, iso)
                results[minutes].append({
                    'stop_id': stop_id,
                    'stop_name': stop_name,
                    'minutes': minutes,
                    'population_covered': pop,
                    'geom': iso
                })

    engine2 = get_engine()
    with engine2.connect() as conn:
        total_pop_result = conn.execute(text(
            "SELECT SUM(population) FROM census_tracts WHERE city_id = :city_id"
        ), {"city_id": CITY_ID})
        total_pop = total_pop_result.fetchone()[0] or 1

    print("\n--- MADISON ISOCHRONE COVERAGE REPORT ---")
    for minutes in [5, 10, 15]:
        if results[minutes]:
            all_covered = sum(r['population_covered'] for r in results[minutes])
            avg_covered = all_covered / len(results[minutes])
            pct = round((avg_covered / total_pop) * 100, 1)
            print(f"{minutes}-min walk coverage: {pct}% of Madison population")

    print("\nTop 5 stops by 15-min coverage:")
    if results[15]:
        top5 = sorted(results[15], key=lambda x: x['population_covered'], reverse=True)[:5]
        for r in top5:
            print(f"  {r['stop_name']}: {r['population_covered']:,} residents within 15-min walk")

    print("\nCold email insight:")
    if results[15]:
        avg_15 = sum(r['population_covered'] for r in results[15]) / len(results[15])
        pct_15 = round((avg_15 / total_pop) * 100, 1)
        print(f"  'Only {pct_15}% of Madison residents can reach a transit stop")
        print(f"   within a 15-minute walk from the average stop catchment area.'")

if __name__ == "__main__":
    run_isochrone_analysis()