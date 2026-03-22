import pandas as pd
from sqlalchemy import text
import sys
sys.path.append('C:/Projects/meridian')
from database.db import get_engine

CITY_ID = 1
DESERT_THRESHOLD_M = 800

def detect_transit_deserts():
    engine = get_engine()
    print("Detecting transit deserts in Madison, WI...")

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                ct.id,
                ct.geoid,
                ct.population,
                ct.median_income,
                ST_AsText(ST_Centroid(ct.geometry)) as centroid_wkt,
                ST_AsText(ct.geometry) as geom_wkt,
                MIN(ST_Distance(
                    ST_Centroid(ct.geometry)::geography,
                    ts.geometry::geography
                )) as nearest_stop_m
            FROM census_tracts ct
            CROSS JOIN LATERAL (
                SELECT geometry
                FROM transit_stops
                WHERE city_id = :city_id
                ORDER BY ST_Distance(
                    ST_Centroid(ct.geometry)::geography,
                    geometry::geography
                )
                LIMIT 1
            ) ts
            WHERE ct.city_id = :city_id
            GROUP BY ct.id, ct.geoid, ct.population, ct.median_income,
                     ct.geometry
        """), {"city_id": CITY_ID})
        tracts = result.fetchall()

    print(f"Analyzed {len(tracts)} census tracts")

    deserts = []
    for tract in tracts:
        tid, geoid, population, income, centroid_wkt, geom_wkt, nearest_stop_m = tract
        if nearest_stop_m and nearest_stop_m > DESERT_THRESHOLD_M:
            if nearest_stop_m > 1600:
                severity = 'severe'
            elif nearest_stop_m > 1200:
                severity = 'moderate'
            else:
                severity = 'mild'

            deserts.append({
                'tract_id': tid,
                'geoid': geoid,
                'population': population or 0,
                'income': income or 0,
                'nearest_stop_m': nearest_stop_m,
                'severity': severity,
                'geom_wkt': geom_wkt
            })

    print(f"Found {len(deserts)} transit desert tracts")

    with engine.connect() as conn:
        conn.execute(text("DELETE FROM transit_deserts WHERE city_id = :city_id"), {"city_id": CITY_ID})
        conn.commit()

    with engine.connect() as conn:
        for i, d in enumerate(deserts):
            summary = (
                f"Census tract {d['geoid']} has no transit stop within "
                f"{int(d['nearest_stop_m'])}m. "
                f"Population affected: {d['population']:,}. "
                f"Severity: {d['severity']}."
            )
            conn.execute(text("""
                INSERT INTO transit_deserts
                (city_id, cluster_id, population_affected, nearest_stop_distance_m,
                 severity, summary, geometry)
                VALUES (:city_id, :cluster_id, :population, :nearest_stop_m,
                        :severity, :summary,
                        ST_GeometryN(ST_GeomFromText(:geom, 4326), 1))
            """), {
                'city_id': CITY_ID,
                'cluster_id': i + 1,
                'population': d['population'],
                'nearest_stop_m': d['nearest_stop_m'],
                'severity': d['severity'],
                'summary': summary,
                'geom': d['geom_wkt']
            })
        conn.commit()

    total_affected = sum(d['population'] for d in deserts)
    severe = [d for d in deserts if d['severity'] == 'severe']

    print(f"\n--- MADISON TRANSIT DESERT REPORT ---")
    print(f"Total desert tracts: {len(deserts)}")
    print(f"Total population in deserts: {total_affected:,}")
    print(f"Severe deserts (>1600m from transit): {len(severe)}")

    if deserts:
        worst = max(deserts, key=lambda x: x['nearest_stop_m'])
        print(f"\nWorst desert:")
        print(f"  Tract: {worst['geoid']}")
        print(f"  Distance to nearest stop: {int(worst['nearest_stop_m'])}m")
        print(f"  Population affected: {worst['population']:,}")
        print(f"\nCold email hook:")
        print(f"  '{total_affected:,} Madison residents live more than")
        print(f"   800 meters from any transit stop. {len(severe)} neighborhoods")
        print(f"   qualify as severe transit deserts.'")

if __name__ == "__main__":
    detect_transit_deserts()