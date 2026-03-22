import geopandas as gpd
import pandas as pd
from sqlalchemy import text
import sys
sys.path.append('C:/Projects/meridian')
from database.db import get_engine

CITY_ID = 1

def compute_poi_density(engine, tract_geoid, tract_geom_wkt):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM road_network
            WHERE city_id = :city_id
            AND ST_Within(
                ST_Centroid(geometry),
                ST_GeomFromText(:geom, 4326)
            )
        """), {"city_id": CITY_ID, "geom": tract_geom_wkt})
        road_count = result.fetchone()[0]

        result2 = conn.execute(text("""
            SELECT COUNT(*) FROM transit_stops
            WHERE city_id = :city_id
            AND ST_Within(
                geometry,
                ST_Buffer(ST_GeomFromText(:geom, 4326)::geography, 800)::geometry
            )
        """), {"city_id": CITY_ID, "geom": tract_geom_wkt})
        stop_count = result2.fetchone()[0]

    poi_score = min(100, (road_count / 50) * 40 + (stop_count / 5) * 60)
    return round(poi_score, 2)

def compute_transit_access(engine, tract_geom_wkt):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM transit_stops
            WHERE city_id = :city_id
            AND ST_DWithin(
                geometry::geography,
                ST_Centroid(ST_GeomFromText(:geom, 4326))::geography,
                800
            )
        """), {"city_id": CITY_ID, "geom": tract_geom_wkt})
        nearby_stops = result.fetchone()[0]

    transit_score = min(100, nearby_stops * 12)
    return round(transit_score, 2)

def compute_street_connectivity(engine, tract_geom_wkt):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM road_network
            WHERE city_id = :city_id
            AND ST_Intersects(geometry, ST_GeomFromText(:geom, 4326))
        """), {"city_id": CITY_ID, "geom": tract_geom_wkt})
        road_count = result.fetchone()[0]

    connectivity_score = min(100, (road_count / 30) * 100)
    return round(connectivity_score, 2)

def run_walkability_engine():
    engine = get_engine()
    print("Loading census tracts...")

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, geoid, population, ST_AsText(geometry) as geom_wkt
            FROM census_tracts
            WHERE city_id = :city_id
        """), {"city_id": CITY_ID})
        tracts = result.fetchall()

    print(f"Computing walkability scores for {len(tracts)} tracts...")

    with engine.connect() as conn:
        conn.execute(text("DELETE FROM walkability_scores WHERE city_id = :city_id"), {"city_id": CITY_ID})
        conn.commit()

    scores = []
    for i, tract in enumerate(tracts):
        tract_id, geoid, population, geom_wkt = tract

        try:
            poi_score = compute_poi_density(engine, geoid, geom_wkt)
            transit_score = compute_transit_access(engine, geom_wkt)
            connectivity_score = compute_street_connectivity(engine, geom_wkt)
            land_use_score = min(100, (poi_score * 0.5 + transit_score * 0.5))

            total_score = (
                poi_score * 0.30 +
                connectivity_score * 0.25 +
                transit_score * 0.25 +
                land_use_score * 0.20
            )
            total_score = round(total_score, 2)

            scores.append({
                'tract_id': tract_id,
                'geoid': geoid,
                'score': total_score,
                'poi': poi_score,
                'transit': transit_score,
                'connectivity': connectivity_score,
                'land_use': land_use_score
            })

            if (i + 1) % 10 == 0:
                print(f"  Processed {i+1}/{len(tracts)} tracts...")

        except Exception as e:
            print(f"  Error on tract {geoid}: {e}")
            continue

    with engine.connect() as conn:
        for s in scores:
            conn.execute(text("""
                INSERT INTO walkability_scores
                (city_id, census_tract_id, score, poi_density_score,
                 street_connectivity_score, transit_access_score, land_use_mix_score)
                VALUES (:city_id, :tract_id, :score, :poi,
                        :connectivity, :transit, :land_use)
            """), {
                'city_id': CITY_ID,
                'tract_id': s['tract_id'],
                'score': s['score'],
                'poi': s['poi'],
                'transit': s['transit'],
                'connectivity': s['connectivity'],
                'land_use': s['land_use']
            })
        conn.commit()

    print(f"\nWalkability engine complete!")
    print(f"Scored {len(scores)} tracts")

    if scores:
        avg = sum(s['score'] for s in scores) / len(scores)
        top5 = sorted(scores, key=lambda x: x['score'], reverse=True)[:5]
        bottom5 = sorted(scores, key=lambda x: x['score'])[:5]

        print(f"\nMadison Average Walkability Score: {avg:.1f}/100")
        print(f"\nTop 5 most walkable tracts:")
        for s in top5:
            print(f"  Tract {s['geoid']}: {s['score']}")
        print(f"\nBottom 5 least walkable tracts:")
        for s in bottom5:
            print(f"  Tract {s['geoid']}: {s['score']}")

if __name__ == "__main__":
    run_walkability_engine()