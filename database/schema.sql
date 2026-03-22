CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

CREATE TABLE IF NOT EXISTS cities (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    state VARCHAR(50) NOT NULL,
    fips_code VARCHAR(10),
    population INTEGER,
    area_sqmi FLOAT,
    geometry GEOMETRY(MULTIPOLYGON, 4326),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS road_network (
    id BIGSERIAL PRIMARY KEY,
    city_id INTEGER REFERENCES cities(id),
    osm_id BIGINT,
    name VARCHAR(200),
    highway_type VARCHAR(50),
    length_m FLOAT,
    lanes INTEGER,
    maxspeed INTEGER,
    geometry GEOMETRY(LINESTRING, 4326),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transit_stops (
    id SERIAL PRIMARY KEY,
    city_id INTEGER REFERENCES cities(id),
    stop_id VARCHAR(50),
    stop_name VARCHAR(200),
    stop_lat FLOAT,
    stop_lon FLOAT,
    wheelchair_boarding INTEGER,
    geometry GEOMETRY(POINT, 4326),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transit_routes (
    id SERIAL PRIMARY KEY,
    city_id INTEGER REFERENCES cities(id),
    route_id VARCHAR(50),
    route_name VARCHAR(200),
    route_type INTEGER,
    frequency_peak INTEGER,
    frequency_offpeak INTEGER,
    geometry GEOMETRY(MULTILINESTRING, 4326),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS census_tracts (
    id SERIAL PRIMARY KEY,
    city_id INTEGER REFERENCES cities(id),
    geoid VARCHAR(20),
    tract_name VARCHAR(100),
    population INTEGER,
    median_income FLOAT,
    pct_no_vehicle FLOAT,
    pct_transit_commute FLOAT,
    pct_walk_commute FLOAT,
    geometry GEOMETRY(MULTIPOLYGON, 4326),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS walkability_scores (
    id SERIAL PRIMARY KEY,
    city_id INTEGER REFERENCES cities(id),
    census_tract_id INTEGER REFERENCES census_tracts(id),
    score FLOAT,
    poi_density_score FLOAT,
    street_connectivity_score FLOAT,
    transit_access_score FLOAT,
    land_use_mix_score FLOAT,
    computed_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transit_deserts (
    id SERIAL PRIMARY KEY,
    city_id INTEGER REFERENCES cities(id),
    cluster_id INTEGER,
    population_affected INTEGER,
    area_sqmi FLOAT,
    nearest_stop_distance_m FLOAT,
    severity VARCHAR(20),
    summary TEXT,
    geometry GEOMETRY(POLYGON, 4326),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS traffic_readings (
    id BIGSERIAL PRIMARY KEY,
    city_id INTEGER REFERENCES cities(id),
    segment_id VARCHAR(50),
    speed_mph FLOAT,
    free_flow_speed FLOAT,
    congestion_level VARCHAR(20),
    recorded_at TIMESTAMP,
    geometry GEOMETRY(LINESTRING, 4326)
);

CREATE TABLE IF NOT EXISTS traffic_predictions (
    id BIGSERIAL PRIMARY KEY,
    city_id INTEGER REFERENCES cities(id),
    segment_id VARCHAR(50),
    predicted_speed_mph FLOAT,
    confidence FLOAT,
    model_version VARCHAR(20),
    predicted_for TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS satellite_analyses (
    id SERIAL PRIMARY KEY,
    city_id INTEGER REFERENCES cities(id),
    analysis_type VARCHAR(50),
    acquisition_date DATE,
    ndvi_mean FLOAT,
    urban_heat_index FLOAT,
    impervious_surface_pct FLOAT,
    green_space_pct FLOAT,
    geometry GEOMETRY(POLYGON, 4326),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_road_network_geom ON road_network USING GIST(geometry);
CREATE INDEX IF NOT EXISTS idx_transit_stops_geom ON transit_stops USING GIST(geometry);
CREATE INDEX IF NOT EXISTS idx_transit_routes_geom ON transit_routes USING GIST(geometry);
CREATE INDEX IF NOT EXISTS idx_census_tracts_geom ON census_tracts USING GIST(geometry);
CREATE INDEX IF NOT EXISTS idx_transit_deserts_geom ON transit_deserts USING GIST(geometry);
CREATE INDEX IF NOT EXISTS idx_traffic_readings_geom ON traffic_readings USING GIST(geometry);
CREATE INDEX IF NOT EXISTS idx_satellite_analyses_geom ON satellite_analyses USING GIST(geometry);
CREATE INDEX IF NOT EXISTS idx_traffic_readings_time ON traffic_readings(recorded_at);
CREATE INDEX IF NOT EXISTS idx_traffic_predictions_time ON traffic_predictions(predicted_for);

INSERT INTO cities (name, state, fips_code, population, area_sqmi)
VALUES ('Madison', 'WI', '5548000', 269840, 100.6)
ON CONFLICT DO NOTHING;