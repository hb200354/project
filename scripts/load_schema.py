from sqlalchemy import create_engine, text

def create_schema():
    engine = create_engine("postgresql://postgres:0111@localhost:5432/project")
    schema_sql = """
-- 0. Enable PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;
-- 1. SA2 Boundaries
DROP TABLE IF EXISTS sa2_boundaries CASCADE;
CREATE TABLE sa2_boundaries (
  sa2_code21    VARCHAR(20) PRIMARY KEY,
  sa2_name21    VARCHAR(100) NOT NULL,
  gcc_name21    VARCHAR(50),
  sa4_name21    VARCHAR(50),
  geom          geometry(MultiPolygon,4326) NOT NULL
);
CREATE INDEX idx_sa2_geom ON sa2_boundaries USING GIST (geom);
-- 2. Population
DROP TABLE IF EXISTS population;
CREATE TABLE population (
  sa2_name        VARCHAR(100) PRIMARY KEY,
  total_people    INTEGER,
  "0-4_people"    INTEGER,
  "5-9_people"    INTEGER,
  "10-14_people"  INTEGER,
  "15-19_people"  INTEGER,
  young_people    INTEGER
);
-- 3. Businesses
DROP TABLE IF EXISTS businesses;
CREATE TABLE businesses (
  sa2_name          VARCHAR(100) NOT NULL,
  industry_code     VARCHAR(10),
  total_businesses  INTEGER,
  PRIMARY KEY(sa2_name, industry_code)
);
CREATE INDEX idx_businesses_sa2 ON businesses(sa2_name);
-- 4. Stops
DROP TABLE IF EXISTS stops;
CREATE TABLE stops (
  stop_id        VARCHAR(20) PRIMARY KEY,
  stop_name      VARCHAR(200),
  stop_lat       DOUBLE PRECISION,
  stop_lon       DOUBLE PRECISION,
  geom           geometry(Point,4326) NOT NULL
);
CREATE INDEX idx_stops_geom ON stops USING GIST (geom);
-- 5. School Catchments
DROP TABLE IF EXISTS school_catchments;
CREATE TABLE school_catchments (
  catchment_id   SERIAL PRIMARY KEY,
  school_type    VARCHAR(20),
  geom           geometry(Polygon,4326) NOT NULL
);
CREATE INDEX idx_schools_geom ON school_catchments USING GIST (geom);
-- 6. Income
DROP TABLE IF EXISTS income;
CREATE TABLE income (
  sa2_name       VARCHAR(100) PRIMARY KEY,
  median_income  NUMERIC
);
-- 7. Points of Interest
DROP TABLE IF EXISTS points_of_interest;
CREATE TABLE points_of_interest (
  poi_id      SERIAL PRIMARY KEY,
  sa2_name    VARCHAR(100),
  poi_name    VARCHAR(200),
  category    VARCHAR(50),
  geom        geometry(Point,4326) NOT NULL
);
CREATE INDEX idx_poi_geom ON points_of_interest USING GIST (geom);
"""
    with engine.connect() as conn:
        for stmt in schema_sql.strip().split(";"):
            if stmt.strip():
                conn.execute(text(stmt.strip() + ";"))
        conn.commit()
    print("✅ PostgreSQL schema created.")
