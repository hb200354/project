# Fetches POIs from NSW Transport API and inserts into database
import time
import requests
import geopandas as gpd
from shapely.geometry import Point
from sqlalchemy import create_engine

def fetch_pois():
    # Dummy implementation - replace with actual API fetching
    engine = create_engine("postgresql://postgres:0111@localhost:5432/project")

    # Mock POI data
    poi_data = {
        "poi_name": ["Station A", "Library B"],
        "category": ["Transport", "Education"],
        "lon": [151.0, 151.1],
        "lat": [-33.9, -33.8]
    }

    gdf = gpd.GeoDataFrame(
        poi_data,
        geometry=gpd.points_from_xy(poi_data["lon"], poi_data["lat"]),
        crs="EPSG:4326"
    )
    gdf.to_postgis("points_of_interest", engine, if_exists="replace", index=False)