import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import shape
from sqlalchemy import create_engine

# Connect to PostgreSQL/PostGIS
engine = create_engine("postgresql://postgres:0111@localhost:5432/project")

# Load full SA2 shapefile
sa2_path = "data\SA2_2021_AUST_SHP_GDA2020\SA2_2021_AUST_GDA2020.shp"
sa2_gdf = gpd.read_file(sa2_path)

# Filter to SA2s within Sydney - Inner South West
inner_south_west_sa2 = sa2_gdf[sa2_gdf["SA4_NAME21"] == "Sydney - Inner South West"]

# Save to PostGIS
inner_south_west_sa2.to_postgis("sa2_boundaries", engine, if_exists="replace", index=False)
print("✅ SA2 boundaries saved to PostGIS.")

# Compute bounding box (xmin, ymin, xmax, ymax) for API call
xmin, ymin, xmax, ymax = inner_south_west_sa2.total_bounds
bbox_str = f"{xmin},{ymin},{xmax},{ymax}"
print(f"Bounding box: {bbox_str}")

# Query NSW ArcGIS API for POI data within bounding box
url = "https://maps.six.nsw.gov.au/arcgis/rest/services/public/NSW_POI/MapServer/0/query"
params = {
    "f": "geojson",
    "geometryType": "esriGeometryEnvelope",
    "geometry": bbox_str,
    "inSR": "4326",
    "spatialRel": "esriSpatialRelIntersects",
    "outFields": "*",
    "returnGeometry": "true"
}
response = requests.get(url, params=params)
data = response.json()

# Convert GeoJSON to GeoDataFrame
features = data["features"]
gdf_poi = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")

# Save POI data to PostGIS
gdf_poi.to_postgis("points_of_interest", engine, if_exists="replace", index=False)
print("✅ POI data was successfully stored in PostGIS.")
