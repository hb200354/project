# Loads all input data and inserts into PostgreSQL
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine

def load_all_data():
    engine = create_engine("postgresql://postgres:0111@localhost:5432/project")
    folder_path = "/Users/S/Desktop/DATA2001/project/data/"

    # 1. Load and insert population data
    pop = pd.read_csv(folder_path + "Population.csv")
    pop.to_sql("population", engine, if_exists="replace", index=False)

    # 2. Load and insert business data
    biz = pd.read_csv(folder_path + "Businesses.csv")
    biz.to_sql("businesses", engine, if_exists="replace", index=False)

    # 3. Load and insert income data
    inc = pd.read_csv(folder_path + "Income.csv")
    inc.to_sql("income", engine, if_exists="replace", index=False)

    # 4. Load and insert stops
    stops = pd.read_csv(folder_path + "Stops.txt")
    gdf_stops = gpd.GeoDataFrame(
        stops,
        geometry=gpd.points_from_xy(stops["stop_lon"], stops["stop_lat"]),
        crs="EPSG:4326"
    )
    gdf_stops.to_postgis("stops", engine, if_exists="replace", index=False)

    # 5. Load and insert school catchments
    p = gpd.read_file(folder_path + "catchments/catchments/catchments_primary.shp")
    s = gpd.read_file(folder_path + "catchments/catchments/catchments_secondary.shp")
    schools = pd.concat([p, s])
    schools["school_type"] = ["primary"] * len(p) + ["secondary"] * len(s)
    schools = schools.to_crs(4326)
    schools.to_postgis("school_catchments", engine, if_exists="replace", index=False)