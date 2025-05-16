import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from sqlalchemy import create_engine, text
import time
import requests
from sklearn.preprocessing import MinMaxScaler
import seaborn as sns
import numpy as np
from IPython.display import display
import os


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

print("✅ All tables and indexes created successfully.")



def sigmoid(x):
    return 1 / (1 + np.exp(-x))

def analyze_sa4(sa4_name):
    # 1. Load SA2 shapefile and filter to the selected SA4
    sa2 = gpd.read_file("../data/SA2_2021_AUST_SHP_GDA2020/SA2_2021_AUST_GDA2020.shp")
    sa2_gsyd = sa2[sa2['GCC_NAME21'] == 'Greater Sydney']
    sa2_filtered = sa2_gsyd[sa2_gsyd['SA4_NAME21'] == sa4_name].copy()
    # 1-1. data cleaning
    sa2_filtered = sa2_filtered[sa2_filtered.geometry.is_valid]
    # 2. Population
    population = pd.read_csv("../data/Population.csv")
    population = population[population['sa2_name'].isin(sa2_filtered['SA2_NAME21'])]
    # 2-1. data cleaning
    population = population.drop_duplicates(subset=['sa2_name'])
    population = population[population['total_people'] >= 100]
    population = population[population['sa2_name'].isin(sa2_filtered['SA2_NAME21'])]
    # 2-3. Compute young people population (ages 0–19)
    young_cols = ['0-4_people', '5-9_people', '10-14_people', '15-19_people']
    population['young_people'] = population[young_cols].sum(axis=1)
    # 2-4. data cleaning
    population.loc[population['young_people'] > population['total_people'], 'young_people'] = \
        population.loc[population['young_people'] > population['total_people'], 'total_people']
    # 3. Businesses
    businesses = pd.read_csv("../data/Businesses.csv")
    biz_summary = businesses.groupby('sa2_name')['total_businesses'].sum().reset_index()
    sa2_filtered = sa2_filtered.merge(biz_summary, left_on='SA2_NAME21', right_on='sa2_name', how='left')
    sa2_filtered['total_businesses'] = sa2_filtered['total_businesses'].fillna(0)
    # 4. Stops
    stops = pd.read_csv("../data/Stops.txt")
    stops = stops.dropna(subset=['stop_lat', 'stop_lon'])
    stops['geometry'] = stops.apply(lambda row: Point(row['stop_lon'], row['stop_lat']), axis=1)
    stops_gdf = gpd.GeoDataFrame(stops, geometry='geometry', crs="EPSG:4326").to_crs(sa2_filtered.crs)
    stop_counts = gpd.sjoin(stops_gdf, sa2_filtered, how="inner", predicate="intersects") \
                     .groupby("SA2_NAME21").size().reset_index(name="stop_count")
    sa2_filtered = sa2_filtered.merge(stop_counts, on="SA2_NAME21", how="left")
    sa2_filtered["stop_count"] = sa2_filtered["stop_count"].fillna(0)
    # 5. Schools
    primary = gpd.read_file("../data/catchments/catchments/catchments_primary.shp")
    secondary = gpd.read_file("../data/catchments/catchments/catchments_secondary.shp")
    schools = pd.concat([primary, secondary], ignore_index=True).to_crs(sa2_filtered.crs)
    school_counts = gpd.sjoin(sa2_filtered, schools, how="left", predicate="intersects") \
                       .groupby("SA2_NAME21").size().reset_index(name="school_count")
    sa2_filtered = sa2_filtered.merge(school_counts, on="SA2_NAME21", how="left")
    sa2_filtered["school_count"] = sa2_filtered["school_count"].fillna(0)
    # 6. Income
    income = pd.read_csv("../data/Income.csv")
    income['median_income'] = pd.to_numeric(income['median_income'], errors='coerce')
    income = income[income['sa2_name'].isin(sa2_filtered['SA2_NAME21'])]
    sa2_filtered = sa2_filtered.merge(income[['sa2_name', 'median_income']], 
                                  left_on='SA2_NAME21', 
                                  right_on='sa2_name', 
                                  how='left')
    # 7. Retrieve POIs within each SA2 area using bounding boxes and NSW API
    all_pois = []
    for idx, row in sa2_filtered.iterrows():
        minx, miny, maxx, maxy = row.geometry.bounds
        bbox_str = f"{minx},{miny},{maxx},{maxy}"
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
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            features = data.get("features", [])
            for f in features:
                props = f.get("properties", {})
                coords = f.get("geometry", {}).get("coordinates", [None, None])
                if coords[0] is not None and coords[1] is not None:
                    all_pois.append({
                        "sa2_name": row["SA2_NAME21"],
                        "poi_name": props.get("NAME"),
                        "category": props.get("POI_GROUP"),
                        "longitude": coords[0],
                        "latitude": coords[1]
                    })
        except Exception:
            pass
        time.sleep(0.5)
    # 8. Convert POI list into GeoDataFrame and spatially join with SA2 boundaries
    poi_df = pd.DataFrame(all_pois).dropna(subset=['longitude', 'latitude'])
    # 8-1. data cleaning
    poi_df = poi_df.drop_duplicates(subset=['sa2_name','longitude','latitude','category'])

    if not poi_df.empty:
        poi_df['geometry'] = poi_df.apply(lambda r: Point(r['longitude'], r['latitude']), axis=1)
        poi_gdf = gpd.GeoDataFrame(poi_df, geometry='geometry', crs="EPSG:4326").to_crs(sa2_filtered.crs)
        joined = gpd.sjoin(poi_gdf, sa2_filtered, how="left", predicate="intersects")
        poi_counts = joined.groupby("SA2_NAME21").size().reset_index(name="POI_count")
        sa2_filtered = sa2_filtered.merge(poi_counts, on="SA2_NAME21", how="left")
        sa2_filtered["POI_count"] = sa2_filtered["POI_count"].fillna(0)
    else:
        sa2_filtered["POI_count"] = 0
    # 9. Calculate z-scores 
    df = pd.DataFrame()
    df['SA2_NAME'] = sa2_filtered['SA2_NAME21'].values

    df['z_business'] = (sa2_filtered['total_businesses'] - sa2_filtered['total_businesses'].mean()) / sa2_filtered['total_businesses'].std()
    df['z_POI'] = (sa2_filtered['POI_count'] - sa2_filtered['POI_count'].mean()) / sa2_filtered['POI_count'].std() if sa2_filtered['POI_count'].std() != 0 else 0
    if sa2_filtered['median_income'].std() == 0:
        df['z_income'] = 0
    else:
        df['z_income'] = (sa2_filtered['median_income'] - sa2_filtered['median_income'].mean()) / sa2_filtered['median_income'].std()
    # 9-1. Calculate z-score for young population
    young_pop_summary = population.groupby('sa2_name')['young_people'].sum().reset_index()
    sa2_filtered = sa2_filtered.merge(young_pop_summary, left_on='SA2_NAME21', right_on='sa2_name', how='left')
    sa2_filtered['young_people'] = sa2_filtered['young_people'].fillna(0)
    df['z_young'] = (sa2_filtered['young_people'] - sa2_filtered['young_people'].mean()) / sa2_filtered['young_people'].std()
    # 9-2. Combine z-scores into a final score using weighted sigmoid function
    df['score'] = sigmoid(
        0.3 * df['z_POI'] +
        0.3 * df['z_income'] +
        0.2 * df['z_business'] +
        0.2 * df['z_young']
    )
    # 10. Display
    display(df.style.set_table_styles(
        [{'selector': 'table', 'props': [('border', '1px solid black')]}]
    ).set_properties(**{'border': '1px solid black'}))
    # 11. Create a choropleth map of the composite score and save as PNG
    score_map = sa2_filtered.merge(df[['SA2_NAME', 'score']], left_on='SA2_NAME21', right_on='SA2_NAME')
    fig, ax = plt.subplots(1, 1, figsize=(10, 8))
    score_map.plot(column='score', cmap='viridis', linewidth=0.8, edgecolor='0.8', legend=True, ax=ax)
    ax.set_title(f"{sa4_name} - SA2 Level Score Map", fontsize=14)
    ax.axis('off')
    # 11-1. Ensure output folder exists
    os.makedirs("../output", exist_ok=True)
    # 11-2. Save PNG to output directory
    safe_sa4_name = sa4_name.lower().replace(" ", "_").replace("-", "")
    filename = f"../output/score_map_{safe_sa4_name}.png"
    plt.savefig(filename, bbox_inches='tight')
    plt.show()
    plt.close()
    print(f"🖼️ Map saved to {filename}")
    # 12. Save analysis results to CSV
    safe_name = sa4_name.lower().replace(" ", "_").replace("-", "_")
    csv_path = f"../output/sa2_scores_{safe_name}.csv"
    df.to_csv(csv_path, index=False)
    print("📁 Results saved to sa2_scores.csv")
    # 13. Save analysis results to PostgreSQL
    engine = create_engine("postgresql://postgres:0111@localhost:5432/project")
    table_name = f"sa2_scores_{safe_name}"
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print("💾 Results saved to PostgreSQL")

    return df




df_parramatta = analyze_sa4('Sydney - Parramatta')
df_inner_south_west = analyze_sa4('Sydney - Inner South West')
df_northern_beaches = analyze_sa4('Sydney - Northern Beaches')



def summarize_scores(df):
    print("📊 Average score:", df['score'].mean())
    print("📊 Highest score:", df['score'].max())
    print("📊 Lowest score:", df['score'].min())
    print("📊 Standard deviation:", df['score'].std())
    
    #print("\nTop 5 SA2 areas by score:")
    #display(df.sort_values(by='score', ascending=False).head(5))
    
    #print("\nBottom 5 SA2 areas by score:")
    #display(df.sort_values(by='score', ascending=True).head(5))


summarize_scores(df_parramatta)
summarize_scores(df_inner_south_west)
summarize_scores(df_northern_beaches)


def compare_sa4_scores_named(df_dict):
    """
    Compare multiple SA4 regions' score statistics from a dictionary of {region_name: df}
    """
    summary = []

    for region, df in df_dict.items():
        summary.append({
            "SA4 Region": region,
            "Mean Score": round(df["score"].mean(), 3),
            "Std Dev": round(df["score"].std(), 3),
            "Max Score": round(df["score"].max(), 3),
            "Max SA2": df.loc[df["score"].idxmax(), "SA2_NAME"],
            "Min Score": round(df["score"].min(), 3),
            "Min SA2": df.loc[df["score"].idxmin(), "SA2_NAME"],
        })

    result_df = pd.DataFrame(summary)
    display(result_df.style.set_caption("📊 SA4 Region Score Comparison"))

compare_sa4_scores_named({
    "Northern Beaches": df_northern_beaches,
    "Inner West": df_inner_south_west,
    "Parramatta": df_parramatta
})

