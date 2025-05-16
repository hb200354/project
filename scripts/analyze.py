import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from sqlalchemy import create_engine
import time
import requests
import numpy as np
import os

def sigmoid(x):
    return 1 / (1 + np.exp(-x))

def analyze_sa4(sa4_name):
    sa2 = gpd.read_file("..\data\SA2_2021_AUST_SHP_GDA2020\SA2_2021_AUST_GDA2020.shp")
    sa2_gsyd = sa2[sa2['GCC_NAME21'] == 'Greater Sydney']
    sa2_filtered = sa2_gsyd[sa2_gsyd['SA4_NAME21'] == sa4_name].copy()
    sa2_filtered = sa2_filtered[sa2_filtered.geometry.is_valid]

    population = pd.read_csv("../data/Population.csv")
    population = population[population['sa2_name'].isin(sa2_filtered['SA2_NAME21'])]
    population = population.drop_duplicates(subset=['sa2_name'])
    population = population[population['total_people'] >= 100]
    population = population[population['sa2_name'].isin(sa2_filtered['SA2_NAME21'])]
    young_cols = ['0-4_people', '5-9_people', '10-14_people', '15-19_people']
    population['young_people'] = population[young_cols].sum(axis=1)
    population.loc[population['young_people'] > population['total_people'], 'young_people'] =         population.loc[population['young_people'] > population['total_people'], 'total_people']

    businesses = pd.read_csv("../data/Businesses.csv")
    biz_summary = businesses.groupby('sa2_name')['total_businesses'].sum().reset_index()
    sa2_filtered = sa2_filtered.merge(biz_summary, left_on='SA2_NAME21', right_on='sa2_name', how='left')
    sa2_filtered['total_businesses'] = sa2_filtered['total_businesses'].fillna(0)

    stops = pd.read_csv("../data/Stops.txt")
    stops = stops.dropna(subset=['stop_lat', 'stop_lon'])
    stops['geometry'] = stops.apply(lambda row: Point(row['stop_lon'], row['stop_lat']), axis=1)
    stops_gdf = gpd.GeoDataFrame(stops, geometry='geometry', crs="EPSG:4326").to_crs(sa2_filtered.crs)
    stop_counts = gpd.sjoin(stops_gdf, sa2_filtered, how="inner", predicate="intersects")                      .groupby("SA2_NAME21").size().reset_index(name="stop_count")
    sa2_filtered = sa2_filtered.merge(stop_counts, on="SA2_NAME21", how="left")
    sa2_filtered["stop_count"] = sa2_filtered["stop_count"].fillna(0)

    primary = gpd.read_file("../data/catchments/catchments/catchments_primary.shp")
    secondary = gpd.read_file("../data/catchments/catchments/catchments_secondary.shp")
    schools = pd.concat([primary, secondary], ignore_index=True).to_crs(sa2_filtered.crs)
    school_counts = gpd.sjoin(sa2_filtered, schools, how="left", predicate="intersects")                        .groupby("SA2_NAME21").size().reset_index(name="school_count")
    sa2_filtered = sa2_filtered.merge(school_counts, on="SA2_NAME21", how="left")
    sa2_filtered["school_count"] = sa2_filtered["school_count"].fillna(0)

    income = pd.read_csv("../data/Income.csv")
    income['median_income'] = pd.to_numeric(income['median_income'], errors='coerce')
    income = income[income['sa2_name'].isin(sa2_filtered['SA2_NAME21'])]
    sa2_filtered = sa2_filtered.merge(income[['sa2_name', 'median_income']], 
                                  left_on='SA2_NAME21', 
                                  right_on='sa2_name', 
                                  how='left')

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

    poi_df = pd.DataFrame(all_pois).dropna(subset=['longitude', 'latitude'])
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

    df = pd.DataFrame()
    df['SA2_NAME'] = sa2_filtered['SA2_NAME21'].values
    df['z_business'] = (sa2_filtered['total_businesses'] - sa2_filtered['total_businesses'].mean()) / sa2_filtered['total_businesses'].std()
    df['z_POI'] = (sa2_filtered['POI_count'] - sa2_filtered['POI_count'].mean()) / sa2_filtered['POI_count'].std() if sa2_filtered['POI_count'].std() != 0 else 0
    if sa2_filtered['median_income'].std() == 0:
        df['z_income'] = 0
    else:
        df['z_income'] = (sa2_filtered['median_income'] - sa2_filtered['median_income'].mean()) / sa2_filtered['median_income'].std()
    young_pop_summary = population.groupby('sa2_name')['young_people'].sum().reset_index()
    sa2_filtered = sa2_filtered.merge(young_pop_summary, left_on='SA2_NAME21', right_on='sa2_name', how='left')
    sa2_filtered['young_people'] = sa2_filtered['young_people'].fillna(0)
    df['z_young'] = (sa2_filtered['young_people'] - sa2_filtered['young_people'].mean()) / sa2_filtered['young_people'].std()
    df['score'] = sigmoid(
        0.3 * df['z_POI'] +
        0.3 * df['z_income'] +
        0.2 * df['z_business'] +
        0.2 * df['z_young']
    )

    os.makedirs("../output", exist_ok=True)
    safe_sa4_name = sa4_name.lower().replace(" ", "_").replace("-", "")
    filename = f"../output/score_map_{safe_sa4_name}.png"
    score_map = sa2_filtered.merge(df[['SA2_NAME', 'score']], left_on='SA2_NAME21', right_on='SA2_NAME')
    fig, ax = plt.subplots(1, 1, figsize=(10, 8))
    score_map.plot(column='score', cmap='viridis', linewidth=0.8, edgecolor='0.8', legend=True, ax=ax)
    ax.set_title(f"{sa4_name} - SA2 Level Score Map", fontsize=14)
    ax.axis('off')
    plt.savefig(filename, bbox_inches='tight')
    plt.close()
    print(f"🖼️ Map saved to {filename}")
    safe_name = sa4_name.lower().replace(" ", "_").replace("-", "_")
    csv_path = f"../output/sa2_scores_{safe_name}.csv"
    df.to_csv(csv_path, index=False)
    print(f"📁 Results saved to {csv_path}")
    engine = create_engine("postgresql://postgres:0111@localhost:5432/project")
    table_name = f"sa2_scores_{safe_name}"
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print("💾 Results saved to PostgreSQL")
    return df
