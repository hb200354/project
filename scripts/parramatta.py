import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from sqlalchemy import create_engine
import time
import requests
from sklearn.preprocessing import MinMaxScaler
import seaborn as sns

engine = create_engine("postgresql://postgres:0111@localhost:5432/project2")

shapefile_path = ".\data\SA2_2021_AUST_GDA2020\SA2_2021_AUST_GDA2020.shp"
gdf = gpd.read_file(shapefile_path)
gdf = gdf.set_crs('EPSG:7844', allow_override=True)
gdf_sydney = gdf[gdf["GCC_NAME21"] == "Greater Sydney"].to_crs(epsg=4326)
parramatta = gdf_sydney[gdf_sydney["SA4_NAME21"] == "Sydney - Parramatta"]
parramatta = parramatta[~parramatta.geometry.isna()]
parramatta.to_postgis('parramatta', engine, if_exists='replace')

csv_files = {
    "Population.csv": "population",
    "Businesses.csv": "businesses",
    "Income.csv": "income"
}
for filename, tablename in csv_files.items():
    df = pd.read_csv(f"./data/{filename}")
    df.dropna(inplace=True)
    df.columns = df.columns.str.lower()
    df.to_sql(tablename, engine, if_exists='replace', index=False)

stops_df = pd.read_csv("./data/Stops.txt")
stops_df['geometry'] = stops_df.apply(lambda row: Point(row['stop_lon'], row['stop_lat']), axis=1)
stops_gdf = gpd.GeoDataFrame(stops_df, geometry='geometry', crs='EPSG:4326')
stops_gdf.to_postgis('stops', engine, if_exists='replace')


catchments_gdf = gpd.read_file('.\data\catchments\catchments\catchments_primary.shx')
catchments_gdf = catchments_gdf[catchments_gdf.is_valid].to_crs(epsg=4326)
catchments_gdf.to_postgis('school_catchments', engine, if_exists='replace')


def get_arcgis_pois(minx, miny, maxx, maxy):
    url = "https://maps.six.nsw.gov.au/arcgis/rest/services/public/NSW_POI/MapServer/0/query"
    bbox_str = f"{minx},{miny},{maxx},{maxy}"
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
        response = requests.get(url, params=params)
        data = response.json()
        return data.get("features", [])
    except Exception as e:
        print("API 오류:", e)
        return []

poi_data = []
for idx, row in parramatta.iterrows():
    minx, miny, maxx, maxy = row.geometry.bounds
    features = get_arcgis_pois(minx, miny, maxx, maxy)
    for feature in features:
        props = feature.get("properties", {})
        geom = feature.get("geometry", {})
        coords = geom.get("coordinates", [None, None])
        poi_data.append({
            "sa2_code": row["SA2_CODE21"],
            "sa2_name": row["SA2_NAME21"],
            "poi_name": props.get("NAME"),
            "category": props.get("POI_GROUP"),
            "longitude": coords[0],
            "latitude": coords[1]
        })
    time.sleep(1)

poi_df = pd.DataFrame(poi_data)
poi_df.dropna(inplace=True)
poi_df['geometry'] = poi_df.apply(lambda r: Point(r['longitude'], r['latitude']), axis=1)
poi_gdf = gpd.GeoDataFrame(poi_df, geometry='geometry', crs="EPSG:4326")
poi_gdf.to_postgis("points_of_interest", engine, if_exists="replace")

stops_with_sa2 = gpd.sjoin(stops_gdf, parramatta[['SA2_CODE21', 'geometry']], how='left', predicate='within')
stops_with_sa2['SA2_CODE21'] = stops_with_sa2['SA2_CODE21'].astype(str).str.zfill(9)
stop_counts = stops_with_sa2.groupby('SA2_CODE21').size()

catchments_with_sa2 = gpd.sjoin(catchments_gdf, parramatta[['SA2_CODE21', 'geometry']], how='left', predicate='intersects')
catchments_with_sa2['SA2_CODE21'] = catchments_with_sa2['SA2_CODE21'].astype(str).str.zfill(9)
school_counts = catchments_with_sa2.groupby('SA2_CODE21').size()

business_df = pd.read_sql("SELECT * FROM businesses", engine)
poi_df = pd.read_sql("SELECT * FROM points_of_interest", engine)

business_df['sa2_code'] = business_df['sa2_code'].astype(str).str.zfill(9)
poi_df['sa2_code'] = poi_df['sa2_code'].astype(str).str.zfill(9)

business_counts = business_df.groupby('sa2_code')['total_businesses'].sum()
poi_counts = poi_df.groupby('sa2_code')['poi_name'].count()

business_counts.index = business_counts.index.astype(str)
stop_counts.index = stop_counts.index.astype(str)
school_counts.index = school_counts.index.astype(str)
poi_counts.index = poi_counts.index.astype(str)

score_df = pd.DataFrame(index=parramatta['SA2_CODE21'].astype(str).str.zfill(9))
score_df['businesses'] = business_counts
score_df['stops'] = stop_counts
score_df['schools'] = school_counts
score_df['pois'] = poi_counts
score_df = score_df.fillna(0)

population_df = pd.read_csv("./data/Population.csv")
population_df.columns = (
    population_df.columns
        .str.strip()
        .str.lower()
        .str.replace("-", "_")
        .str.replace(" ", "_")
)
population_df['sa2_code'] = population_df['sa2_code'].astype(str).str.zfill(9)
population_df['young_people'] = population_df[[
    '0_4_people', '5_9_people', '10_14_people', '15_19_people'
]].sum(axis=1)

parramatta_sa2_codes = parramatta['SA2_CODE21'].astype(str).str.zfill(9).tolist()
population_sub = population_df[population_df['sa2_code'].isin(parramatta_sa2_codes)].copy()

score_df['young_people'] = score_df.index.map(population_sub.set_index('sa2_code')['young_people'].to_dict())

print(score_df[['young_people']].head())
print("남은 NaN 수:", score_df['young_people'].isna().sum())

score_df_clean = score_df[
    (score_df['young_people'] >= 100) &
    (
        (score_df['businesses'] > 0) |
        (score_df['stops'] > 0) |
        (score_df['schools'] > 0) |
        (score_df['pois'] > 0)
    )
].copy()

scaler = MinMaxScaler()
scaled = scaler.fit_transform(score_df_clean[[
    'businesses_per_1000', 'stops_per_1000', 'schools_per_1000', 'pois_per_1000'
]])
score_df_clean[['businesses_scaled', 'stops_scaled', 'schools_scaled', 'pois_scaled']] = scaled
score_df_clean['final_score'] = score_df_clean[['businesses_scaled', 'stops_scaled', 'schools_scaled', 'pois_scaled']].mean(axis=1)

import seaborn as sns
import matplotlib.pyplot as plt
plt.figure(figsize=(8, 4))
sns.histplot(score_df_clean['final_score'], bins=20, kde=True)
plt.title("Cleaned Final Score Distribution")
plt.xlabel("Final Score")
plt.ylabel("Count")
plt.grid(True)
plt.show()


score_df_clean['businesses_per_1000'] = (score_df_clean['businesses'] / score_df_clean['young_people']) * 1000
score_df_clean['stops_per_1000'] = (score_df_clean['stops'] / score_df_clean['young_people']) * 1000
score_df_clean['schools_per_1000'] = (score_df_clean['schools'] / score_df_clean['young_people']) * 1000
score_df_clean['pois_per_1000'] = (score_df_clean['pois'] / score_df_clean['young_people']) * 1000

import numpy as np
for col in ['businesses_per_1000', 'stops_per_1000', 'schools_per_1000', 'pois_per_1000']:
    score_df_clean[f'{col}_log'] = np.log1p(score_df_clean[col])

from sklearn.preprocessing import MinMaxScaler
scaler = MinMaxScaler()
scaled = scaler.fit_transform(score_df_clean[[
    'businesses_per_1000_log', 'stops_per_1000_log',
    'schools_per_1000_log', 'pois_per_1000_log'
]])
score_df_clean[['b_scaled', 's_scaled', 'sch_scaled', 'p_scaled']] = scaled

score_df_clean['final_score'] = score_df_clean[['b_scaled', 's_scaled', 'sch_scaled', 'p_scaled']].mean(axis=1)

score_df_sorted = score_df_clean.sort_values(by='final_score', ascending=False)
print(score_df_sorted[[
    'businesses_per_1000', 'stops_per_1000', 'schools_per_1000', 'pois_per_1000', 'final_score'
]].head())

income_df = pd.read_sql("SELECT * FROM income", engine)
income_df['sa2_code21'] = income_df['sa2_code21'].astype(str).str.zfill(9)

income_sub = income_df[income_df['sa2_code21'].isin(score_df_clean.index)]

score_df_clean['median_income'] = score_df_clean.index.map(
    income_sub.set_index('sa2_code21')['median_income'].to_dict()
)

plt.figure(figsize=(8, 6))
sns.scatterplot(data=score_df_clean, x='final_score', y='median_income')
plt.title('Final Score vs Median Income (Cleaned)')
plt.xlabel('Final Score')
plt.ylabel('Median Income')
plt.grid(True)
plt.tight_layout()
plt.show()

correlation = score_df_clean[['final_score', 'median_income']].corr().iloc[0, 1]
print(f"✅ Final Score와 Median Income 간 상관계수: {correlation:.3f}")



