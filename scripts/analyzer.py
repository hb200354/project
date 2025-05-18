import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from scipy.special import expit as sigmoid
from shapely.geometry import Point
from sqlalchemy import create_engine
from IPython.display import display

# Define input paths
folder_path = "/Users/S/Desktop/DATA2001/project/data/"

businesses_path = folder_path + "Businesses.csv"
income_path = folder_path + "Income.csv"
population_path = folder_path + "Population.csv"
shapefile_path = folder_path + "SA2_2021_AUST_SHP_GDA2020/SA2_2021_AUST_GDA2020.shp"
stops_path = folder_path + "Stops.txt"
catchments_primary_path = folder_path + "catchments/catchments/catchments_primary.shp"
catchments_secondary_path = folder_path + "catchments/catchments/catchments_secondary.shp"

def safe_zscore(series):
    std = series.std(ddof=0)
    return (series - series.mean()) / std if std != 0 else pd.Series(0, index=series.index)

def fetch_and_save_pois_from_api(sa2_df):
    # This function mocks API POI fetching and returns a GeoDataFrame
    # Replace this logic with real API calls as needed
    data = {
        "sa2_name": [sa2_df.iloc[0]['sa2_name21']] * 3,
        "poi_name": ["Library", "Playground", "Clinic"],
        "category": ["Education", "Recreation", "Health"],
        "lon": [151.01, 151.03, 151.05],
        "lat": [-33.88, -33.89, -33.87]
    }
    pois = gpd.GeoDataFrame(
        data,
        geometry=gpd.points_from_xy(data["lon"], data["lat"]),
        crs="EPSG:4326"
    )
    return pois

def analyze_sa4(sa4_name):
    engine = create_engine("postgresql://postgres:0111@localhost:5432/project")

    # Load and filter SA2 boundaries
    sa2 = gpd.read_file(shapefile_path)
    sa2 = sa2.rename(columns={
        "SA2_CODE21": "sa2_code21", "SA2_NAME21": "sa2_name21",
        "GCC_NAME21": "gcc_name21", "SA4_NAME21": "sa4_name21"
    })
    sa2.to_postgis("sa2_boundaries", engine, if_exists="replace", index=False)
    sa2_f = sa2[(sa2["gcc_name21"] == "Greater Sydney") & (sa2["sa4_name21"] == sa4_name)].copy()

    # Load population and filter
    pop = pd.read_csv(population_path).drop_duplicates(subset=['sa2_name'])
    pop_clean = pop[["sa2_name", "total_people"]].dropna().drop_duplicates(subset="sa2_name")
    pop_clean = pop_clean[pop_clean["total_people"] >= 100]

    sa2_f = sa2_f.merge(pop_clean, left_on="sa2_name21", right_on="sa2_name", how="left")
    sa2_f["total_people"] = sa2_f["total_people"].fillna(0)
    sa2_f = sa2_f[sa2_f["total_people"] >= 100].copy()
    sa2_f = sa2_f.drop(columns=["sa2_name"])

    # Compute young population
    young_cols = ['0-4_people', '5-9_people', '10-14_people', '15-19_people']
    pop['young'] = pop[young_cols].sum(axis=1)
    pop.loc[pop['young'] > pop['total_people'], 'young'] = pop['total_people']

    # Load and merge business data
    biz = pd.read_csv(businesses_path)
    biz_sum = biz.groupby('sa2_name')['total_businesses'].sum().reset_index()
    sa2_f = sa2_f.merge(biz_sum, left_on='sa2_name21', right_on='sa2_name', how='left').fillna({'total_businesses': 0})

    # Load and spatial join public transport stops
    st = pd.read_csv(stops_path).dropna(subset=['stop_lat', 'stop_lon'])
    st_g = gpd.GeoDataFrame(
        st.assign(geometry=st.apply(lambda r: Point(r.stop_lon, r.stop_lat), axis=1)),
        crs="EPSG:4326"
    ).to_crs(sa2_f.crs)
    sc = gpd.sjoin(st_g, sa2_f, predicate="intersects").groupby('sa2_name21').size().reset_index(name='stop_count')
    sa2_f = sa2_f.merge(sc, on='sa2_name21', how='left').fillna({'stop_count': 0})

    # Load and spatial join school catchments
    p = gpd.read_file(catchments_primary_path)
    s = gpd.read_file(catchments_secondary_path)
    sch = gpd.GeoDataFrame(pd.concat([p, s], ignore_index=True), crs=p.crs).to_crs(sa2_f.crs)
    sc2 = gpd.sjoin(sa2_f, sch, predicate="intersects").groupby('sa2_name21').size().reset_index(name='school_count')
    sa2_f = sa2_f.merge(sc2, on='sa2_name21', how='left').fillna({'school_count': 0})

    # Load and merge income data
    inc = pd.read_csv(income_path)
    inc['median_income'] = pd.to_numeric(inc['median_income'], errors='coerce')
    inc = inc[inc['sa2_name'].isin(sa2_f['sa2_name21'])]
    sa2_f = sa2_f.merge(inc[['sa2_name', 'median_income']], left_on='sa2_name21', right_on='sa2_name', how='left')

    # Fetch POIs via API and aggregate
    pois = fetch_and_save_pois_from_api(sa2_f)
    poi_cnt = pois.groupby('sa2_name').size().rename('poi_count').reset_index()
    sa2_f = sa2_f.merge(poi_cnt, left_on='sa2_name21', right_on='sa2_name', how='left').fillna({'poi_count': 0})

    # Merge young population
    if 'sa2_name' in sa2_f.columns:
        sa2_f = sa2_f.drop(columns=['sa2_name'])
    sa2_f = sa2_f.merge(pop[['sa2_name', 'young']], left_on='sa2_name21', right_on='sa2_name', how='left')
    sa2_f = sa2_f.drop(columns=['sa2_name'])
    sa2_f['young'] = sa2_f['young'].fillna(0)

    # Z-score calculation
    df = pd.DataFrame({'SA2_NAME': sa2_f['sa2_name21']})
    sa2_f['biz_per_1000'] = sa2_f['total_businesses'] / (sa2_f['total_people'] / 1000)

    df['z_biz'] = safe_zscore(sa2_f['biz_per_1000'])
    df['z_stops'] = safe_zscore(sa2_f['stop_count'])
    df['z_schools'] = safe_zscore(sa2_f['school_count'])
    df['z_poi'] = safe_zscore(sa2_f['poi_count'])
    df['score'] = sigmoid(df['z_biz'] + df['z_stops'] + df['z_schools'] + df['z_poi'])

    # Save to DB and file
    safe = sa4_name.lower().replace("sydney - ", "").replace(" ", "_")
    sa2_f[['sa2_code21', 'sa2_name21', 'poi_count']].to_csv(f"/Users/S/Desktop/DATA2001/project/data/output/sa2_scores_{safe}.csv", index=False)
    df.to_sql(f"sa2_scores_{safe}", engine, if_exists='replace', index=False)

    # Plot and save score map
    score_map = sa2_f.merge(df[['SA2_NAME', 'score']], left_on='sa2_name21', right_on='SA2_NAME')
    fig, ax = plt.subplots(1, 1, figsize=(10, 8))
    score_map.plot(column='score', cmap='viridis', legend=True, edgecolor='0.8', ax=ax)
    ax.set_axis_off()
    ax.set_title(f"{sa4_name} SA2 Score Map (Top 3 Highlighted)", fontsize=15)

    top5 = score_map.nlargest(3, 'score').reset_index(drop=True)
    for i, row in top5.iterrows():
        centroid = row["geometry"].centroid
        ax.annotate(f"#{i+1}: {row['SA2_NAME']}\n({row['score']:.2f})",
                    xy=(centroid.x, centroid.y),
                    xytext=(3, 3),
                    textcoords="offset points",
                    fontsize=8,
                    bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8))

    plt.savefig(f"/Users/S/Desktop/DATA2001/project/data/output/score_map_{safe}.png", bbox_inches='tight')
    plt.show()

    # Plot and save correlation
    correlation = df['score'].corr(sa2_f['median_income'])
    print(f"Correlation between score and income: {correlation:.4f}")
    plt.figure(figsize=(8, 6))
    plt.scatter(df['score'], sa2_f['median_income'])
    plt.title(f'Score vs Income (Corr={correlation:.2f})')
    plt.xlabel('Score')
    plt.ylabel('Median Income')
    plt.grid(True, alpha=0.3)
    plt.savefig(f"/Users/S/Desktop/DATA2001/project/data/output/correlation_{safe}.png")
    plt.show()

    # Display DataFrame
    display(df.style.set_properties(**{'border': '1px solid black'}))

    return df
