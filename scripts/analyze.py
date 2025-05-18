import os
import time
import requests
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from sqlalchemy import create_engine, text
from IPython.display import display

# --- File paths (adjust absolute paths here) ---
DATA_DIR       = "/Users/S/Desktop/DATA2001/project/data/"
BUSINESSES_CSV = DATA_DIR + "Businesses.csv"
INCOME_CSV     = DATA_DIR + "Income.csv"
POPULATION_CSV = DATA_DIR + "Population.csv"
SA2_SHP        = DATA_DIR + "SA2_2021_AUST_SHP_GDA2020/SA2_2021_AUST_GDA2020.shp"
STOPS_TXT      = DATA_DIR + "Stops.txt"
CATCH_PRIM_SHP = DATA_DIR + "catchments/catchments/catchments_primary.shp"
CATCH_SEC_SHP  = DATA_DIR + "catchments/catchments/catchments_secondary.shp"
OUTPUT_DIR     = "/Users/S/Desktop/DATA2001/project/output/"

# --- Utility functions ---
def sigmoid(x):
    return 1 / (1 + np.exp(-x))

def fetch_pois(sa2_gdf):
    """
    Fetch unique POIs via NSW API for each SA2.
    """
    url = ("https://maps.six.nsw.gov.au/"
           "arcgis/rest/services/public/NSW_POI/MapServer/0/query")
    recs = []
    for _, row in sa2_gdf.iterrows():
        minx, miny, maxx, maxy = row.geometry.bounds
        params = {
            "f":"geojson",
            "geometryType":"esriGeometryEnvelope",
            "geometry":f"{minx},{miny},{maxx},{maxy}",
            "inSR":"4326",
            "spatialRel":"esriSpatialRelIntersects",
            "outFields":"*",
            "returnGeometry":"true"
        }
        try:
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            for feat in r.json().get("features", []):
                lon, lat = feat["geometry"]["coordinates"]
                if lon is not None and lat is not None:
                    recs.append({
                        "sa2_name": row.sa2_name21,
                        "longitude": lon,
                        "latitude": lat
                    })
        except:
            pass
        time.sleep(0.3)
    df = pd.DataFrame(recs).drop_duplicates(["sa2_name","longitude","latitude"])
    return df

def analyze_sa4(sa4_name: str) -> pd.DataFrame:
    """
    Full SA4 workflow:
      1. Load & filter SA2
      2. Preprocess population, biz, stops, schools, income, POI
      3. Compute per-1000 indicators & z-scores
      4. Composite via weighted sigmoid
      5. Save CSV, DB, Map, Correlation, Table
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    engine = create_engine("postgresql://postgres:0111@localhost:5432/project")

    # 1) Load & filter SA2 boundaries
    sa2 = gpd.read_file(SA2_SHP).to_crs(epsg=4326)
    sa2 = sa2.rename(columns={
        "SA2_CODE21":"sa2_code21","SA2_NAME21":"sa2_name21",
        "GCC_NAME21":"gcc_name21","SA4_NAME21":"sa4_name21"
    })
    region = sa2[
        (sa2.gcc_name21=="Greater Sydney") &
        (sa2.sa4_name21==sa4_name)
    ].copy()

    # 2) Population: filter ≥100, compute young
    pop = pd.read_csv(POPULATION_CSV).drop_duplicates("sa2_name")
    pop = pop[pop.total_people >= 100]
    pop["young"] = pop[["0-4_people","5-9_people","10-14_people","15-19_people"]].sum(axis=1)
    pop.loc[pop.young>pop.total_people, "young"] = pop.total_people

    # 3) Business per 1 000 people
    biz = pd.read_csv(BUSINESSES_CSV).groupby("sa2_name", as_index=False).total_businesses.sum()
    biz = biz.rename(columns={"sa2_name":"SA2_NAME"})

    # 4) Stops per 1 000 young people
    stops = pd.read_csv(STOPS_TXT).dropna(subset=["stop_lat","stop_lon"])
    stops["geometry"] = stops.apply(lambda r: Point(r.stop_lon,r.stop_lat), axis=1)
    stops_gdf = gpd.GeoDataFrame(stops, geometry="geometry", crs="EPSG:4326").to_crs(region.crs)
    stops_cnt = (
        gpd.sjoin(stops_gdf, region[["sa2_name21","geometry"]], predicate="intersects")
          .groupby("sa2_name21").size()
          .reset_index(name="stop_count")
          .rename(columns={"sa2_name21":"SA2_NAME"})
    )

    # 5) School catchment area per 1 000 young people
    p = gpd.read_file(CATCH_PRIM_SHP)
    s = gpd.read_file(CATCH_SEC_SHP)
    catch = gpd.GeoDataFrame(pd.concat([p,s],ignore_index=True), crs=p.crs).to_crs(region.crs)
    # intersect and sum polygon area
    area_series = (
        gpd.overlay(region[["sa2_name21","geometry"]], catch, how="intersection")
           .assign(area=lambda df: df.geometry.area)
           .groupby("sa2_name21").area.sum()
    ).reset_index(name="catchment_area")
    area_series = area_series.rename(columns={"sa2_name21":"SA2_NAME"})

    # 6) Income
    inc = pd.read_csv(INCOME_CSV)
    inc["median_income"] = pd.to_numeric(inc.median_income, errors="coerce")
    inc = inc.drop_duplicates("sa2_name").rename(columns={"sa2_name":"SA2_NAME"})

    # 7) POI per 1 000 people
    pois = fetch_pois(region)
    pois["geometry"] = pois.apply(lambda r: Point(r.longitude,r.latitude), axis=1)
    poi_gdf = gpd.GeoDataFrame(pois, geometry="geometry", crs="EPSG:4326").to_crs(region.crs)
    poi_cnt = (
        gpd.sjoin(poi_gdf, region[["sa2_name21","geometry"]], predicate="intersects")
          .groupby("sa2_name21").size()
          .reset_index(name="poi_count")
          .rename(columns={"sa2_name21":"SA2_NAME"})
    )

    # 8) Merge all indicators
    df = pd.DataFrame({"SA2_NAME": region.sa2_name21.values})
    df = (
        df
        .merge(biz,        on="SA2_NAME", how="left")
        .merge(stops_cnt,  on="SA2_NAME", how="left")
        .merge(area_series,on="SA2_NAME", how="left")
        .merge(inc,        on="SA2_NAME", how="left")
        .merge(poi_cnt,    on="SA2_NAME", how="left")
        .merge(pop[["sa2_name","young","total_people"]]
                   .rename(columns={"sa2_name":"SA2_NAME"}),
               on="SA2_NAME", how="left")
    ).fillna(0)

    # compute per-1 000 metrics
    df["biz_per_1000"]   = df.total_businesses   / (df.total_people   / 1000)
    df["stops_per_1000_young"] = df.stop_count  / (df.young          / 1000)
    df["school_area_per_1000_young"] = df.catchment_area / (df.young  / 1000)
    df["poi_per_1000"]   = df.poi_count         / (df.total_people   / 1000)

    # 9) z-scores & composite via weighted sigmoid
    for col, w in [
        ("biz_per_1000",   0.25),
        ("stops_per_1000_young", 0.25),
        ("school_area_per_1000_young", 0.25),
        ("poi_per_1000",   0.25)
    ]:
        df[f"z_{col}"] = (df[col] - df[col].mean()) / df[col].std()
    # equal weights → adjust as needed
    df["score"] = sigmoid(
        0.25*df.z_biz_per_1000
      + 0.25*df.z_stops_per_1000_young
      + 0.25*df.z_school_area_per_1000_young
      + 0.25*df.z_poi_per_1000
    )

    # 10) Save CSV & Postgres
    safe = sa4_name.lower().replace("sydney - ","").replace(" ","_")
    csv_out = os.path.join(OUTPUT_DIR, f"sa2_scores_{safe}.csv")
    df.to_csv(csv_out, index=False)
    df.to_sql(f"sa2_scores_{safe}", engine, if_exists="replace", index=False)
    print(f"✅ Saved scores CSV: {csv_out}")

    # 11) Map + top-3 labels
    m = region.merge(df[["SA2_NAME","score"]], left_on="sa2_name21", right_on="SA2_NAME")
    ax = m.plot(column="score", cmap="viridis", legend=True,
                figsize=(10,8), edgecolor="white")
    ax.set_title(f"{sa4_name} SA2 Score Map")
    ax.axis("off")
    top3 = m.nlargest(3, "score")
    for idx, row in enumerate(top3.itertuples(), 1):
        c = row.geometry.centroid
        ax.annotate(f"#{idx}\n{row.SA2_NAME}\n({row.score:.2f})",
                    xy=(c.x,c.y), ha="center", fontsize=8,
                    bbox=dict(boxstyle="round,pad=0.3",fc="white",ec="black"))
    png_out = os.path.join(OUTPUT_DIR, f"score_map_{safe}.png")
    plt.savefig(png_out, bbox_inches="tight"); plt.close()
    print(f"✅ Saved map PNG: {png_out}")

    # 12) Correlation (pop≥100)
    corr_df = df[df.total_people>=100]
    corr = corr_df.score.corr(corr_df.median_income)
    print(f"Correlation (pop≥100): {corr:.2f}")

    # 13) Display styled table
    display(df.style.set_table_styles([{"selector":"th","props":[("background","#EEE")]}]))
    return df
