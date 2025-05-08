import pandas as pd
from sqlalchemy import create_engine
from scipy.stats import zscore

# Connect to PostgreSQL database
engine = create_engine("postgresql://postgres:0111@localhost:5432/project")

# Load POI data aggregated by SA2
poi = pd.read_sql("""
    SELECT s."SA2_CODE21" AS sa2_code21, COUNT(*) AS poi_count
    FROM sa2_boundaries s
    JOIN points_of_interest p
      ON ST_Within(ST_Transform(p.geometry, 7844), s.geometry)
    GROUP BY s."SA2_CODE21"
""", engine)

# Load income, business, and population data
income = pd.read_sql("SELECT sa2_code21, mean_income FROM income", engine)
biz = pd.read_sql("SELECT sa2_code AS sa2_code21, total_businesses FROM businesses", engine)
pop = pd.read_sql("SELECT sa2_code AS sa2_code21, total_people FROM population", engine)

# Ensure all merge keys are strings
for df_ in [poi, income, biz, pop]:
    df_["sa2_code21"] = df_["sa2_code21"].astype(str)

# Merge all datasets on SA2 code
df = poi.merge(income, on="sa2_code21")\
        .merge(biz, on="sa2_code21")\
        .merge(pop, on="sa2_code21")

# Convert relevant columns to numeric type
for col in ["mean_income", "total_businesses", "total_people", "poi_count"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# Calculate z-scores for each indicator
df["z_poi"] = zscore(df["poi_count"])
df["z_income"] = zscore(df["mean_income"])
df["z_business"] = zscore(df["total_businesses"])
df["z_population"] = zscore(df["total_people"])

# Compute final composite score
df["final_score"] = (
    df["z_poi"] * 0.3 +
    df["z_income"] * 0.3 +
    df["z_business"] * 0.2 +
    df["z_population"] * 0.2
)

# Save the final result to the database
df.to_sql("sa2_scores", engine, if_exists="replace", index=False)
print("✅ Score calculation complete and saved.")
