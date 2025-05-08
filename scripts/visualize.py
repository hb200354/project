import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# Set up matplotlib parameters for better readability
plt.rcParams.update({
    'font.size': 12,
    'axes.titlesize': 16,
    'axes.labelsize': 12,
    'figure.figsize': (12, 10),
    'figure.dpi': 100
})

# Connect to PostgreSQL database
engine = create_engine("postgresql://postgres:0111@localhost:5432/project")

# Load SA2 boundaries and scores
sa2_gdf = gpd.read_postgis("SELECT * FROM sa2_boundaries", engine, geom_col="geometry")
scores_df = pd.read_sql("SELECT * FROM sa2_scores", engine)

# Ensure all merge keys are strings
sa2_gdf["SA2_CODE21"] = sa2_gdf["SA2_CODE21"].astype(str)
scores_df["sa2_code21"] = scores_df["sa2_code21"].astype(str)

# Merge SA2 boundaries with scores
merged = sa2_gdf.merge(scores_df, left_on="SA2_CODE21", right_on="sa2_code21")

# Set up the figure and axis
fig, ax = plt.subplots()
merged.plot(
    column="final_score",
    cmap="YlGnBu",
    edgecolor="black",
    linewidth=0.5,
    legend=True,
    ax=ax
)
ax.set_title("Final Score by SA2 Region (Inner South West)", fontsize=16)
ax.axis("off")
plt.tight_layout()
plt.savefig("output/score_map.png")
plt.show()
print("✅ Visualization complete and saved as 'output/score_map.png'.")
