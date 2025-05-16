readme_content = """
# DATA2001 Group Assignment – Inner South West, Parramatta, and Northern Beaches Analysis

This project analyzes socio-economic features of the Inner South West, Parramatta, and Northern Beaches SA4 region using various datasets, PostgreSQL/PostGIS, and Python-based spatial processing.

## 📁 Project Structure
- `scripts/` – Python scripts for data loading, score calculation, and visualization
- `data/` – Input datasets (CSV and shapefiles)
- `output/` – Output visualizations (e.g., score maps)
- `code.ipynb` – Step-by-step Jupyter notebook with the full workflow
- `Assignment Report.pdf` – Final written report with methodology and insights
- `README.md` – Project documentation

## 🧰 Technologies Used

- **Python**: `pandas`, `geopandas`, `sqlalchemy`, `matplotlib`, `shapely`, `requests`
- **PostgreSQL + PostGIS**: for storing and querying spatial and tabular data
- **NSW ArcGIS API**: to collect Point of Interest (POI) data dynamically

---

## 🚀 Execution Flow

1. **Data loading & processing**  
   Load population, income, business, stops, school catchments, and POI data  
   ➤ handled inside `analyze_sa4()` in `jupyter.ipynb`

2. **Score calculation**  
   For each SA2 in a selected SA4, calculate a weighted z-score:
   ```text
   Final Score = 
   0.3 * z(POI count) + 
   0.3 * z(median income) + 
   0.2 * z(business count) + 
   0.2 * z(young population)

## 📌 Output
- `sa2_scores` table with computed values
- `score_map` showing spatial score distribution

---

Developed for DATA2001, University Project 2025.
"""

with open("../README.md", "w", encoding="utf-8") as f:
    f.write(readme_content)

print("✅ README.md generated.")
