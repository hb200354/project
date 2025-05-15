readme_content = """
# DATA2001 Group Assignment – Inner South West, Parramatta Analysis

This project analyzes socio-economic features of the Inner South West, Parramatta SA4 region using various datasets, PostgreSQL/PostGIS, and Python-based spatial processing.

## 📁 Project Structure
- `scripts/` – Python scripts for data loading, score calculation, and visualization
- `data/` – Input datasets (CSV and shapefiles)
- `output/` – Output visualizations (e.g., score maps)
- `code.ipynb` – Step-by-step Jupyter notebook with the full workflow
- `Assignment Report.pdf` – Final written report with methodology and insights
- `README.md` – Project documentation

## 🔧 Technologies Used
- Python (pandas, geopandas, sqlalchemy, scipy, matplotlib)
- PostgreSQL + PostGIS
- NSW ArcGIS API

## 🚀 Execution Flow
1. `scripts/load_data.py` – Loads and filters data into PostgreSQL
2. `scripts/calculate_score.py` – Calculates z-score-based final scores per SA2
3. `scripts/visualize.py` – Generates choropleth map of final scores

## 📊 Scoring Methodology
Final score per SA2 is calculated as:
```text
0.3 * z(POIs) + 0.3 * z(mean_income) + 0.2 * z(businesses) + 0.2 * z(population)
```

## 📌 Output
- `sa2_scores` table with computed values
- `output/score_map.png` showing spatial score distribution

---

Developed for DATA2x01, University Project 2025.
"""

with open("../README.md", "w", encoding="utf-8") as f:
    f.write(readme_content)

print("✅ README.md generated.")
