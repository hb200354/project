# DATA2001 Group Assignment – SA4 Well-Resourced Area Scoring

This project analyzes the well-resourced status of SA2 regions within a given SA4 area in Greater Sydney. It uses various datasets to compute composite scores based on public amenities, economic indicators, and services.

## 📁 Project Structure

- `scripts/`
  - `main.py` – Orchestrates the pipeline execution
  - `data_loader.py` – Loads CSV and shapefile data into PostgreSQL/PostGIS
  - `analyzer.py` – Contains the `analyze_sa4` function that processes an SA4
  - `poi_fetcher.py` – Fetches and processes POI data using a mock API
- `data/` – Contains input datasets such as Population.csv, Income.csv, Businesses.csv, and shapefiles
- `output/` – Stores generated score maps, correlation plots, and CSV outputs
- `notebooks/code.ipynb` – Jupyter notebook version of the analysis for interactive development
- `README.md` – Project documentation

## 🧰 Technologies Used

- **Python Libraries:** `pandas`, `geopandas`, `sqlalchemy`, `matplotlib`, `shapely`, `requests`
- **Database:** PostgreSQL with PostGIS for spatial data support

## 🚀 Execution Flow

1. **Data Loading**
   - Run `data_loader.py` to import all CSV and shapefiles into the PostgreSQL database.
   - This creates and populates tables like `population`, `businesses`, `income`, `stops`, `school_catchments`.

2. **SA4 Analysis**
   - Call `analyze_sa4(sa4_name)` from `main.py` or the notebook.
   - For each SA2 within the SA4, compute the following:

     - Business density per 1000 people
     - Public transport stop count
     - School catchment intersection count
     - Point of Interest count (mock data)
     
   - Z-scores are computed and normalized using the sigmoid function:

     ```python
     Final Score = sigmoid(
         z(business per 1000 people) +
         z(stop count) +
         z(school count) +
         z(poi count)
     )
     ```

3. **Output**
   - A CSV of scores is saved in `output/`
   - A PNG map of SA2 scores with top-3 annotated is generated
   - A PNG scatterplot of score vs income with correlation coefficient is also saved
   - All scores are written to a PostgreSQL table for reuse

## 📌 Example

```bash
python scripts/main.py
