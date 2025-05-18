# Main script to run the entire analysis pipeline
from data_loader import load_all_data
from poi_fetcher import fetch_pois
from analyzer import analyze_sa4

if __name__ == "__main__":
    # 1. Load and insert raw data into PostgreSQL
    load_all_data()

    # 2. Fetch POIs from NSW Transport API and save to DB
    fetch_pois()

    # 3. Analyze selected SA4 regions
    analyze_sa4("Sydney - Parramatta")
    analyze_sa4("Sydney - Inner South West")
    analyze_sa4("Sydney - Northern Beaches")