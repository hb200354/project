# scripts/db.py
from sqlalchemy import create_engine

# Reusable DB connection function
def get_engine():
    return create_engine("postgresql://postgres:0111@localhost:5432/project")
