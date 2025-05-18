import pandas as pd

def summarize_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute simple summary statistics of the score distribution.
    """
    summary = pd.DataFrame({
        "mean_score": [df.score.mean()],
        "median_score": [df.score.median()],
        "min_score": [df.score.min()],
        "max_score": [df.score.max()]
    })
    return summary

def compare_sa4_scores_named(df_dict: dict) -> pd.DataFrame:
    """
    Given a dict {SA4_name: df_scores}, build a comparison table.
    """
    records = []
    for name, df in df_dict.items():
        records.append({
            "SA4": name,
            "mean": df.score.mean(),
            "median": df.score.median()
        })
    return pd.DataFrame(records)
