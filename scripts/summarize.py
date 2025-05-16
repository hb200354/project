import pandas as pd
from IPython.display import display

def summarize_scores(df):
    print("📊 Average score:", df['score'].mean())
    print("📊 Highest score:", df['score'].max())
    print("📊 Lowest score:", df['score'].min())
    print("📊 Standard deviation:", df['score'].std())

def compare_sa4_scores_named(df_dict):
    summary = []
    for region, df in df_dict.items():
        summary.append({
            "SA4 Region": region,
            "Mean Score": round(df["score"].mean(), 3),
            "Std Dev": round(df["score"].std(), 3),
            "Max Score": round(df["score"].max(), 3),
            "Max SA2": df.loc[df["score"].idxmax(), "SA2_NAME"],
            "Min Score": round(df["score"].min(), 3),
            "Min SA2": df.loc[df["score"].idxmin(), "SA2_NAME"],
        })
    result_df = pd.DataFrame(summary)
    display(result_df.style.set_caption("📊 SA4 Region Score Comparison"))
