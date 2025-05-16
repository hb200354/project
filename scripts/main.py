from load_schema import create_schema
from analyze import analyze_sa4
from summarize import summarize_scores, compare_sa4_scores_named

def main():
    create_schema()

    df_parramatta = analyze_sa4('Sydney - Parramatta')
    df_inner_south_west = analyze_sa4('Sydney - Inner South West')
    df_northern_beaches = analyze_sa4('Sydney - Northern Beaches')

    print("\n[ Parramatta Summary ]")
    summarize_scores(df_parramatta)
    print("\n[ Inner South West Summary ]")
    summarize_scores(df_inner_south_west)
    print("\n[ Northern Beaches Summary ]")
    summarize_scores(df_northern_beaches)

    compare_sa4_scores_named({
        "Parramatta": df_parramatta,
        "Inner South West": df_inner_south_west,
        "Northern Beaches": df_northern_beaches
    })

if __name__ == "__main__":
    main()
