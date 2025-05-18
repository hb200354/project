from load_schema import create_schema
from analyze import analyze_sa4
from summarize import summarize_scores, compare_sa4_scores_named

def main():
    # 0) Initialize DB schema
    create_schema()

    # List your SA4 regions here:
    SA4_REGIONS = [
        "Sydney - Parramatta",
        "Sydney - Inner South West",
        "Sydney - Northern Beaches"
    ]

    # 1) Run analysis for each SA4
    results = {r: analyze_sa4(r) for r in SA4_REGIONS}

    # 2) Summaries
    for name, df in results.items():
        print(f"\n=== Summary for {name} ===")
        print(summarize_scores(df).to_string(index=False))

    # 3) Cross-SA4 comparison
    comp = compare_sa4_scores_named(results)
    print("\n=== Cross-SA4 Comparison ===")
    print(comp.to_string(index=False))

if __name__ == "__main__":
    main()
