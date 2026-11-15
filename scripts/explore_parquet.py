"""Quick look at a raw TLC parquet file (not a test; run manually).

    .venv/bin/python scripts/explore_parquet.py [path]
"""

import sys

import pandas as pd


def main(path: str = "data/raw/yellow_tripdata_2023-02.parquet") -> None:
    df = pd.read_parquet(path)
    print(df.shape)
    print(df.dtypes)
    print(df.isna().sum())
    print(df.describe().T)
    print(df.payment_type.value_counts())


if __name__ == "__main__":
    main(*sys.argv[1:2])
