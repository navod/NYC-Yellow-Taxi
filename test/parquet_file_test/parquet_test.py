
import pandas as pd
df = pd.read_parquet("data/raw/yellow_tripdata_2023-02.parquet")
df.shape
df.dtypes
df.isna().sum()
df.describe().T
df.payment_type.value_counts()
