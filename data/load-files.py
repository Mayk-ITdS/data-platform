from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
from pandas import to_datetime

pd.set_option('display.max_columns', 87)
pd.set_option('display.max_rows', 100)
pd.set_option('display.width', 1000)
file_path = Path(__file__).resolve().parent / "NfipPoliciesV3.parquet"

parquet_file = pq.ParquetFile(file_path)
parquet_file2 = pq.ParquetFile(file_path)
first_batch = next(parquet_file.iter_batches(batch_size=3000))

df = first_batch.to_pandas()

# df = df.groupby(['reportedZipCode','nfipCommunityName','longitude','latitude'])
df = df.sort_values(by=['reportedZipCode','longitude','latitude','nfipCommunityName'])

print(df[df['originalConstructionDate'] == to_datetime('1910-06-01').date()])
print("\nMetadata:")
print(f"Liczba wszystkich rekordów: {parquet_file.metadata.num_rows}")
print(f"Liczba kolumn: {parquet_file.metadata.num_columns}")
print(f"Liczba row groups: {parquet_file.metadata.num_row_groups}")