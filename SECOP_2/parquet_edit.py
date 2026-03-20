import pandas as pd

df = pd.read_parquet('secop.parquet')
print(df.head())

print(df.dtypes)
print(len(df))

df.info()
