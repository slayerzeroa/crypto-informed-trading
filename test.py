import pandas as pd

df = pd.read_parquet('data/price/BTCUSDT_spot_2017_2024_1s_klines.parquet')

print(df.head())