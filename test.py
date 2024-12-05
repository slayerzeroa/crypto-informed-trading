# ### Parquet 파일 읽고 Test 데이터 추출하기 ###
# import pyarrow.parquet as pq
# import pyarrow as pa
# import os

# # Parquet 파일 읽기
# input_file = "data/price/BTCUSDT_spot_2017_2024_1s_klines.parquet"
# table = pq.read_table(input_file)

# # 타임스탬프를 숫자로 변환하여 비교하는 경우
# start_timestamp = 1633046400000  # 2021-10-01 00:00:00 UTC in milliseconds
# end_timestamp = 1635724800000    # 2021-11-01 00:00:00 UTC in milliseconds
# # 10개 행만 출력
# df = (table.to_pandas())

# test_df = df[(df['Open time'] >= start_timestamp) & (df['Open time'] < end_timestamp)]

# test_df.to_csv('test.csv', index=False)



import pandas as pd
import data.helpful_function as hf


pd.set_option('display.max_columns', None)
df = pd.read_csv('data/price/BTCUSDT-spot-2021-10.csv')
# print(df.head())


df['Open datetime'] = df['Open time'].apply(hf.ts_to_datetime)
df.set_index('Open datetime', inplace=True)

df.index = pd.to_datetime(df.index)

minute_df = hf.resample_df(df, 'T')
hour_df = hf.resample_df(df, 'H')
day_df = hf.resample_df(df, 'D')

minute_df.to_csv('data/price/BTCUSDT-spot-2021-10_minute.csv', index=False)
hour_df.to_csv('data/price/BTCUSDT-spot-2021-10_hour.csv', index=False)
day_df.to_csv('data/price/BTCUSDT-spot-2021-10_day.csv', index=False)
print('Done!')