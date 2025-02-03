'''
price 원본 데이터 추출
'''

### Parquet 파일 읽고 Test 데이터 추출하기 ###
import pyarrow.parquet as pq
import pyarrow as pa
import os

# # Parquet 파일 읽기
# input_file = "data/price/BTCUSDT_spot_2017_2024_1s_klines.parquet"
# table = pq.read_table(input_file)

# # # # 타임스탬프를 숫자로 변환하여 비교하는 경우
# # # start_timestamp = 1609459200000  # 2021-10-01 00:00:00 UTC in milliseconds
# # # end_timestamp = 164099520000    # 2021-11-01 00:00:00 UTC in milliseconds


# timestamp_list = [1609459200, 1612137600, 1614556800, 1617235200, 1619827200, 1622505600, 1625097600, 1627776000, 1630454400, 1633046400, 1635724800, 1638316800, 1640995200]

# timestamp_list = [int(str(i)+'000') for i in timestamp_list]

# df = (table.to_pandas())

# iter = len(timestamp_list) - 1
# for i in range(iter):
#     test_df = df[(df['Open time'] >= timestamp_list[i]) & (df['Open time'] < timestamp_list[i+1])]
#     test_df.to_csv(f'data/test/test_price_1208_{i+1}.csv', index=False)



# '''
# price 데이터 시간 단위 전처리
# '''
# import pandas as pd
# import helpful_function as hf



# minute_df = pd.DataFrame()
# hour_df = pd.DataFrame()
# day_df = pd.DataFrame()
# for i in range(1, 13):
#     df = pd.read_csv(f'data/test/price/test_price_1208_21_{i}.csv')
#     df['Open datetime'] = df['Open time'].apply(hf.ts_to_datetime)
#     df.set_index('Open datetime', inplace=True)
#     df.index = pd.to_datetime(df.index)

#     temp_minute_df = hf.resample_df(df, 'T')
#     temp_hour_df = hf.resample_df(df, 'H')
#     temp_day_df = hf.resample_df(df, 'D')

#     minute_df = pd.concat([minute_df, temp_minute_df])
#     hour_df = pd.concat([hour_df, temp_hour_df])
#     day_df = pd.concat([day_df, temp_day_df])

# minute_df.to_csv('data/test/test_price_1208_21_minute.csv', index=False)
# hour_df.to_csv('data/test/test_price_1208_21_hour.csv', index=False)
# day_df.to_csv('data/test/test_price_1208_21_day.csv', index=False)




# '''
# Volume으로 Target 데이터 만들기
# '''
# import pandas as pd
# import helpful_function as hf


# day_df = pd.DataFrame()

# for i in range(1, 13):
#     df = pd.read_csv(f'data/test/price/test_price_1208_21_{i}.csv')
#     df['Open datetime'] = df['Open time'].apply(hf.ts_to_datetime)
#     df.set_index('Open datetime', inplace=True)
#     df.index = pd.to_datetime(df.index)
#     df['trade_per_volume'] = df['Volume'] / df['Number of trades']

#     df['trade_per_volume_flag'] = df['trade_per_volume'].apply(lambda x: 1 if x > 2 else 0)


#     period = 'D'

#     result_df = pd.DataFrame()

#     result_df['Open'] = df['Open'].resample(period).first()
#     result_df['High'] = df['High'].resample(period).max()
#     result_df['Low'] = df['Low'].resample(period).min()
#     result_df['Close'] = df['Close'].resample(period).last()
#     result_df['Volume'] = df['Volume'].resample(period).sum()
#     result_df['Quote asset volume'] = df['Quote asset volume'].resample(period).sum()
#     result_df['Number of trades'] = df['Number of trades'].resample(period).sum()
#     result_df['Taker buy base asset volume'] = df['Taker buy base asset volume'].resample(period).sum()
#     result_df['Taker buy quote asset volume'] = df['Taker buy quote asset volume'].resample(period).sum()
#     result_df['trade_per_volume'] = df['trade_per_volume'].resample(period).mean()
#     result_df['trade_per_volume_flag'] = df['trade_per_volume_flag'].resample(period).apply(lambda x: 1 if x.sum() > 0 else 0)

#     day_df = pd.concat([day_df, result_df])

# day_df.to_csv('data/test/test_price_1208_21_day_with_target.csv', index=True)

# print(temp_day_df)

# print(df['Volume'].describe())
# print(df['trade_per_volume'].describe())

# print(df['trade_per_volume_flag'].value_counts())


