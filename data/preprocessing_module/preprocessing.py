import pandas as pd

interval = [i for i in range(0, 41, 1)]
interval = [str(i).zfill(2) for i in interval]

# [스키마]
column_names = [
    'transaction_hash', 'block_timestamp', 'send_count', 'receive_count',
    'send_value', 'receive_value', 'send_address', 'send_amount',
    'receive_address', 'receive_amount'
]


def filter_one_sender(df):
    result = df[df['send_count'] == 1]
    return result

def filter_not_loop(df):
    # send_address와 receive_address가 같은 transaction 모두 제거
    loop_df = df[df['send_address'] == df['receive_address']]
    loop_hash = loop_df['transaction_hash'].unique()
    result = df[~df['transaction_hash'].isin(loop_hash)]
    return result

def filter_threshold(df, threshold=1000, target='receive_amount'):
    ### target can be only
    # send_value
    # receive_value
    # send_amount
    # receive_amount

    satoshi = 100000000
    threshold_satoshi = threshold * satoshi

    result = df[df[target] > threshold_satoshi]

    return result


'''preprocessing.csv'''

### test_transaction 업데이트 먼저 하고 진행
pd.set_option('display.max_columns', None)
test_df = pd.read_csv('data/test/test_transaction_241208.csv')
test_df['block_datetime'] = test_df['block_timestamp'].apply(lambda x: x[:-4])

# 2015년 1월 1일부터 2024년 11월 19일까지의 시간(초)를 인덱스로 가지는 DataFrame 생성
time_range = pd.date_range(start='2021-01-01', end='2022-01-01', freq='S').astype(str)
time_range = time_range.to_frame(index=False)
time_range.columns = ['block_datetime']

# 날짜별로 데이터 집계
aggregated = test_df.groupby('block_datetime').agg(
    transaction_count=('transaction_hash', 'nunique'),
    transaction_amount=('receive_amount', 'sum')
).reset_index()


result_df = time_range.merge(aggregated, left_on='block_datetime', right_on='block_datetime', how='left')
result_df['transaction_count'] = result_df['transaction_count'].fillna(0).astype(int)
result_df['transaction_amount'] = result_df['transaction_amount'].fillna(0)
result_df['transaction_flag'] = result_df['transaction_count'].apply(lambda x: 1 if x > 0 else 0)
result_df = result_df.iloc[:-1, :]

result_df.to_csv('data/test/test_processing_1208.csv', index=False)



# # # target = 'data/transaction/bitcoin_big_transaction_raw/transactions_000000000040.csv'

# # # df = pd.read_csv(target, header=None)
# # # df.columns = column_names

# # # # block_timestamp을 datetime으로 변환 후 날짜만 추출
# # # df['block_date'] = pd.to_datetime(df['block_timestamp']).dt.date
# # # df['block_date'] = df['block_date'].apply(lambda x: x.strftime('%Y%m%d'))

# # # df = filter_one_sender(df)
# # # df = filter_not_loop(df)
# # # df = filter_threshold(df, threshold=1000, target='send_amount')

# # # pd.set_option('display.max_columns', None)
# # # print(df)


# # # # print(df['block_date'].unique())


# # # dfs = []

# # # #### 1000 BTC 이상의 거래
# # # for i in interval:
# # #     target = f'data/transaction/bitcoin_big_transaction_raw/transactions_0000000000{i}.csv'
# # #     df = pd.read_csv(target, header=None)
# # #     df.columns = column_names

# # #     # block_timestamp을 datetime으로 변환 후 날짜만 추출
# # #     df['block_date'] = pd.to_datetime(df['block_timestamp']).dt.date
# # #     df['block_date'] = df['block_date'].apply(lambda x: x.strftime('%Y%m%d'))

# # #     dfs.append(df[['block_date', 'transaction_hash', 'receive_amount']])

# # # # 모든 데이터프레임을 하나로 결합
# # # full_df = pd.concat(dfs, ignore_index=True)

# # # # 날짜별로 데이터 집계
# # # aggregated = full_df.groupby('block_date').agg(
# # #     transaction_count=('transaction_hash', 'nunique'),
# # #     transaction_amount=('receive_amount', 'sum')
# # # ).reset_index()

# # # # 2015년 1월 1일부터 2024년 11월 19일까지의 날짜를 인덱스로 가지는 DataFrame 생성
# # # date_range = pd.date_range(start='2015-01-01', end='2024-11-19')
# # # date_range = date_range.to_frame(index=False)
# # # date_range.columns = ['date']

# # # # 날짜 범위 데이터프레임 생성
# # # date_range = pd.DataFrame({'date': pd.date_range(start='2015-01-01', end='2024-11-19')})
# # # date_range['date'] = date_range['date'].apply(lambda x: x.strftime('%Y%m%d'))

# # # # 병합 및 누락된 값 채우기
# # # result_df = date_range.merge(aggregated, left_on='date', right_on='block_date', how='left').drop(columns='block_date')
# # # result_df['transaction_count'] = result_df['transaction_count'].fillna(0).astype(int)
# # # result_df['transaction_amount'] = result_df['transaction_amount'].fillna(0)


# # # # 결과 열 추가
# # # result_df['transaction_flag'] = result_df['transaction_count'].apply(lambda x: 1 if x > 0 else 0)

# # # # CSV로 저장
# # # result_df.to_csv('data/over_1000btc_result.csv', index=False)



# # dfs = []

# # #### 1000 BTC 이상, one sender filter의 거래
# # for i in interval:
# #     target = f'data/transaction/bitcoin_big_transaction_raw/transactions_0000000000{i}.csv'
# #     df = pd.read_csv(target, header=None)
# #     df.columns = column_names

# #     # block_timestamp을 datetime으로 변환 후 날짜만 추출
# #     df['block_date'] = pd.to_datetime(df['block_timestamp']).dt.date
# #     df['block_date'] = df['block_date'].apply(lambda x: x.strftime('%Y%m%d'))

# #     df = filter_one_sender(df)

# #     dfs.append(df[['block_date', 'transaction_hash', 'receive_amount']])

# # # 모든 데이터프레임을 하나로 결합
# # full_df = pd.concat(dfs, ignore_index=True)

# # # 날짜별로 데이터 집계
# # aggregated = full_df.groupby('block_date').agg(
# #     transaction_count=('transaction_hash', 'nunique'),
# #     transaction_amount=('receive_amount', 'sum')
# # ).reset_index()

# # # 2015년 1월 1일부터 2024년 11월 19일까지의 날짜를 인덱스로 가지는 DataFrame 생성
# # date_range = pd.date_range(start='2015-01-01', end='2024-11-19')
# # date_range = date_range.to_frame(index=False)
# # date_range.columns = ['date']

# # # 날짜 범위 데이터프레임 생성
# # date_range = pd.DataFrame({'date': pd.date_range(start='2015-01-01', end='2024-11-19')})
# # date_range['date'] = date_range['date'].apply(lambda x: x.strftime('%Y%m%d'))

# # # 병합 및 누락된 값 채우기
# # result_df = date_range.merge(aggregated, left_on='date', right_on='block_date', how='left').drop(columns='block_date')
# # result_df['transaction_count'] = result_df['transaction_count'].fillna(0).astype(int)
# # result_df['transaction_amount'] = result_df['transaction_amount'].fillna(0)


# # # 결과 열 추가
# # result_df['transaction_flag'] = result_df['transaction_count'].apply(lambda x: 1 if x > 0 else 0)

# # # CSV로 저장
# # result_df.to_csv('data/one_sender_result.csv', index=False)




# # dfs = []

# # #### 1000 BTC 이상, not loop의 거래
# # for i in interval:
# #     target = f'data/transaction/bitcoin_big_transaction_raw/transactions_0000000000{i}.csv'
# #     df = pd.read_csv(target, header=None)
# #     df.columns = column_names

# #     # block_timestamp을 datetime으로 변환 후 날짜만 추출
# #     df['block_date'] = pd.to_datetime(df['block_timestamp']).dt.date
# #     df['block_date'] = df['block_date'].apply(lambda x: x.strftime('%Y%m%d'))

# #     df = filter_not_loop(df)

# #     dfs.append(df[['block_date', 'transaction_hash', 'receive_amount']])

# # # 모든 데이터프레임을 하나로 결합
# # full_df = pd.concat(dfs, ignore_index=True)

# # # 날짜별로 데이터 집계
# # aggregated = full_df.groupby('block_date').agg(
# #     transaction_count=('transaction_hash', 'nunique'),
# #     transaction_amount=('receive_amount', 'sum')
# # ).reset_index()

# # # 2015년 1월 1일부터 2024년 11월 19일까지의 날짜를 인덱스로 가지는 DataFrame 생성
# # date_range = pd.date_range(start='2015-01-01', end='2024-11-19')
# # date_range = date_range.to_frame(index=False)
# # date_range.columns = ['date']

# # # 날짜 범위 데이터프레임 생성
# # date_range = pd.DataFrame({'date': pd.date_range(start='2015-01-01', end='2024-11-19')})
# # date_range['date'] = date_range['date'].apply(lambda x: x.strftime('%Y%m%d'))

# # # 병합 및 누락된 값 채우기
# # result_df = date_range.merge(aggregated, left_on='date', right_on='block_date', how='left').drop(columns='block_date')
# # result_df['transaction_count'] = result_df['transaction_count'].fillna(0).astype(int)
# # result_df['transaction_amount'] = result_df['transaction_amount'].fillna(0)


# # # 결과 열 추가
# # result_df['transaction_flag'] = result_df['transaction_count'].apply(lambda x: 1 if x > 0 else 0)

# # # CSV로 저장
# # result_df.to_csv('data/not_loop_result.csv', index=False)



# # dfs = []

# # #### 1000 BTC 이상, receive amount 1000의 거래
# # for i in interval:
# #     target = f'data/transaction/bitcoin_big_transaction_raw/transactions_0000000000{i}.csv'
# #     df = pd.read_csv(target, header=None)
# #     df.columns = column_names

# #     # block_timestamp을 datetime으로 변환 후 날짜만 추출
# #     df['block_date'] = pd.to_datetime(df['block_timestamp']).dt.date
# #     df['block_date'] = df['block_date'].apply(lambda x: x.strftime('%Y%m%d'))

# #     df = filter_threshold(df)

# #     dfs.append(df[['block_date', 'transaction_hash', 'receive_amount']])

# # # 모든 데이터프레임을 하나로 결합
# # full_df = pd.concat(dfs, ignore_index=True)

# # # 날짜별로 데이터 집계
# # aggregated = full_df.groupby('block_date').agg(
# #     transaction_count=('transaction_hash', 'nunique'),
# #     transaction_amount=('receive_amount', 'sum')
# # ).reset_index()

# # # 2015년 1월 1일부터 2024년 11월 19일까지의 날짜를 인덱스로 가지는 DataFrame 생성
# # date_range = pd.date_range(start='2015-01-01', end='2024-11-19')
# # date_range = date_range.to_frame(index=False)
# # date_range.columns = ['date']

# # # 날짜 범위 데이터프레임 생성
# # date_range = pd.DataFrame({'date': pd.date_range(start='2015-01-01', end='2024-11-19')})
# # date_range['date'] = date_range['date'].apply(lambda x: x.strftime('%Y%m%d'))

# # # 병합 및 누락된 값 채우기
# # result_df = date_range.merge(aggregated, left_on='date', right_on='block_date', how='left').drop(columns='block_date')
# # result_df['transaction_count'] = result_df['transaction_count'].fillna(0).astype(int)
# # result_df['transaction_amount'] = result_df['transaction_amount'].fillna(0)


# # # 결과 열 추가
# # result_df['transaction_flag'] = result_df['transaction_count'].apply(lambda x: 1 if x > 0 else 0)

# # # CSV로 저장
# # result_df.to_csv('data/receive_amount_1000_result.csv', index=False)



# # dfs = []

# # #### 1000 BTC 이상, receive amount 3000의 거래
# # for i in interval:
# #     target = f'data/transaction/bitcoin_big_transaction_raw/transactions_0000000000{i}.csv'
# #     df = pd.read_csv(target, header=None)
# #     df.columns = column_names

# #     # block_timestamp을 datetime으로 변환 후 날짜만 추출
# #     df['block_date'] = pd.to_datetime(df['block_timestamp']).dt.date
# #     df['block_date'] = df['block_date'].apply(lambda x: x.strftime('%Y%m%d'))

# #     df = filter_threshold(df, 3000)

# #     dfs.append(df[['block_date', 'transaction_hash', 'receive_amount']])

# # # 모든 데이터프레임을 하나로 결합
# # full_df = pd.concat(dfs, ignore_index=True)

# # # 날짜별로 데이터 집계
# # aggregated = full_df.groupby('block_date').agg(
# #     transaction_count=('transaction_hash', 'nunique'),
# #     transaction_amount=('receive_amount', 'sum')
# # ).reset_index()

# # # 2015년 1월 1일부터 2024년 11월 19일까지의 날짜를 인덱스로 가지는 DataFrame 생성
# # date_range = pd.date_range(start='2015-01-01', end='2024-11-19')
# # date_range = date_range.to_frame(index=False)
# # date_range.columns = ['date']

# # # 날짜 범위 데이터프레임 생성
# # date_range = pd.DataFrame({'date': pd.date_range(start='2015-01-01', end='2024-11-19')})
# # date_range['date'] = date_range['date'].apply(lambda x: x.strftime('%Y%m%d'))

# # # 병합 및 누락된 값 채우기
# # result_df = date_range.merge(aggregated, left_on='date', right_on='block_date', how='left').drop(columns='block_date')
# # result_df['transaction_count'] = result_df['transaction_count'].fillna(0).astype(int)
# # result_df['transaction_amount'] = result_df['transaction_amount'].fillna(0)


# # # 결과 열 추가
# # result_df['transaction_flag'] = result_df['transaction_count'].apply(lambda x: 1 if x > 0 else 0)

# # # CSV로 저장
# # result_df.to_csv('data/receive_amount_3000_result.csv', index=False)



# # dfs = []

# # #### 1000 BTC 이상, one sender, not loop의 거래
# # for i in interval:
# #     target = f'data/transaction/bitcoin_big_transaction_raw/transactions_0000000000{i}.csv'
# #     df = pd.read_csv(target, header=None)
# #     df.columns = column_names

# #     # block_timestamp을 datetime으로 변환 후 날짜만 추출
# #     df['block_date'] = pd.to_datetime(df['block_timestamp']).dt.date
# #     df['block_date'] = df['block_date'].apply(lambda x: x.strftime('%Y%m%d'))

# #     df = filter_one_sender(df)
# #     df = filter_not_loop(df)

# #     dfs.append(df[['block_date', 'transaction_hash', 'receive_amount']])

# # # 모든 데이터프레임을 하나로 결합
# # full_df = pd.concat(dfs, ignore_index=True)

# # # 날짜별로 데이터 집계
# # aggregated = full_df.groupby('block_date').agg(
# #     transaction_count=('transaction_hash', 'nunique'),
# #     transaction_amount=('receive_amount', 'sum')
# # ).reset_index()

# # # 2015년 1월 1일부터 2024년 11월 19일까지의 날짜를 인덱스로 가지는 DataFrame 생성
# # date_range = pd.date_range(start='2015-01-01', end='2024-11-19')
# # date_range = date_range.to_frame(index=False)
# # date_range.columns = ['date']

# # # 날짜 범위 데이터프레임 생성
# # date_range = pd.DataFrame({'date': pd.date_range(start='2015-01-01', end='2024-11-19')})
# # date_range['date'] = date_range['date'].apply(lambda x: x.strftime('%Y%m%d'))

# # # 병합 및 누락된 값 채우기
# # result_df = date_range.merge(aggregated, left_on='date', right_on='block_date', how='left').drop(columns='block_date')
# # result_df['transaction_count'] = result_df['transaction_count'].fillna(0).astype(int)
# # result_df['transaction_amount'] = result_df['transaction_amount'].fillna(0)


# # # 결과 열 추가
# # result_df['transaction_flag'] = result_df['transaction_count'].apply(lambda x: 1 if x > 0 else 0)

# # # CSV로 저장
# # result_df.to_csv('data/one_sender_not_loop_result.csv', index=False)



# ################# 여기서부터 dfs 없었음

# dfs = []


# #### 1000 BTC 이상, one sender, receive amount 3000의 거래
# for i in interval:
#     target = f'data/transaction/bitcoin_big_transaction_raw/transactions_0000000000{i}.csv'
#     df = pd.read_csv(target, header=None)
#     df.columns = column_names

#     # block_timestamp을 datetime으로 변환 후 날짜만 추출
#     df['block_date'] = pd.to_datetime(df['block_timestamp']).dt.date
#     df['block_date'] = df['block_date'].apply(lambda x: x.strftime('%Y%m%d'))

#     df = filter_one_sender(df)
#     df = filter_threshold(df, 3000, 'receive_amount')

#     dfs.append(df[['block_date', 'transaction_hash', 'receive_amount']])

# # 모든 데이터프레임을 하나로 결합
# full_df = pd.concat(dfs, ignore_index=True)

# # 날짜별로 데이터 집계
# aggregated = full_df.groupby('block_date').agg(
#     transaction_count=('transaction_hash', 'nunique'),
#     transaction_amount=('receive_amount', 'sum')
# ).reset_index()

# # 2015년 1월 1일부터 2024년 11월 19일까지의 날짜를 인덱스로 가지는 DataFrame 생성
# date_range = pd.date_range(start='2015-01-01', end='2024-11-19')
# date_range = date_range.to_frame(index=False)
# date_range.columns = ['date']

# # 날짜 범위 데이터프레임 생성
# date_range = pd.DataFrame({'date': pd.date_range(start='2015-01-01', end='2024-11-19')})
# date_range['date'] = date_range['date'].apply(lambda x: x.strftime('%Y%m%d'))

# # 병합 및 누락된 값 채우기
# result_df = date_range.merge(aggregated, left_on='date', right_on='block_date', how='left').drop(columns='block_date')
# result_df['transaction_count'] = result_df['transaction_count'].fillna(0).astype(int)
# result_df['transaction_amount'] = result_df['transaction_amount'].fillna(0)


# # 결과 열 추가
# result_df['transaction_flag'] = result_df['transaction_count'].apply(lambda x: 1 if x > 0 else 0)

# # CSV로 저장
# result_df.to_csv('data/one_sender_receive_amount_3000_result.csv', index=False)


# dfs = []

# #### 1000 BTC 이상, receive amount 3000, not loop, one sender의 거래
# for i in interval:
#     target = f'data/transaction/bitcoin_big_transaction_raw/transactions_0000000000{i}.csv'
#     df = pd.read_csv(target, header=None)
#     df.columns = column_names

#     # block_timestamp을 datetime으로 변환 후 날짜만 추출
#     df['block_date'] = pd.to_datetime(df['block_timestamp']).dt.date
#     df['block_date'] = df['block_date'].apply(lambda x: x.strftime('%Y%m%d'))

#     df = filter_threshold(df, 3000, 'receive_amount')
#     df = filter_not_loop(df)
#     df = filter_one_sender(df)

#     dfs.append(df[['block_date', 'transaction_hash', 'receive_amount']])

# # 모든 데이터프레임을 하나로 결합
# full_df = pd.concat(dfs, ignore_index=True)

# # 날짜별로 데이터 집계
# aggregated = full_df.groupby('block_date').agg(
#     transaction_count=('transaction_hash', 'nunique'),
#     transaction_amount=('receive_amount', 'sum')
# ).reset_index()

# # 2015년 1월 1일부터 2024년 11월 19일까지의 날짜를 인덱스로 가지는 DataFrame 생성
# date_range = pd.date_range(start='2015-01-01', end='2024-11-19')
# date_range = date_range.to_frame(index=False)
# date_range.columns = ['date']

# # 날짜 범위 데이터프레임 생성
# date_range = pd.DataFrame({'date': pd.date_range(start='2015-01-01', end='2024-11-19')})
# date_range['date'] = date_range['date'].apply(lambda x: x.strftime('%Y%m%d'))

# # 병합 및 누락된 값 채우기
# result_df = date_range.merge(aggregated, left_on='date', right_on='block_date', how='left').drop(columns='block_date')
# result_df['transaction_count'] = result_df['transaction_count'].fillna(0).astype(int)
# result_df['transaction_amount'] = result_df['transaction_amount'].fillna(0)


# # 결과 열 추가
# result_df['transaction_flag'] = result_df['transaction_count'].apply(lambda x: 1 if x > 0 else 0)

# # CSV로 저장
# result_df.to_csv('data/every_filter_3000_result.csv', index=False)


# dfs = []


# #### 1000 BTC 이상, one sender, receive amount 1000의 거래
# for i in interval:
#     target = f'data/transaction/bitcoin_big_transaction_raw/transactions_0000000000{i}.csv'
#     df = pd.read_csv(target, header=None)
#     df.columns = column_names

#     # block_timestamp을 datetime으로 변환 후 날짜만 추출
#     df['block_date'] = pd.to_datetime(df['block_timestamp']).dt.date
#     df['block_date'] = df['block_date'].apply(lambda x: x.strftime('%Y%m%d'))

#     df = filter_one_sender(df)
#     df = filter_threshold(df, 1000, 'receive_amount')
#     df = filter_not_loop(df)

#     dfs.append(df[['block_date', 'transaction_hash', 'receive_amount']])

# # 모든 데이터프레임을 하나로 결합
# full_df = pd.concat(dfs, ignore_index=True)

# # 날짜별로 데이터 집계
# aggregated = full_df.groupby('block_date').agg(
#     transaction_count=('transaction_hash', 'nunique'),
#     transaction_amount=('receive_amount', 'sum')
# ).reset_index()

# # 2015년 1월 1일부터 2024년 11월 19일까지의 날짜를 인덱스로 가지는 DataFrame 생성
# date_range = pd.date_range(start='2015-01-01', end='2024-11-19')
# date_range = date_range.to_frame(index=False)
# date_range.columns = ['date']

# # 날짜 범위 데이터프레임 생성
# date_range = pd.DataFrame({'date': pd.date_range(start='2015-01-01', end='2024-11-19')})
# date_range['date'] = date_range['date'].apply(lambda x: x.strftime('%Y%m%d'))

# # 병합 및 누락된 값 채우기
# result_df = date_range.merge(aggregated, left_on='date', right_on='block_date', how='left').drop(columns='block_date')
# result_df['transaction_count'] = result_df['transaction_count'].fillna(0).astype(int)
# result_df['transaction_amount'] = result_df['transaction_amount'].fillna(0)


# # 결과 열 추가
# result_df['transaction_flag'] = result_df['transaction_count'].apply(lambda x: 1 if x > 0 else 0)

# # CSV로 저장
# result_df.to_csv('data/every_filter_1000_result.csv', index=False)







# dfs = []


# #### 1000 BTC 이상, receive amount 10000의 거래
# for i in interval:
#     target = f'data/transaction/bitcoin_big_transaction_raw/transactions_0000000000{i}.csv'
#     df = pd.read_csv(target, header=None)
#     df.columns = column_names

#     # block_timestamp을 datetime으로 변환 후 날짜만 추출
#     df['block_date'] = pd.to_datetime(df['block_timestamp']).dt.date
#     df['block_date'] = df['block_date'].apply(lambda x: x.strftime('%Y%m%d'))

#     df = filter_threshold(df, 10000, 'receive_amount')

#     dfs.append(df[['block_date', 'transaction_hash', 'receive_amount']])

# # 모든 데이터프레임을 하나로 결합
# full_df = pd.concat(dfs, ignore_index=True)

# # 날짜별로 데이터 집계
# aggregated = full_df.groupby('block_date').agg(
#     transaction_count=('transaction_hash', 'nunique'),
#     transaction_amount=('receive_amount', 'sum')
# ).reset_index()

# # 2015년 1월 1일부터 2024년 11월 19일까지의 날짜를 인덱스로 가지는 DataFrame 생성
# date_range = pd.date_range(start='2015-01-01', end='2024-11-19')
# date_range = date_range.to_frame(index=False)
# date_range.columns = ['date']

# # 날짜 범위 데이터프레임 생성
# date_range = pd.DataFrame({'date': pd.date_range(start='2015-01-01', end='2024-11-19')})
# date_range['date'] = date_range['date'].apply(lambda x: x.strftime('%Y%m%d'))

# # 병합 및 누락된 값 채우기
# result_df = date_range.merge(aggregated, left_on='date', right_on='block_date', how='left').drop(columns='block_date')
# result_df['transaction_count'] = result_df['transaction_count'].fillna(0).astype(int)
# result_df['transaction_amount'] = result_df['transaction_amount'].fillna(0)


# # 결과 열 추가
# result_df['transaction_flag'] = result_df['transaction_count'].apply(lambda x: 1 if x > 0 else 0)

# # CSV로 저장
# result_df.to_csv('data/receive_amount_10000_result.csv', index=False)


# dfs = []

# #### 1000 BTC 이상, receive amount 10000, one sender의 거래
# for i in interval:
#     target = f'data/transaction/bitcoin_big_transaction_raw/transactions_0000000000{i}.csv'
#     df = pd.read_csv(target, header=None)
#     df.columns = column_names

#     # block_timestamp을 datetime으로 변환 후 날짜만 추출
#     df['block_date'] = pd.to_datetime(df['block_timestamp']).dt.date
#     df['block_date'] = df['block_date'].apply(lambda x: x.strftime('%Y%m%d'))

#     df = filter_threshold(df, 10000, 'receive_amount')
#     df = filter_one_sender(df)

#     dfs.append(df[['block_date', 'transaction_hash', 'receive_amount']])

# # 모든 데이터프레임을 하나로 결합
# full_df = pd.concat(dfs, ignore_index=True)

# # 날짜별로 데이터 집계
# aggregated = full_df.groupby('block_date').agg(
#     transaction_count=('transaction_hash', 'nunique'),
#     transaction_amount=('receive_amount', 'sum')
# ).reset_index()

# # 2015년 1월 1일부터 2024년 11월 19일까지의 날짜를 인덱스로 가지는 DataFrame 생성
# date_range = pd.date_range(start='2015-01-01', end='2024-11-19')
# date_range = date_range.to_frame(index=False)
# date_range.columns = ['date']

# # 날짜 범위 데이터프레임 생성
# date_range = pd.DataFrame({'date': pd.date_range(start='2015-01-01', end='2024-11-19')})
# date_range['date'] = date_range['date'].apply(lambda x: x.strftime('%Y%m%d'))

# # 병합 및 누락된 값 채우기
# result_df = date_range.merge(aggregated, left_on='date', right_on='block_date', how='left').drop(columns='block_date')
# result_df['transaction_count'] = result_df['transaction_count'].fillna(0).astype(int)
# result_df['transaction_amount'] = result_df['transaction_amount'].fillna(0)


# # 결과 열 추가
# result_df['transaction_flag'] = result_df['transaction_count'].apply(lambda x: 1 if x > 0 else 0)

# # CSV로 저장
# result_df.to_csv('data/one_sender_receive_amount_10000_result.csv', index=False)


# dfs = []


# #### 1000 BTC 이상, not loop, receive amount 10000의 거래
# for i in interval:
#     target = f'data/transaction/bitcoin_big_transaction_raw/transactions_0000000000{i}.csv'
#     df = pd.read_csv(target, header=None)
#     df.columns = column_names

#     # block_timestamp을 datetime으로 변환 후 날짜만 추출
#     df['block_date'] = pd.to_datetime(df['block_timestamp']).dt.date
#     df['block_date'] = df['block_date'].apply(lambda x: x.strftime('%Y%m%d'))

#     df = filter_threshold(df, 10000, 'receive_amount')
#     df = filter_not_loop(df)

#     dfs.append(df[['block_date', 'transaction_hash', 'receive_amount']])

# # 모든 데이터프레임을 하나로 결합
# full_df = pd.concat(dfs, ignore_index=True)

# # 날짜별로 데이터 집계
# aggregated = full_df.groupby('block_date').agg(
#     transaction_count=('transaction_hash', 'nunique'),
#     transaction_amount=('receive_amount', 'sum')
# ).reset_index()

# # 2015년 1월 1일부터 2024년 11월 19일까지의 날짜를 인덱스로 가지는 DataFrame 생성
# date_range = pd.date_range(start='2015-01-01', end='2024-11-19')
# date_range = date_range.to_frame(index=False)
# date_range.columns = ['date']

# # 날짜 범위 데이터프레임 생성
# date_range = pd.DataFrame({'date': pd.date_range(start='2015-01-01', end='2024-11-19')})
# date_range['date'] = date_range['date'].apply(lambda x: x.strftime('%Y%m%d'))

# # 병합 및 누락된 값 채우기
# result_df = date_range.merge(aggregated, left_on='date', right_on='block_date', how='left').drop(columns='block_date')
# result_df['transaction_count'] = result_df['transaction_count'].fillna(0).astype(int)
# result_df['transaction_amount'] = result_df['transaction_amount'].fillna(0)


# # 결과 열 추가
# result_df['transaction_flag'] = result_df['transaction_count'].apply(lambda x: 1 if x > 0 else 0)

# # CSV로 저장
# result_df.to_csv('data/not_loop_10000_result.csv', index=False)





# dfs = []


# #### 1000 BTC 이상, every filter receive amount 10000의 거래
# for i in interval:
#     target = f'data/transaction/bitcoin_big_transaction_raw/transactions_0000000000{i}.csv'
#     df = pd.read_csv(target, header=None)
#     df.columns = column_names

#     # block_timestamp을 datetime으로 변환 후 날짜만 추출
#     df['block_date'] = pd.to_datetime(df['block_timestamp']).dt.date
#     df['block_date'] = df['block_date'].apply(lambda x: x.strftime('%Y%m%d'))

#     df = filter_one_sender(df)
#     df = filter_threshold(df, 100000, 'receive_amount')
#     df = filter_not_loop(df)

#     dfs.append(df[['block_date', 'transaction_hash', 'receive_amount']])

# # 모든 데이터프레임을 하나로 결합
# full_df = pd.concat(dfs, ignore_index=True)

# # 날짜별로 데이터 집계
# aggregated = full_df.groupby('block_date').agg(
#     transaction_count=('transaction_hash', 'nunique'),
#     transaction_amount=('receive_amount', 'sum')
# ).reset_index()

# # 2015년 1월 1일부터 2024년 11월 19일까지의 날짜를 인덱스로 가지는 DataFrame 생성
# date_range = pd.date_range(start='2015-01-01', end='2024-11-19')
# date_range = date_range.to_frame(index=False)
# date_range.columns = ['date']

# # 날짜 범위 데이터프레임 생성
# date_range = pd.DataFrame({'date': pd.date_range(start='2015-01-01', end='2024-11-19')})
# date_range['date'] = date_range['date'].apply(lambda x: x.strftime('%Y%m%d'))

# # 병합 및 누락된 값 채우기
# result_df = date_range.merge(aggregated, left_on='date', right_on='block_date', how='left').drop(columns='block_date')
# result_df['transaction_count'] = result_df['transaction_count'].fillna(0).astype(int)
# result_df['transaction_amount'] = result_df['transaction_amount'].fillna(0)


# # 결과 열 추가
# result_df['transaction_flag'] = result_df['transaction_count'].apply(lambda x: 1 if x > 0 else 0)

# # CSV로 저장
# result_df.to_csv('data/every_filter_10000_result.csv', index=False)

