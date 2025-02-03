import pandas as pd


interval = [i for i in range(0, 41, 1)]
interval = [str(i).zfill(2) for i in interval]


# [스키마]
column_names = [
    'transaction_hash', 'block_timestamp', 'send_count', 'receive_count',
    'send_value', 'receive_value', 'send_address', 'send_amount',
    'receive_address', 'receive_amount'
]


dfs = []

#### 1000 BTC 이상, every filter receive amount 10000의 거래
for i in interval:
    target = f'data/transaction/bitcoin_big_transaction_raw/transactions_0000000000{i}.csv'
    df = pd.read_csv(target, header=None)
    df.columns = column_names

    # block_timestamp을 datetime으로 변환 후 날짜만 추출
    df['block_date'] = pd.to_datetime(df['block_timestamp']).dt.date
    df['block_date'] = df['block_date'].apply(lambda x: x.strftime('%Y%m%d'))

    check_df = df[(df['block_date'] >= '20210101')&(df['block_date'] < '20220101')]

    if check_df.empty:
        continue
    else:
        dfs.append(df[['block_timestamp', 'block_date', 'transaction_hash', 'receive_amount']])

# 모든 데이터프레임을 하나로 결합
full_df = pd.concat(dfs, ignore_index=True)

full_df.sort_values(by='block_timestamp', inplace=True)

full_df = full_df.reset_index(drop=True)

full_df.to_csv('data/test/test_transaction_1208.csv', index=False)


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

