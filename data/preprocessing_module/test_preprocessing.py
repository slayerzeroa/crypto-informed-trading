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


# '''preprocessing.csv'''

# ### test_transaction 업데이트 먼저 하고 진행
# pd.set_option('display.max_columns', None)
# test_df = pd.read_csv('data/test/test_transaction_241208.csv')
# test_df['block_datetime'] = test_df['block_timestamp'].apply(lambda x: x[:-4])

# # 2015년 1월 1일부터 2024년 11월 19일까지의 시간(초)를 인덱스로 가지는 DataFrame 생성
# time_range = pd.date_range(start='2021-01-01', end='2022-01-01', freq='S').astype(str)
# time_range = time_range.to_frame(index=False)
# time_range.columns = ['block_datetime']

# # 날짜별로 데이터 집계
# aggregated = test_df.groupby('block_datetime').agg(
#     transaction_count=('transaction_hash', 'nunique'),
#     transaction_amount=('receive_amount', 'sum')
# ).reset_index()


# result_df = time_range.merge(aggregated, left_on='block_datetime', right_on='block_datetime', how='left')
# result_df['transaction_count'] = result_df['transaction_count'].fillna(0).astype(int)
# result_df['transaction_amount'] = result_df['transaction_amount'].fillna(0)
# result_df['transaction_flag'] = result_df['transaction_count'].apply(lambda x: 1 if x > 0 else 0)
# result_df = result_df.iloc[:-1, :]

# result_df.to_csv('data/test/test_preprocessing_1208.csv', index=False)


# '''target.csv'''

# df = pd.read_csv('data/target/not_loop_10000_result.csv')
# df = df[(df['date'] >= 20210101) & (df['date'] < 20220101)]
# df.to_csv('data/test/test_target_1208.csv', index=False)