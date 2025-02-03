import glob
import pandas as pd

# 환경설정
pd.set_option('display.max_columns', None)
files = glob.glob('data/transaction/wallet_ranking_address_transaction/*')
columns_list = ['transaction_hash', 'block_timestamp', 'send_count', 'receive_count', 'send_value', 'receive_value', 'send_address', 'send_amount', 'receive_address', 'receive_amount']

# 1 BTC = 1억 사토시
satoshi = 100000000

# wallet ranking address 추출
wallet_address = pd.read_csv("data/wallet_ranking/status/wallet_ranking_250103_test.csv")
# wallet_address_list = list(wallet_address['filtered_address'])

wallet_address['wallet_name_flag'] = wallet_address['filtered_wallet'].isna()
wallet_address['wallet_name_flag'] = wallet_address['wallet_name_flag'].apply(lambda x: 0 if x == True else 1)

wallet_address_list = list(wallet_address[wallet_address['wallet_name_flag']==0]['filtered_address'])[:100]

print('wallet address 추출')

### 빈 날짜 데이터프레임 만들기
# 1. 시작일과 종료일 정의
start_date = '2017-01-01'
end_date = '2025-01-09'

# 2. 일 단위(date_range)로 인덱스 생성
date_index = pd.date_range(start=start_date, end=end_date, freq='D')

# 3. 빈 DataFrame 생성 (인덱스만 존재)
result = pd.DataFrame(index=date_index)
result.index = result.index.astype(str)
result['transaction_count'] = 0
result['transaction_amount'] = 0
result['transaction_flag'] = 0

count = 0
for file in files:
    # wallet_ranking_address_transaction 데이터프레임 불러오기
    df = pd.read_csv(file, names=columns_list)

    df['block_timestamp_date'] = df['block_timestamp'].apply(lambda x: x[0:10])

    df['send_value'] = df['send_value']/satoshi
    df['receive_value'] = df['receive_value']/satoshi
    df['send_amount'] = df['send_amount']/satoshi
    df['receive_amount'] = df['receive_amount']/satoshi

    ## address 필터링 있을 경우
    df_send = df[df['send_address'].isin(wallet_address_list)]
    df_receive = df[df['receive_address'].isin(wallet_address_list)]
    df = pd.concat([df_send, df_receive], axis=0)


    # 일별 트랜잭션 수와 receive_value 총합 계산
    daily_stats = df.groupby('block_timestamp_date').agg(
        transaction_count=('transaction_hash', 'count'),
        transaction_amount=('receive_amount', 'sum')
    ).reset_index()

    # daily_stats의 인덱스를 'block_timestamp_date'로 설정
    daily_stats = daily_stats.set_index('block_timestamp_date')

    # daily_stats를 순회하면서 result에 값 더해주기
    for date, row in daily_stats.iterrows():
        # result의 인덱스는 문자열 형태이므로, 일치 여부를 확인합니다.
        if date in result.index:
            result.at[date, 'transaction_count'] += row['transaction_count']
            result.at[date, 'transaction_amount'] += row['transaction_amount']
    count += 1
    print(f'현재 {count} 업데이트 완료')
result.to_csv('wallet_ranking_target_without_named_top_100_250110.csv', index=True)

import pandas as pd
result = pd.read_csv('wallet_ranking_target_without_named_top_100_250110.csv')
result['transaction_flag'] = result['transaction_count'].apply(lambda x: 0 if x==0 else 1)
result.columns = ['date', 'transaction_count', 'transaction_amount', 'transaction_flag']
result.to_csv('wallet_ranking_target_without_named_top_100_250110.csv', index=False)
