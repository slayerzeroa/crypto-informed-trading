import pandas as pd
import glob

# [스키마]
column_names = [
    'transaction_hash', 'block_timestamp', 'send_count', 'receive_count',
    'send_value', 'receive_value', 'send_address', 'send_amount',
    'receive_address', 'receive_amount'
]

address_list = []

files = glob.glob("data/transaction/bitcoin_big_transaction_raw/*.csv")

satoshi = 100000000

for file in files:
    df = pd.read_csv(file, names=column_names)
    send_df = df[(df['send_amount'] > (10000 * satoshi))]
    receive_df = df[(df['send_amount'] > (10000 * satoshi))]

    address_list.extend(list(send_df['send_address']))
    address_list.extend(list(receive_df['receive_address']))

    print(f'{file} address 추출 완료')


address_list = list(set(address_list))

(pd.DataFrame([address_list]).T).to_csv('big_transaction_address_10000.csv', index=False, header=False)