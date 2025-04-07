import requests
import datetime
import pandas as pd
import time
from dateutil.relativedelta import relativedelta

def ts2date(ts):
    return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)

# API 엔드포인트와 매개변수 설정
chart_name = 'transactions-per-second'
chart_name = 'blocks-size'


'''
Chart name list

Total Circulating Bitcoin           - total-bitcoins
Market Price (USD)                  - market-price
Market Capitalization (USD)        - market-cap 
Exchange Trade Volume (USD)       - trade-volume
Blockchain Size (MB)				- blocks-size
Average Block Size (MB)			- avg-block-size
Average Transactions Per Block		- n-transactions-per-block
Average Payments Per Block			- n-payments-per-block
Total Number of Transactions		- n-transactions-total
Median Confirmation Time			- median-confirmation-time
Average Confirmation Time			- avg-confirmation-time
Total Hash Rate (TH/s)				- hash-rate
Hashrate Distribution				- pools
Hashrate Distribution Over Time		- pools-timeseries
Network Difficulty				- difficulty
Miners Revenue (USD)				- miners-revenue
Total Transaction Fees (BTC)			- transaction-fees
Total Transaction Fees (USD)			- transaction-fees-usd
Fees Per Transaction (USD)			- fees-usd-per-transaction
Cost % of Transaction Volume		- cost-per-transaction-percent
Cost Per Transaction				- cost-per-transaction
Unique Addresses Used			- n-unique-addresses
Confirmed Transactions Per Day		- n-transactions
Confirmed Payments Per Day			- n-payments
Transaction Rate Per Second			- transactions-per-second
Output Value Per Day				- output-volume
Mempool Transaction Count			- mempool-count
Mempool Size Growth				- mempool-growth
Mempool Size (Bytes)				- mempool-size
Mempool Bytes Per Fee Level			- mempool-state-by-fee-level
Unspent Transaction Outputs			- utxo-count
Transactions Excluding Popular Addresses	- n-transactions-excluding-popular
Estimated Transaction Value (BTC)		- estimated-transaction-volume
Estimated Transaction Value (USD)		- estimated-transaction-volume-usd
'''

chart_name_list = [
    'total-bitcoins',
    'market-price',
    'market-cap',
    'trade-volume',
    'blocks-size',
    'avg-block-size',
    'n-transactions-per-block',
    'n-payments-per-block',
    'n-transactions-total',
    'median-confirmation-time',
    'avg-confirmation-time',
    'hash-rate',
    'difficulty',
    'miners-revenue',
    'transaction-fees',
    'transaction-fees-usd',
    'fees-usd-per-transaction',
    'cost-per-transaction-percent',
    'cost-per-transaction',
    'n-unique-addresses',
    'n-transactions',
    'n-payments',
    'transactions-per-second',
    'output-volume',
    'mempool-count',
    'mempool-growth',
    'mempool-size',
    'mempool-state-by-fee-level',
    'utxo-count',
    'n-transactions-excluding-popular',
    'estimated-transaction-volume',
    'estimated-transaction-volume-usd'
]

def get_chart_data(chart_name, start: str, timespan:str='1year'):

    '''
    params input example
    start='2010-01-01'
    timespan='1years'
    '''

    params = {
        'timespan': '1year',
        'start': start,
        'format': 'json',
        'sampled': 'true',
    }

    # API 요청 보내기
    url = f'https://api.blockchain.info/charts/{chart_name}?timespan={timespan}&sampled=true&start={start}&metadata=false&daysAverageString=1d&cors=true&format=json'
    response = requests.get(url, params=params)

    # 응답 확인
    if response.status_code == 200:
        data = response.json()

        if len(data['values']) < 3:
            return response.status_code, pd.DataFrame()
        

        else:
            df = pd.DataFrame(data['values'])
            df['timestamp'] = df['x'].apply(ts2date)

            df = df.iloc[:-1, :]
            df = df[['timestamp', 'y']]
            df.columns = ['timestamp', chart_name]

            return response.status_code, df
    
    else:
        return response.status_code, pd.DataFrame()

# df = get_chart_data('processing mempool-count', '2021-01-01')
# print(df)


# url = 'https://api.blockchain.info/charts/n-payments-per-block?timespan=1year&sampled=true&start=2018-01-01&metadata=false&daysAverageString=1d&cors=true&format=json'
# url = 'https://api.blockchain.info/charts/avg-confirmation-time?timespan=1year&sampled=true&start=2016-01-01&metadata=false&daysAverageString=1d&cors=true&format=json'
# url = 'https://api.blockchain.info/charts/fees-usd-per-transaction?timespan=1year&sampled=true&start=2009-01-01&metadata=false&daysAverageString=1d&cors=true&format=json'
# url = 'https://api.blockchain.info/charts/cost-per-transaction-percent?timespan=1year&sampled=true&start=2010-01-01&metadata=false&daysAverageString=1d&cors=true&format=json'
# url = 'https://api.blockchain.info/charts/n-payments?timespan=1year&sampled=true&start=2018-01-01&metadata=false&daysAverageString=1d&cors=true&format=json'
# url = 'https://api.blockchain.info/charts/transactions-per-second?timespan=1year&sampled=true&start=2016-01-01&metadata=false&daysAverageString=1d&cors=true&format=json'
# url = 'https://api.blockchain.info/charts/mempool-count?timespan=1year&sampled=true&start=2016-01-01&metadata=false&daysAverageString=1d&cors=true&format=json'

# response = requests.get(url)
# print(response.json())




for chart_name in chart_name_list:
    try:
        start = '2009-01-01'
        print(f"Processing {chart_name}...")
        part = pd.DataFrame()
        
        while start <= '2024-01-01':
            response, df = get_chart_data(chart_name, start)
            if not df.empty:  # Only concatenate if df has data
                part = pd.concat([part, df], axis=0)
                start = str(int(start[:4])+1) + start[4:]
            else:
                start = str(int(start[:4])+1) + start[4:]
            time.sleep(1)

        part = part.reset_index(drop=True)
        
        time.sleep(5)
        part.to_csv(f'./data/{chart_name}.csv', index=False)
    
    except Exception as e:
        print(e)
        continue
