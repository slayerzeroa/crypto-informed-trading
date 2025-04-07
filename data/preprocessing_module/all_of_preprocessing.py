import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow as pa
import os
import glob
import datetime
import pandas as pd
import numpy as np
import helpful_function as hf
import copy
import time


def main(target_year:int):
    # # target_year = 2021
    today_date = 241219

    start_time = time.time()

    # data/test/price에 있는 csv 파일 모두 삭제
    files = glob.glob('data/test/price/secondly/*.csv')
    if files:
        for f in files:
            os.remove(f)
            print(f'{f} is removed')


    # 1단계: 초 단위 가격 데이터 불러오기

    # 날짜 생성 관련 함수
    def monthly_first_days_utc(year):
        '''
        특정 연도의 1월 1일부터 12월 1일까지의 datetime 객체 리스트 생성 (내년 1월 1일까지)
        '''
        tz = datetime.timezone.utc
        dates = []
        for month in range(1, 13):
            dt = datetime.datetime(year, month, 1, tzinfo=tz)
            dates.append(dt)
        dates.append(datetime.datetime(year + 1, 1, 1))
        return dates

    def ts_to_datetime(ts):
        '''
        타임스탬프를 datetime 객체로 변환
        '''
        return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)

    def datetime_to_ts(dt):
        '''
        datetime 객체를 타임스탬프로 변환
        '''
        # dt가 tzinfo가 없다면, UTC 타임존으로 가정
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return int(dt.timestamp())


    def days_in_year(year: int) -> int:
        # 윤년 판단 조건
        # 1. 400으로 나누어 떨어지면 윤년
        # 2. 위 조건에 해당하지 않고 100으로 나누어 떨어지면 평년
        # 3. 위 조건들에 해당하지 않고 4로 나누어 떨어지면 윤년
        if (year % 400 == 0) or (year % 4 == 0 and year % 100 != 0):
            return 366
        else:
            return 365

    datetime_list = monthly_first_days_utc(target_year)
    datetime_str_list = [dt.strftime("%Y-%m-%d %H:%M:%S") for dt in datetime_list]

    timestamp_list = [datetime_to_ts(i) for i in datetime_list]
    timestamp_list = [int(str(i)+'000') for i in timestamp_list]


    input_file = "data/price/BTCUSDT_spot_2017_2024_1s_klines.parquet"
    dataset = ds.dataset(input_file, format="parquet")

    iter = len(timestamp_list) - 1
    for i in range(iter):
        filtered_table = dataset.to_table(filter=(ds.field("Open time") >= timestamp_list[i]) & (ds.field("Open time") < timestamp_list[i+1]))
        filtered_df = filtered_table.to_pandas()

        filtered_df.to_csv(f'data/test/price/secondly/test_price_{target_year}_{i+1}.csv', index=False)


    # 메모리 관리
    del filtered_df
    del filtered_table
    del dataset


    # 2단계: spread 데이터 생성하기
    def edge(open: np.array, high: np.array, low: np.array, close: np.array, sign: bool = False) -> float:
        """
        Efficient Estimation of Bid-Ask Spreads from Open, High, Low, and Close Prices

        Implements an efficient estimator of bid-ask spreads from open, high, low, and close prices 
        as described in Ardia, Guidotti, & Kroencke (2024) -> https://doi.org/10.1016/j.jfineco.2024.103916

        Prices must be sorted in ascending order of the timestamp.

        Parameters
        ----------
        - `open`: array-like vector of open prices
        - `high`: array-like vector of high prices
        - `low`: array-like vector of low prices
        - `close`: array-like vector of close prices
        - `sign`: whether signed estimates should be returned

        Returns
        -------
        The spread estimate. A value of 0.01 corresponds to a spread of 1%.
        
        """

        o = np.log(np.asarray(open))
        h = np.log(np.asarray(high))
        l = np.log(np.asarray(low))
        c = np.log(np.asarray(close))
        m = (h + l) / 2.

        h1, l1, c1, m1 = h[:-1], l[:-1], c[:-1], m[:-1]
        o, h, l, c, m = o[1:], h[1:], l[1:], c[1:], m[1:]

        tau = np.logical_or(h != l, l != c1) 
        phi1 = np.logical_and(o != h, tau)
        phi2 = np.logical_and(o != l, tau)
        phi3 = np.logical_and(c1 != h1, tau)
        phi4 = np.logical_and(c1 != l1, tau)
    
        pt = np.nanmean(tau)
        po = np.nanmean(phi1) + np.nanmean(phi2)
        pc = np.nanmean(phi3) + np.nanmean(phi4)
        
        if pt == 0 or po == 0 or pc == 0:
            return np.nan

        r1 = m-o
        r2 = o-m1
        r3 = m-c1
        r4 = c1-m1
        r5 = o-c1
    
        d1 = r1 - tau * np.nanmean(r1) / pt
        d3 = r3 - tau * np.nanmean(r3) / pt
        d5 = r5 - tau * np.nanmean(r5) / pt
    
        x1 = -4./po*d1*r2 -4./pc*d3*r4 
        x2 = -4./po*d1*r5 -4./pc*d5*r4 
    
        e1 = np.nanmean(x1)
        e2 = np.nanmean(x2)
    
        v1 = np.nanmean(x1**2) - e1**2
        v2 = np.nanmean(x2**2) - e2**2
    
        s2 = (v2*e1 + v1*e2) / (v1 + v2)
    
        s = np.sqrt(np.abs(s2))
        if sign and s2 < 0: 
            s = -s
    
        return float(s)


    for j in range(12):
        # 파일명 설정
        price_df = pd.read_csv(f'data/test/price/secondly/test_price_{target_year}_{j+1}.csv')
        open_np = price_df['Open'].values
        high_np = price_df['High'].values
        low_np = price_df['Low'].values
        close_np = price_df['Close'].values
        window_size = 60

        bidask_np = np.zeros(len(open_np))

        for i in range(window_size-1, len(open_np)):
            open_window = open_np[i-window_size:i]
            high_window = high_np[i-window_size:i]
            low_window = low_np[i-window_size:i]
            close_window = close_np[i-window_size:i]
            spread = edge(open_window, high_window, low_window, close_window)
            bidask_np[i] = spread
            if i % 100000 == 0:  # 진행 상황 출력
                print(f'{i}: {spread}')

        # bidask_df = pd.DataFrame(bidask_np, columns=['Spread'])
        price_df['Spread'] = bidask_np

        price_df.to_csv(f'data/test/price/secondly/test_price_{target_year}_{j+1}.csv', index=False)
        

    # 메모리관리
    del price_df
    del open_np
    del high_np
    del low_np
    del close_np
    del bidask_np


    # 3단계: 초 단위 가격 데이터를 일 단위로 변경하기

    day_df = pd.DataFrame()
    for i in range(1, 13):
        price_df = pd.read_csv(f'data/test/price/secondly/test_price_{target_year}_{i}.csv')
        price_df['Open datetime'] = price_df['Open time'].apply(hf.ts_to_datetime)
        price_df.set_index('Open datetime', inplace=True)
        price_df.index = pd.to_datetime(price_df.index)
        
        temp_day_df = hf.resample_df(price_df, 'D')

        day_df = pd.concat([day_df, temp_day_df])
    day_df.index.name = 'Open datetime'
    day_df.to_csv(f'data/test/price/daily/test_daily_price_{target_year}.csv', index=True)


    # 메모리 관리
    del price_df
    del day_df
    del temp_day_df



    # 4단계: target 데이터 마련하기

    target_df = pd.read_csv('data/target/not_loop_10000_result.csv')
    target_df = target_df[(target_df['date'] >= int(f'{target_year}0101')) & (target_df['date'] < int(f'{target_year+1}0101'))]
    target_df.to_csv(f'data/test/target/test_target_{target_year}_{today_date}.csv', index=False)

    # 메모리 관리
    del target_df


    print("get data --- %s seconds ---" % (time.time() - start_time))



    ## 여기는 ipynb로 진행해야 할 수도 있음
    # 5단계: feature 생성하기
    target_df = pd.read_csv(f'data/test_data/target/test_target_{target_year}_{today_date}.csv')
    daily_price_df = pd.read_csv(f'data/test_data/price/daily/test_daily_price_{target_year}.csv')
    target_df['date'] = target_df['date'].astype(str).apply(lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:])


    sec_price_df = pd.DataFrame()

    for i in range(1, 13):
        temp_sec_price_df = pd.read_csv(f'data/test/price/secondly/test_price_{target_year}_{i}.csv')
        sec_price_df = pd.concat([sec_price_df, temp_sec_price_df])

    sec_price_columns_list = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore', 'spread']
    sec_price_df.columns = sec_price_columns_list
    sec_price_df['time'] = sec_price_df['open_time'].apply(lambda x: hf.ts_to_datetime(x))


    daily_price_columns_list = ['date', 'open', 'high', 'low', 'close', 'volume', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
    daily_price_df.columns = daily_price_columns_list


    daily_price_df['returns'] = daily_price_df['close'].pct_change()


    # 피쳐 생성
    target_df['returns'] = daily_price_df['returns']
    target_df['close'] = daily_price_df['close']
    target_df['abs_returns'] = target_df['returns'].abs()
    target_df['volume'] = daily_price_df['volume']
    target_df['open'] = daily_price_df['open']
    target_df['high'] = daily_price_df['high']
    target_df['low'] = daily_price_df['low']
    target_df['high_low'] = target_df['high'] - target_df['low']


    sec_price_df['volume_weighted_price'] = sec_price_df['close'] * sec_price_df['volume']

    sec_price_df['morning_flag'] = sec_price_df['time'].apply(lambda x: 1 if ((x[-8:] >= '06:00:00') and (x[-8:] < '12:00:00')) else 0)
    sec_price_df['afternoon_flag'] = sec_price_df['time'].apply(lambda x: 1 if ((x[-8:] >= '12:00:00') and (x[-8:] < '18:00:00')) else 0)
    sec_price_df['night_flag'] = sec_price_df['time'].apply(lambda x: 1 if ((x[-8:] >= '18:00:00') and (x[-8:] <= '23:59:59')) else 0)
    sec_price_df['dawn_flag'] = sec_price_df['time'].apply(lambda x: 1 if ((x[-8:] >= '00:00:00') and (x[-8:] < '06:00:00')) else 0)

    sec_price_df['returns'] = sec_price_df['close'].pct_change()
    sec_price_df = sec_price_df.dropna()
    sec_price_df['returns_for_cal'] = sec_price_df['returns']+1

    sec_price_df['date'] = pd.to_datetime(sec_price_df['time']).dt.date
    daily_avg_trades = sec_price_df.groupby('date')['number_of_trades'].mean()
    daily_sum_trades = sec_price_df.groupby('date')['number_of_trades'].sum()
    daily_sum_taker_buy_base_asset_volume = sec_price_df.groupby('date')['taker_buy_base_asset_volume'].sum()
    daily_sum_taker_buy_quote_asset_volume = sec_price_df.groupby('date')['taker_buy_quote_asset_volume'].sum()
    daily_avg_spread = sec_price_df.groupby('date')['spread'].mean()
    daily_quote_asset_volume = sec_price_df.groupby('date')['quote_asset_volume'].sum()
    daily_avg_quote_asset_volume = sec_price_df.groupby('date')['quote_asset_volume'].mean()
    daily_low = sec_price_df.groupby('date')['low'].min()
    daily_high = sec_price_df.groupby('date')['high'].max()
    daily_avg_price = sec_price_df.groupby('date')['close'].mean()
    daily_vwap = sec_price_df.groupby('date')['volume_weighted_price'].sum() / sec_price_df.groupby('date')['volume'].sum()
    daily_avg_volume = sec_price_df.groupby('date')['volume'].mean()
    daily_morning_returns = (sec_price_df[sec_price_df['morning_flag'] == 1].groupby('date')['returns_for_cal']).prod()-1
    daily_afternoon_returns = (sec_price_df[sec_price_df['afternoon_flag'] == 1].groupby('date')['returns_for_cal']).prod()-1
    daily_night_returns = (sec_price_df[sec_price_df['night_flag'] == 1].groupby('date')['returns_for_cal']).prod()-1
    daily_dawn_returns = (sec_price_df[sec_price_df['dawn_flag'] == 1].groupby('date')['returns_for_cal']).prod()-1

    daily_avg_morning_returns = sec_price_df[sec_price_df['morning_flag'] == 1].groupby('date')['returns'].mean()
    daily_avg_afternoon_returns = sec_price_df[sec_price_df['afternoon_flag'] == 1].groupby('date')['returns'].mean()
    daily_avg_night_returns = sec_price_df[sec_price_df['night_flag'] == 1].groupby('date')['returns'].mean()
    daily_avg_dawn_returns = sec_price_df[sec_price_df['dawn_flag'] == 1].groupby('date')['returns'].mean()

    daily_avg_morning_volume = sec_price_df[sec_price_df['morning_flag'] == 1].groupby('date')['volume'].mean()
    daily_avg_afternoon_volume = sec_price_df[sec_price_df['afternoon_flag'] == 1].groupby('date')['volume'].mean()
    daily_avg_night_volume = sec_price_df[sec_price_df['night_flag'] == 1].groupby('date')['volume'].mean()
    daily_avg_dawn_volume = sec_price_df[sec_price_df['dawn_flag'] == 1].groupby('date')['volume'].mean()
    daily_morning_volatility = sec_price_df[sec_price_df['morning_flag'] == 1].groupby('date')['returns'].std()
    daily_afternoon_volatility = sec_price_df[sec_price_df['afternoon_flag'] == 1].groupby('date')['returns'].std()
    daily_night_volatility = sec_price_df[sec_price_df['night_flag'] == 1].groupby('date')['returns'].std()
    daily_dawn_volatility = sec_price_df[sec_price_df['dawn_flag'] == 1].groupby('date')['returns'].std()
    daily_volatility = sec_price_df.groupby('date')['returns'].std()

    
    target_df.set_index('date', inplace=True)

    target_df.index = pd.to_datetime(target_df.index).date

    # 피쳐 상속
    target_df['daily_avg_trades'] = daily_avg_trades
    target_df['daily_sum_trades'] = daily_sum_trades
    target_df['daily_sum_taker_buy_base_asset_volume'] = daily_sum_taker_buy_base_asset_volume
    target_df['daily_sum_taker_buy_quote_asset_volume'] = daily_sum_taker_buy_quote_asset_volume
    target_df['daily_avg_spread'] = daily_avg_spread
    target_df['daily_quote_asset_volume'] = daily_quote_asset_volume
    target_df['daily_avg_quote_asset_volume'] = daily_avg_quote_asset_volume
    target_df['daily_low'] = daily_low
    target_df['daily_high'] = daily_high
    target_df['daily_avg_price'] = daily_avg_price
    target_df['daily_vwap'] = daily_vwap
    target_df['daily_avg_volume'] = daily_avg_volume
    target_df['daily_morning_returns'] = daily_morning_returns
    target_df['daily_afternoon_returns'] = daily_afternoon_returns
    target_df['daily_night_returns'] = daily_night_returns
    target_df['daily_dawn_returns'] = daily_dawn_returns
    target_df['daily_avg_morning_returns'] = daily_avg_morning_returns
    target_df['daily_avg_afternoon_returns'] = daily_avg_afternoon_returns
    target_df['daily_avg_night_returns'] = daily_avg_night_returns
    target_df['daily_avg_dawn_returns'] = daily_avg_dawn_returns
    target_df['daily_avg_morning_volume'] = daily_avg_morning_volume
    target_df['daily_avg_afternoon_volume'] = daily_avg_afternoon_volume
    target_df['daily_avg_night_volume'] = daily_avg_night_volume
    target_df['daily_avg_dawn_volume'] = daily_avg_dawn_volume
    target_df['daily_morning_volatility'] = daily_morning_volatility
    target_df['daily_afternoon_volatility'] = daily_afternoon_volatility
    target_df['daily_night_volatility'] = daily_night_volatility
    target_df['daily_dawn_volatility'] = daily_dawn_volatility
    target_df['daily_volatility'] = daily_volatility




    # 6단계: special feature 생성하기
    # special features 읽어오기
    files = glob.glob('data/special_feature/*.csv')

    num_files = len(files)
    for i, file in enumerate(files):
        try:
            globals()['special_feature_df'+str(i)] = pd.read_csv(file)
        except:
            print('Error in reading file:', file)
            continue


    for i in range(num_files):
        try:
            globals()['special_feature_df'+str(i)] = globals()['special_feature_df'+str(i)][((globals()['special_feature_df'+str(i)]['timestamp'] >= datetime_str_list[0]) & (globals()['special_feature_df'+str(i)]['timestamp'] < datetime_str_list[-1]))]
        except Exception as e:
            print('Error in processing special feature:', i)
            print(e)
            continue


    special_features_df = pd.DataFrame()

    for i in range(num_files):
        try:
            if len(globals()['special_feature_df'+str(i)]) == len(special_feature_df0):
                globals()['special_feature_df'+str(i)].set_index('timestamp', inplace=True)
                special_features_df = pd.concat([special_features_df, globals()['special_feature_df'+str(i)]], axis=1)
        
        except Exception as e:
            print('Error in processing special feature:', i)
            print(e)
            continue


    special_features_df.index = pd.to_datetime(special_features_df.index).date

    target_df = pd.concat([target_df, special_features_df], axis=1)

    target_df.to_csv(f'data/test/preprocessed/test_dataset_{target_year}_{today_date}.csv', index=True)

    print("every process --- %s seconds ---" % (time.time() - start_time))

target_year = 2017

while target_year < 2025:
    main(target_year)
    target_year += 1
    break