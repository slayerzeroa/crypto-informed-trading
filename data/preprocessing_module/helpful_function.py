import time
import pandas as pd

def ts_to_datetime(ts):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts / 1000))

# 초단위 데이터를 분단위로 변환
def resample_df(df, period='T'):

    # 인덱스가 datetime 형식이어야 함

    result_df = pd.DataFrame()
    result_df['Open'] = df['Open'].resample(period).first()
    result_df['High'] = df['High'].resample(period).max()
    result_df['Low'] = df['Low'].resample(period).min()
    result_df['Close'] = df['Close'].resample(period).last()
    result_df['Volume'] = df['Volume'].resample(period).sum()
    result_df['Quote asset volume'] = df['Quote asset volume'].resample(period).sum()
    result_df['Number of trades'] = df['Number of trades'].resample(period).sum()
    result_df['Taker buy base asset volume'] = df['Taker buy base asset volume'].resample(period).sum()
    result_df['Taker buy quote asset volume'] = df['Taker buy quote asset volume'].resample(period).sum()

    # # 결과 데이터프레임의 인덱스를 리셋
    # result_df.reset_index(inplace=True)
    return result_df


