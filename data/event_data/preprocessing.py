import pandas as pd
import glob
import os

csv_file = 'data/event_data/bitcoin_events_final_updated.csv'
df = pd.read_csv(csv_file, parse_dates=['Date'])
print("원본 데이터프레임:")
print(df.head())

start_date = '2017-01-01'
end_date = '2025-01-01'
date_range = pd.date_range(start=start_date, end=end_date, freq='D')
print("날짜 범위:")
print(date_range)

event_df = pd.DataFrame(0, index=date_range, columns=['Event'])

# Date 열이 datetime 형식인지 확인하고, 아니라면 변환
if not pd.api.types.is_datetime64_any_dtype(df['Date']):
    df['Date'] = pd.to_datetime(df['Date'])

# 이벤트 날짜 추출 (시간 부분 제거)
event_dates = df['Date'].dt.normalize()

# 이벤트 표시
event_df['Event'] = event_df.index.isin(event_dates).astype(int)

event_df.to_csv("bitcoin_events.csv")

# total_events = event_df['Event'].sum()
# print(f"총 이벤트 수: {total_events}")

# # 특정 날짜 예시
# specific_date = '2020-05-15'
# print(f"{specific_date}의 이벤트: {event_df.loc[specific_date, 'Event']}")

# # 데이터프레임을 CSV 파일로 저장
# output_file = 'data/event_data/bitcoin_event_binary.csv'
# event_df.to_csv(output_file, encoding='utf-8-sig')
# print(f"이벤트 데이터가 '{output_file}' 파일로 저장되었습니다.")
