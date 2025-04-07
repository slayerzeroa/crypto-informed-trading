import time
import pandas as pd
import requests
import calendar
import time

year_list = [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
year_list = [2021, 2022, 2023, 2024]


for year in year_list:
    # Kraken API 엔드포인트
    BASE_URL = "https://api.kraken.com/0/public/Trades"
    PAIR = "XXBTZUSD"  # BTC/USD의 Kraken 표기

    # 2017년 시작 및 종료 타임스탬프 (초 단위)
    start_timestamp = calendar.timegm(time.strptime(f"{year}-01-01", "%Y-%m-%d"))
    end_timestamp = calendar.timegm(time.strptime(f"{year}-12-31 23:59:59", "%Y-%m-%d %H:%M:%S"))


    # 데이터를 저장할 리스트
    all_trades = []

    since = start_timestamp * 10**9  # Kraken API는 나노초 단위 사용

    while True:
        # API 요청
        response = requests.get(BASE_URL, params={"pair": PAIR, "since": since})
        data = response.json()

        if "error" in data and data["error"]:
            print("API 오류:", data["error"])
            break

        # 거래 데이터 추출
        trades = data["result"][PAIR]
        last_timestamp = int(data["result"]["last"])  # 다음 요청을 위한 since 값 (나노초)

        if not trades:
            print("더 이상 거래 데이터 없음. 종료.")
            break

        # 데이터 저장
        for trade in trades:
            trade_time = int(trade[2])  # Unix timestamp (초 단위)

            if trade_time > end_timestamp:  # 2017년 데이터까지만 가져오기
                print("2022년 데이터를 모두 수집 완료. 종료합니다.")
                break

            all_trades.append({
                "price": float(trade[0]),   # 가격 (float 변환)
                "volume": float(trade[1]),  # 거래량 (float 변환)
                "timestamp": trade_time,    # Unix timestamp (초 단위)
                "side": trade[3],            # 거래 유형 ("b" = buy, "s" = sell)
                "trade_id" :trade[6]
            })

        since_to_date = pd.to_datetime(since, unit="ns")
        print(f"{len(trades)}건의 거래 데이터를 수집했습니다. (since={since_to_date}))")

        # 다음 요청을 위해 since 값을 갱신
        since = last_timestamp

        # 더 이상 새로운 데이터가 없으면 종료
        if last_timestamp // 10**9 > end_timestamp:
            print("2022년 거래 데이터 수집 완료.")
            break

        # Kraken API 요청 제한 준수를 위해 딜레이 추가
        time.sleep(1.5)

    trade_df = pd.DataFrame(all_trades)
    trade_df.to_parquet(f'./data/orderbook_snapshot/orderbook/orderbook_{year}.parquet')