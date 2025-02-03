import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time


from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import subprocess
import time  # 대기 시간을 위해 추가
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service


# '''
# 비트코인 지갑 랭킹 데이터 수집
# '''


# # 페이지 URL
# url = "https://bitinfocharts.com/top-100-richest-bitcoin-addresses.html"


# def get_ranking_df(url, jump=False):
#     # requests로 페이지 가져오기
#     response = requests.get(url)

#     # BeautifulSoup으로 파싱
#     soup = BeautifulSoup(response.content, 'html.parser')

#     # 테이블에서 데이터 추출
#     table = soup.find('table', {'class': 'table table-striped abtb'})
#     data = []

#     # 각 행(row) 데이터 추출
#     rows = table.find_all('tr')


#     for row in rows[1:]:  # 첫 번째 행은 헤더이므로 건너뜁니다.
#         cols = row.find_all('td')
#         cols = [' '.join(col.text.split()) for col in cols]
#         data.append(cols)

#     table = soup.find('table', {'class': 'table table-striped bb'})


#     # 각 행(row) 데이터 추출
#     rows = table.find_all('tr')

#     if jump:
#         for row in rows[1:]:  # 첫 번째 행은 헤더이므로 건너뜁니다.
#             cols = row.find_all('td')
#             cols = [' '.join(col.text.split()) for col in cols]
#             data.append(cols)
#     else:
#         for row in rows:  # 첫 번째 행은 헤더이므로 건너뜁니다.
#             cols = row.find_all('td')
#             cols = [' '.join(col.text.split()) for col in cols]
#             data.append(cols)
#     # 데이터 프레임으로 변환
#     df = pd.DataFrame(data, columns=['Index', 'Address', 'Balance', '% of coins', 'First In', 'Last In', 'Ins', 'First Out', 'Last Out', 'Outs'])

#     # 모든 <a> 태그에서 href 속성의 주소 부분만 추출
#     addresses = [a['href'].split('/')[-1] for a in soup.find_all('a', href=True) if 'address' in a['href']]

#     filtered_addresses = [address for address in addresses if re.match(r'^[13][a-km-zA-HJ-NP-Z0-9]{25,34}$|^bc1[a-zA-HJ-NP-Z0-9]{39,59}$', address)]

#     df['filtered_address'] = filtered_addresses

#     return df

# result = get_ranking_df(url, jump=False)
# for i in range(2, 101):
#     try:
#         url = f"https://bitinfocharts.com/top-100-richest-bitcoin-addresses-{i}.html"

#         temp_df = get_ranking_df(url)
#         result = pd.concat([result, temp_df], axis=0)
#         print('하는 중:', i*100, '행')

#     except:
#         print('종료합니다.')
#         break

# result.to_csv("wallet_ranking_250103", index=False)


# '''
# wallet 이름 전처리
# '''

# df = pd.read_csv('wallet_ranking_250103_test.csv')

# df['wallet'] = df.apply(lambda row: row['Address'].replace(row['filtered_address'], '', 1) if row['filtered_address'] in row['Address'] else row['Address'], axis=1)
# df['filtered_wallet'] = df['wallet'].apply(lambda x: x.split('wallet: ')[1] if 'wallet: ' in x else (x.split('Balance:')[0] if 'Balance' in x else (x.split('30d')[0] if '30d' in x else (x.split('7d')[0] if '7d' in x else x))))
# df['filtered_wallet'] = df['filtered_wallet'].apply(lambda x: x.split('wallet: ')[1] if 'wallet: ' in x else (x.split('Balance:')[0] if 'Balance' in x else (x.split('30d')[0] if '30d' in x else (x.split('7d')[0] if '7d' in x else x))))
# df['filtered_wallet'] = df['filtered_wallet'].apply(lambda x: x.split('wallet: ')[1] if 'wallet: ' in x else (x.split('Balance:')[0] if 'Balance' in x else (x.split('30d')[0] if '30d' in x else (x.split('7d')[0] if '7d' in x else x))))
# df['filtered_wallet'] = df['filtered_wallet'].apply(lambda x: x.split('wallet: ')[1] if 'wallet: ' in x else (x.split('Balance:')[0] if 'Balance' in x else (x.split('30d')[0] if '30d' in x else (x.split('7d')[0] if '7d' in x else x))))
# df['filtered_wallet'] = df['filtered_wallet'].apply(lambda x: x.strip())

# df.to_csv("wallet_ranking_250103_test.csv", index=False)



# """
# wallet ranking transaction 데이터 받아오기 (수동으로 캡차 해결)
# """
# import time
# import pandas as pd
# from bs4 import BeautifulSoup

# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from webdriver_manager.chrome import ChromeDriverManager
# from selenium_stealth import stealth
# import random

# # ---------------------------------------------------
# # 1) CSV 로딩 및 주소 리스트 추출
# # ---------------------------------------------------
# pd.set_option('display.max_columns', None)
# df_csv = pd.read_csv('wallet_ranking_250103_test.csv')
# addresses = list(df_csv['filtered_address'])



# # ---------------------------------------------------
# # 3) 데이터 크롤링 + 파싱
# # ---------------------------------------------------
# result = pd.DataFrame()
# count = 1

# for address in addresses:
#     # Chrome 실행 경로 (디버깅 포트 열기)
#     subprocess.Popen(r"C:\Program Files\Google\Chrome\Application\chrome.exe --remote-debugging-port=9222")

#     # Selenium 설정
#     option = Options()
#     option.add_experimental_option("debuggerAddress", "127.0.0.1:9222")

#     # Chrome WebDriver 실행
#     driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=option)

#     # stealth 설정 (Headless 탐지/자동화 탐지 우회)
#     stealth(
#         driver,
#         languages=["en-US", "en"],
#         vendor="Google Inc.",
#         platform="Win32",
#         webgl_vendor="Intel Inc.",
#         renderer="Intel Iris OpenGL Engine",
#         fix_hairline=True,
#     )
    
#     driver.maximize_window()

#     url = f'https://bitinfocharts.com/bitcoin/address/{address}-full'

#     # 3-1) URL 접속
#     driver.get(url)

#     # # 3-2) 잠시 대기 (페이지 로딩)
#     # driver.implicitly_wait(3)

#     # 3-3) 페이지 소스를 BeautifulSoup으로 파싱
#     soup = BeautifulSoup(driver.page_source, 'html.parser')

#     # 3-4) tr 태그 추출
#     rows = soup.find_all('tr', {'class': 'trb'})

#     # 3-5) 행 데이터 수집
#     data = []
#     for row in rows:
#         cols = row.find_all('td')
#         if len(cols) > 1:
#             block_number = cols[0].find('a').text.strip() if cols[0].find('a') else None
#             block_time = cols[1].text.strip()
#             btc_change = cols[2].text.strip()
#             btc_balance = cols[3].text.strip()
#             usd_value = cols[4].text.strip()
#             usd_value_hidden = cols[5].text.strip()
#             data.append([block_number, block_time, btc_change, btc_balance, usd_value, usd_value_hidden])

#     # 3-6) 수집한 행 데이터를 DataFrame으로 변환
#     df_temp = pd.DataFrame(data, columns=[
#         'Block Number', 'Block Time', 'BTC Change',
#         'BTC Balance', 'USD Value', 'USD Value Hidden'
#     ])
#     df_temp['Address'] = address

#     # # 테스트 출력
#     # print(df_temp)

#     driver.close()

#     # 3-7) 누적
#     result = pd.concat([result, df_temp], ignore_index=True)

#     print("현재진행:", count, "/", len(addresses), " / Address:", address)

#     # 3-8) 일정 건수마다 임시 저장
#     if count % 100 == 0:
#         result.to_csv(f"data/wallet_ranking/transaction/transaction_{count}_250104.csv", index=False)
#         result = pd.DataFrame()

#     count += 1


# # ---------------------------------------------------
# # 4) 남은 데이터 최종 저장
# # ---------------------------------------------------
# if not result.empty:
#     result.to_csv("data/wallet_ranking/transaction/transaction_final_250104.csv", index=False)

# # ---------------------------------------------------
# # 5) 브라우저 종료
# # ---------------------------------------------------

"""
wallet ranking transaction 데이터 받아오기 (수동으로 캡차 해결)
"""
import time
import subprocess
import pandas as pd
from bs4 import BeautifulSoup

# Selenium import
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium_stealth import stealth

# ---------------------------------------------------
# 1) CSV 로딩 및 주소 리스트 추출
# ---------------------------------------------------
pd.set_option('display.max_columns', None)
df_csv = pd.read_csv('wallet_ranking_250103_test.csv')
addresses = list(df_csv['filtered_address'])

# ---------------------------------------------------
# 2) Chrome 디버깅 모드 실행 (한 번만)
# ---------------------------------------------------
# 이미 Chrome이 --remote-debugging-port=9222로 실행 중이면 생략 가능
# 아래 Popen을 계속 실행하면 Chrome 창이 매번 새로 떠서 충돌 날 수 있음
subprocess.Popen(
    r"C:\Program Files\Google\Chrome\Application\chrome.exe --remote-debugging-port=9222"
)

time.sleep(3)  # Chrome 디버깅 모드가 뜰 시간을 줌

# ---------------------------------------------------
# 3) Selenium WebDriver 설정 (한 번만)
# ---------------------------------------------------
chrome_options = Options()
chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=chrome_options
)

# stealth 설정
stealth(
    driver,
    languages=["en-US", "en"],
    vendor="Google Inc.",
    platform="Win32",
    webgl_vendor="Intel Inc.",
    renderer="Intel Iris OpenGL Engine",
    fix_hairline=True,
)

# driver.maximize_window()
time.sleep(3)
# ---------------------------------------------------
# 4) 데이터 크롤링 + 파싱
# ---------------------------------------------------
result = pd.DataFrame()
count = 5001
lost=[]
for address in addresses[5083:]:
    # if count in [5084]:
    #     lost.append(count)
    #     count += 1
    #     continue
    try:
        url = f'https://bitinfocharts.com/bitcoin/address/{address}-full'

        # 4-1) URL 접속
        driver.get(url)

        # 4-2) Cloudflare나 캡차가 뜨면, 사용자가 직접 해결
        #      (원하는 경우 input()을 사용해 일시 정지)
        # input("캡차가 뜨면 수동으로 해결 후 Enter를 누르세요...")

        # 4-3) 페이지 로딩 대기 (명시적 대기 or sleep)
        driver.implicitly_wait(8)

        # 4-4) BeautifulSoup 파싱
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        print(soup)

        rows = soup.find_all('tr', {'class': 'trb'})

        # 4-5) 행 데이터 수집
        data = []
        for row in rows:
            cols = row.find_all('td')
            if len(cols) > 1:
                block_number = cols[0].find('a').text.strip() if cols[0].find('a') else None
                block_time = cols[1].text.strip()
                btc_change = cols[2].text.strip()
                btc_balance = cols[3].text.strip()
                usd_value = cols[4].text.strip()
                usd_value_hidden = cols[5].text.strip()
                data.append([block_number, block_time, btc_change, btc_balance, usd_value, usd_value_hidden])

        df_temp = pd.DataFrame(data, columns=[
            'Block Number', 'Block Time', 'BTC Change',
            'BTC Balance', 'USD Value', 'USD Value Hidden'
        ])
        df_temp['Address'] = address

        # 4-6) 누적
        result = pd.concat([result, df_temp], ignore_index=True)
        print("현재진행:", count, "/", len(addresses), " / Address:", address)

    except:
        lost.append(count)

    result.to_csv(f"data/wallet_ranking/transaction/transaction_5804_250104.csv", index=False)
    break

    if count % 100 == 0:
        result.to_csv(f"data/wallet_ranking/transaction/transaction_{count}_250104.csv", index=False)
        result = pd.DataFrame()

    count += 1
print(lost)
# ---------------------------------------------------
# 5) 남은 데이터 최종 저장
# ---------------------------------------------------
if not result.empty:
    result.to_csv("data/wallet_ranking/transaction/transaction_final_250104.csv", index=False)

# ---------------------------------------------------
# 6) 브라우저 종료 (모든 작업 완료 후)
# ---------------------------------------------------
driver.quit()



# import cloudscraper

# pd.set_option('display.max_columns', None)
# df = pd.read_csv('wallet_ranking_250103_test.csv')

# addresses = list(df['filtered_address'])


# result = pd.DataFrame()
# count = 1
# for address in addresses:
#     # HTML 데이터를 받아오기
#     url = f'https://bitinfocharts.com/bitcoin/address/{address}-full'  # 실제 URL을 넣어야 합니다.

#     headers = {
#         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
#     }

#     scraper = cloudscraper.create_scraper()

#     response = scraper.get(url, headers=headers)

#     print(response)

#     # BeautifulSoup으로 HTML 파싱
#     soup = BeautifulSoup(response.content, 'html.parser')

#     # print(soup)

#     # tr 태그에서 필요한 데이터 추출
#     rows = soup.find_all('tr', {'class': 'trb'})  # 'trb' 클래스를 가진 모든 tr 태그를 찾음

#     # print(rows)

#     # 데이터 저장할 리스트
#     data = []

#     # 각 행에서 필요한 데이터 추출
#     for row in rows:
#         # 각 열(td) 찾기
#         cols = row.find_all('td')
#         if len(cols) > 1:
#             block_number = cols[0].find('a').text.strip()  # 블록 번호
#             block_time = cols[1].text.strip()  # 블록 시간
#             btc_change = cols[2].text.strip()  # BTC 변화량
#             btc_balance = cols[3].text.strip()  # BTC 잔액
#             usd_value = cols[4].text.strip()  # USD 가치
#             usd_value_hidden = cols[5].text.strip()  # 숨겨진 USD 가치 (필요한 경우)

#             # 데이터 추가
#             data.append([block_number, block_time, btc_change, btc_balance, usd_value, usd_value_hidden])

#     # 데이터 프레임으로 변환
#     df = pd.DataFrame(data, columns=['Block Number', 'Block Time', 'BTC Change', 'BTC Balance', 'USD Value', 'USD Value Hidden'])
#     df['Address'] = address

#     result = pd.concat([result, df], axis=0)

#     print(result)
#     print("현재진행:", count)

#     if count % 100 == 0:
#         result.to_csv(f"data/wallet_ranking/transaction/transaction_{count}_250104.csv", index=False)
#         result = pd.DataFrame()

#     time.sleep(0.1)

#     count += 1




# result.to_csv(f"data/wallet_ranking/transaction/transaction_last.csv", index=False)

# result.to_csv('wallet_ranking_transaction_250104.csv', index=False)





