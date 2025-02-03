# import time
# import subprocess
# import pandas as pd
# from bs4 import BeautifulSoup
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# from webdriver_manager.chrome import ChromeDriverManager
# from selenium_stealth import stealth
# from selenium.common.exceptions import NoSuchElementException, WebDriverException

# # ---------------------------------------------------
# # 1) 시작 트윗 ID 및 설정
# # ---------------------------------------------------
# # 시작 트윗 ID 설정 (예시)
# start_tweet_id = 1682964919325724673
# # 수집할 트윗 수 설정
# num_tweets_to_collect = 100

# # ---------------------------------------------------
# # 2) Chrome 디버깅 모드 실행 (한 번만)
# # ---------------------------------------------------
# # 이미 Chrome이 --remote-debugging-port=9222로 실행 중이면 생략 가능
# # 아래 Popen을 계속 실행하면 Chrome 창이 매번 새로 떠서 충돌 날 수 있음
# # 필요 시 주석 해제하여 한 번만 실행
# # subprocess.Popen(
# #     r"C:\Program Files\Google\Chrome\Application\chrome.exe --remote-debugging-port=9222"
# # )
# # time.sleep(3)  # Chrome 디버깅 모드가 뜰 시간을 줌

# # ---------------------------------------------------
# # 3) Selenium WebDriver 설정 (한 번만)
# # ---------------------------------------------------
# chrome_options = Options()
# # 이미 실행 중인 Chrome 브라우저와 연결 (디버깅 포트 사용 시)
# # chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")

# # 헤드리스 모드 사용 시 주석 처리 해제
# # chrome_options.add_argument('--headless')

# # 브라우저 창을 숨기지 않으려면 아래 옵션 제거
# chrome_options.add_argument('--start-maximized')

# # WebDriver 설정
# driver = webdriver.Chrome(
#     service=Service(ChromeDriverManager().install()),
#     options=chrome_options
# )

# # stealth 설정
# stealth(
#     driver,
#     languages=["en-US", "en"],
#     vendor="Google Inc.",
#     platform="Win32",
#     webgl_vendor="Intel Inc.",
#     renderer="Intel Iris OpenGL Engine",
#     fix_hairline=True,
# )

# time.sleep(3)  # 설정 적용 대기

# # ---------------------------------------------------
# # 4) 데이터 크롤링 + 파싱
# # ---------------------------------------------------
# result = pd.DataFrame()
# lost = []  # 수집에 실패한 트윗 ID 저장
# count = 0

# for i in range(num_tweets_to_collect):
#     tweet_id = start_tweet_id + i
#     tweet_url = f'https://x.com/elonmusk/status/{tweet_id}'
#     print(f'접속 중: {tweet_url}')
    
#     try:
#         # 4-1) URL 접속
#         driver.get(tweet_url)
        
#         # 4-2) Cloudflare나 캡차가 뜨면, 사용자가 직접 해결
#         # 페이지에 캡차가 있는지 확인
#         time.sleep(5)  # 페이지 로딩 대기
        
#         # # 캡차 존재 여부 확인
#         # if "captcha" in driver.page_source.lower():
#         #     print("캡차가 감지되었습니다. 수동으로 해결해주세요.")
#         #     input("캡차를 해결한 후 Enter 키를 누르세요...")
#         #     time.sleep(5)  # 캡차 해결 후 페이지 로딩 대기
        
#         # 4-3) 페이지 로딩 대기 (명시적 대기 또는 sleep)
#         driver.implicitly_wait(10)
        
#         # 4-4) BeautifulSoup 파싱
#         soup = BeautifulSoup(driver.page_source, 'html.parser')
        
#         # 트윗 내용 추출 (트윗 구조에 따라 XPath나 CSS 선택자 수정 필요)
#         try:
#             tweet_element = soup.find('div', {'data-testid': 'tweet'})
#             if not tweet_element:
#                 print(f'트윗을 찾을 수 없습니다 (ID: {tweet_id})')
#                 lost.append(tweet_id)
#                 continue
            
#             # 트윗 텍스트 추출
#             tweet_text = tweet_element.get_text(separator=' ', strip=True)
            
#             # 트윗 작성 시간 추출
#             time_element = soup.find('time')
#             tweet_time = time_element['datetime'] if time_element else None
            
#             tweet_data = {
#                 'Tweet ID': tweet_id,
#                 'URL': tweet_url,
#                 'Text': tweet_text,
#                 'Created At': tweet_time
#             }
#             result = result.append(tweet_data, ignore_index=True)
#             print(f'트윗 수집 성공: {tweet_id}')
        
#         except Exception as e:
#             print(f'트윗 파싱 오류 (ID: {tweet_id}): {e}')
#             lost.append(tweet_id)
    
#     except WebDriverException as e:
#         print(f'웹 드라이버 오류 발생 (ID: {tweet_id}): {e}')
#         lost.append(tweet_id)
    
#     # 4-5) 요청 간 대기 (레이트 리밋 방지)
#     time.sleep(3)  # 필요 시 조절
    
#     # 4-6) 주기적으로 데이터 저장
#     count += 1
#     if count % 20 == 0:
#         result.to_csv(f"data/x_crawling/tweets_{count}_250104.csv", index=False)
#         print(f'{count}개의 트윗을 수집했습니다.')
    
# # ---------------------------------------------------
# # 5) 남은 데이터 최종 저장
# # ---------------------------------------------------
# if not result.empty:
#     result.to_csv("data/x_crawling/tweets_final_250104.csv", index=False)
#     print(f'최종적으로 {len(result)}개의 트윗을 수집했습니다.')

# # ---------------------------------------------------
# # 6) 브라우저 종료 (모든 작업 완료 후)
# # ---------------------------------------------------
# driver.quit()

# # ---------------------------------------------------
# # 7) 수집에 실패한 트윗 ID 저장
# # ---------------------------------------------------
# if lost:
#     lost_df = pd.DataFrame(lost, columns=['Lost Tweet ID'])
#     lost_df.to_csv("data/x_crawling/lost_tweets_250104.csv", index=False)
#     print(f'수집에 실패한 트윗 ID: {lost}')



import time
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium_stealth import stealth
from selenium.common.exceptions import NoSuchElementException, WebDriverException
import logging
import os

# ---------------------------------------------------
# 1) 로깅 설정
# ---------------------------------------------------
logging.basicConfig(
    filename='theverge_elon_musk_crawling.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# ---------------------------------------------------
# 2) Selenium WebDriver 설정 함수
# ---------------------------------------------------
def setup_driver(headless=True):
    chrome_options = Options()
    if headless:
        chrome_options.add_argument('--headless')  # 브라우저 창을 띄우지 않음
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--start-maximized')  # 창 최대화
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')  # 자동화 탐지 방지

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )

    # stealth 설정으로 자동화 탐지 방지
    stealth(
        driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )

    return driver

# ---------------------------------------------------
# 3) 기사 정보 추출 함수 정의
# ---------------------------------------------------
def extract_articles(soup):
    articles = soup.find_all('div', class_='c-compact-river__entry')
    data = []

    for article in articles:
        # 기사 제목과 URL 추출
        title_tag = article.find('h2', class_='c-entry-box--compact__title')
        if title_tag and title_tag.find('a'):
            title = title_tag.find('a').get_text(strip=True)
            article_url = title_tag.find('a')['href']
        else:
            title = None
            article_url = None

        # 저자 및 날짜 추출
        byline_div = article.find('div', class_='c-byline')
        if byline_div:
            # 저자 이름 추출
            author_tag = byline_div.find('span', class_='c-byline__author-name')
            author = author_tag.get_text(strip=True) if author_tag else None

            # 작성 날짜 추출
            time_tag = byline_div.find('time')
            date = time_tag['datetime'] if time_tag and time_tag.has_attr('datetime') else None
        else:
            author = None
            date = None

        # 추출한 정보를 딕셔너리로 저장
        article_data = {
            'Title': title,
            'URL': article_url,
            'Author': author,
            'Date': date
        }

        data.append(article_data)

    return data

# ---------------------------------------------------
# 4) 메인 크롤링 함수 정의
# ---------------------------------------------------
def crawl_theverge_elon_musk(start_year=2017, start_month=1, end_year=2025, end_month=1, headless=True):
    # 데이터 저장 디렉토리 설정
    output_dir = "data/theverge_elon_musk/"
    os.makedirs(output_dir, exist_ok=True)

    driver = setup_driver(headless=headless)
    result = []
    lost = []

    try:
        current_year = start_year
        current_month = start_month

        while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
            # 크롤링할 아카이브 페이지 URL 설정
            url = f'https://www.theverge.com/archives/elon-musk/{current_year}/{current_month}'
            print(f'접속 중: {url}')
            logging.info(f'접속 중: {url}')

            driver.get(url)
            time.sleep(5)  # 페이지 로딩 대기

            # # CAPTCHAs 감지 및 수동 해결 요청
            # if "captcha" in driver.page_source.lower():
            #     print("캡차가 감지되었습니다. 수동으로 해결해주세요.")
            #     logging.warning("캡차가 감지되었습니다. 수동으로 해결해주세요.")
            #     input("캡차를 해결한 후 Enter 키를 누르세요...")
            #     time.sleep(5)  # 캡차 해결 후 페이지 로딩 대기

            # 페이지 소스 가져오기 및 파싱
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            # 기사 정보 추출
            articles_data = extract_articles(soup)
            if not articles_data:
                print(f'페이지 {current_year}-{current_month}에서 기사를 찾을 수 없습니다.')
                logging.warning(f'페이지 {current_year}-{current_month}에서 기사를 찾을 수 없습니다.')
                # 다음 월로 이동
            else:
                result.extend(articles_data)
                print(f'페이지 {current_year}-{current_month}의 {len(articles_data)}개 기사 수집 완료.')
                logging.info(f'페이지 {current_year}-{current_month}의 {len(articles_data)}개 기사 수집 완료.')

            # 데이터 저장
            current_page = f"{current_year}_{current_month:02d}"
            if len(result) >= 100:  # 예: 100개 기사마다 저장
                df = pd.DataFrame(result)
                df.to_csv(os.path.join(output_dir, f'theverge_elon_musk_articles_{current_page}.csv'), index=False, encoding='utf-8-sig')
                logging.info(f'{current_page} 데이터 저장 완료.')
                print(f'{current_page} 데이터 저장 완료.')
                result = []  # 초기화

            # 다음 월로 이동
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1

            # 요청 간 대기 (레이트 리밋 방지)
            time.sleep(3)

    except Exception as e:
        print(f'오류 발생: {e}')
        logging.error(f'오류 발생: {e}', exc_info=True)

    finally:
        # 남은 데이터 저장
        if result:
            # 현재 연도와 월을 반영한 파일 이름 생성
            final_page = f"{current_year}_{current_month:02d}"
            df = pd.DataFrame(result)
            df.to_csv(os.path.join(output_dir, f'theverge_elon_musk_articles_final_{final_page}.csv'), index=False, encoding='utf-8-sig')
            print(f'최종적으로 {len(result)}개의 기사를 "theverge_elon_musk_articles_final_{final_page}.csv" 파일로 저장했습니다.')
            logging.info(f'최종적으로 {len(result)}개의 기사를 저장했습니다.')

        # 수집에 실패한 기사 정보 저장 (현재는 빈 리스트)
        if lost:
            df_lost = pd.DataFrame(lost, columns=['Lost Article'])
            df_lost.to_csv(os.path.join(output_dir, "lost_articles.csv"), index=False, encoding='utf-8-sig')
            print(f'수집에 실패한 기사: {lost}')
            logging.info(f'수집에 실패한 기사: {lost}')

        # 브라우저 종료
        driver.quit()
        print('브라우저를 종료했습니다.')
        logging.info('브라우저를 종료했습니다.')

# ---------------------------------------------------
# 5) 실행 부분
# ---------------------------------------------------
if __name__ == "__main__":
    # 크롤링 설정
    START_YEAR = 2017
    START_MONTH = 1
    END_YEAR = 2025
    END_MONTH = 1
    HEADLESS_MODE = False  # 브라우저 창을 보고 싶으면 False로 설정

    # 크롤링 시작
    crawl_theverge_elon_musk(
        start_year=START_YEAR,
        start_month=START_MONTH,
        end_year=END_YEAR,
        end_month=END_MONTH,
        headless=HEADLESS_MODE
    )
