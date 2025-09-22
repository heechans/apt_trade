import os
import requests
import xml.etree.ElementTree as ET
import sqlite3
from datetime import datetime

# API 정보 (GitHub Actions secrets를 사용할 것을 권장)
SERVICE_KEY = os.environ.get('API_KEY')
LAWD_CD = "11680"
# DEAL_YMD는 현재 날짜를 기반으로 자동으로 설정
DEAL_YMD = datetime.now().strftime('%Y%m')

# API URL
url = f'http://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev'
params = {
    'serviceKey': SERVICE_KEY,
    'LAWD_CD': LAWD_CD,
    'DEAL_YMD': DEAL_YMD,
    'pageNo': '1',
    'numOfRows': '100'
}

# DB 파일 경로
db_path = 'apt_trade.db'

def fetch_and_save_data():
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()

        # XML 파싱
        root = ET.fromstring(response.content)
        data_list = []
        for item in root.findall('.//{http://www.w3.org/2005/Atom}item'):
            data = {}
            for child in item:
                data[child.tag] = child.text
            data_list.append(data)

        if not data_list:
            print("No data received from API.")
            return

        # SQLite DB에 저장
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 테이블 생성
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS apartment_trades (
                거래금액 TEXT, 건축년도 TEXT, 년 TEXT, 법정동 TEXT, 아파트 TEXT,
                월 TEXT, 일 TEXT, 전용면적 TEXT, 지번 TEXT, 지역코드 TEXT, 층 TEXT,
                UNIQUE(거래금액, 건축년도, 년, 법정동, 아파트, 전용면적, 층) ON CONFLICT REPLACE
            )
        ''')

        # 데이터 삽입
        columns = [
            '거래금액', '건축년도', '년', '법정동', '아파트',
            '월', '일', '전용면적', '지번', '지역코드', '층'
        ]
        
        insert_sql = f'''
            INSERT OR REPLACE INTO apartment_trades ({', '.join(columns)}) 
            VALUES ({', '.join(['?' for _ in columns])})
        '''
        
        for row in data_list:
            values = [row.get(col) for col in columns]
            cursor.execute(insert_sql, values)

        conn.commit()
        conn.close()
        print(f"Successfully saved {len(data_list)} records to {db_path}")

    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
    except ET.ParseError as e:
        print(f"XML parsing failed: {e}")
    except sqlite3.Error as e:
        print(f"Database error: {e}")

if __name__ == "__main__':
    fetch_and_save_data()
