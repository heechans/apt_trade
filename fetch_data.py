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
    'numOfRows': '10000'
}

# DB 파일 경로
db_path = 'apt_trade.db'

def fetch_and_save_data():
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()

        # XML 파싱 (네임스페이스 제거)
        xml_string = response.content.decode('utf-8')
        root = ET.fromstring(xml_string)

        # XML 구조에 맞게 <item> 태그 찾기
        items_node = root.find('.//items')
        if items_node is None:
            print("XML structure not as expected: no <items> tag found.")
            return

        data_list = []
        for item in items_node.findall('item'):
            data = {}
            for child in item:
                # 불필요한 공백 제거
                data[child.tag] = child.text.strip() if child.text else ''
            data_list.append(data)

        if not data_list:
            print("No data received from API.")
            return

        # SQLite DB에 연결 및 저장
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 테이블 생성
        # XML <item> 태그의 모든 자식 태그를 열 이름으로 사용
        if data_list:
            columns = data_list[0].keys()
            column_defs = [f'"{col}" TEXT' for col in columns]
            
            # 고유성 보장을 위한 UNIQUE 제약조건 추가
            unique_cols = ['aptNm', 'buildYear', 'dealAmount', 'excluUseAr', 'floor', 'dealYear', 'dealMonth', 'dealDay']
            unique_constraint = ', '.join([f'"{col}"' for col in unique_cols])
            
            create_table_sql = f'''
                CREATE TABLE IF NOT EXISTS apartment_trades (
                    {', '.join(column_defs)},
                    UNIQUE({unique_constraint}) ON CONFLICT REPLACE
                )
            '''
            cursor.execute(create_table_sql)

        # 데이터 삽입
        columns = list(data_list[0].keys())
        insert_sql = f'''
            INSERT OR REPLACE INTO apartment_trades ({', '.join([f'"{col}"' for col in columns])}) 
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

if __name__ == '__main__':
    fetch_and_save_data()
