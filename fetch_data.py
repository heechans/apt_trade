import os
import requests
import xml.etree.ElementTree as ET
from datetime import datetime

# API 정보 (GitHub Actions secrets를 사용할 것을 권장)
SERVICE_KEY = os.environ.get('API_KEY')
LAWD_CD = "11680"
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

# SQL 파일 경로
sql_path = 'apt_trade.sql'

def fetch_and_generate_sql():
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()

        root = ET.fromstring(response.content)
        items_node = root.find('.//items')

        if items_node is None:
            print("XML 응답에서 items 태그를 찾을 수 없습니다.")
            return

        data_list = []
        for item in items_node.findall('item'):
            data = {}
            for child in item:
                data[child.tag] = child.text.strip() if child.text else ''
            data_list.append(data)

        if not data_list:
            print("API에서 데이터를 받지 못했습니다.")
            return

        # MySQL CREATE TABLE 문 생성
        # XML 태그를 컬럼으로 사용
        columns = list(data_list[0].keys())
        # MySQL 호환 데이터 타입 및 백틱 사용
        column_defs = [f'`{col}` VARCHAR(255)' for col in columns]

        # UNIQUE 제약 조건 정의 (오류를 방지하기 위해 키 길이 지정)
        unique_columns = ['aptNm', 'buildYear', 'dealAmount', 'excluUseAr', 'floor', 'dealYear', 'dealMonth', 'dealDay']
        unique_constraint_cols = [f'`{col}`(255)' for col in unique_columns if f'{col}' in columns]
        unique_constraint = f'UNIQUE({", ".join(unique_constraint_cols)})'

        # f-string 오류가 수정된 부분
        create_table_sql = f'CREATE TABLE `apartment_trades` ({", ".join(column_defs)}, {unique_constraint});'

        # SQL 파일에 쓰기 시작
        with open(sql_path, 'w', encoding='utf-8') as f:
            f.write(create_table_sql + "\n\n")

            # INSERT 문 생성 및 쓰기
            insert_sql_header = f'INSERT INTO `apartment_trades` ({", ".join([f"`{col}`" for col in columns])}) VALUES'
            
            for i, row in enumerate(data_list):
                values = []
                for col in columns:
                    val = row.get(col)
                    if val is None:
                        values.append('NULL')
                    else:
                        # SQL 인젝션 방지를 위한 처리 및 특수 문자 이스케이프
                        values.append(f"'{val.replace('\'', '\\\'')}'")

                insert_line = f'({", ".join(values)})'
                if i < len(data_list) - 1:
                    insert_line += ','
                else:
                    insert_line += ';'
                
                # INSERT 문 합치기
                if i == 0:
                    f.write(insert_sql_header + "\n" + insert_line + "\n")
                else:
                    f.write(insert_line + "\n")

        print(f"Successfully generated {sql_path} with {len(data_list)} records.")

    except requests.exceptions.RequestException as e:
        print(f"API 요청 실패: {e}")
    except ET.ParseError as e:
        print(f"XML 파싱 실패: {e}")
    except Exception as e:
        print(f"예상치 못한 오류가 발생했습니다: {e}")

if __name__ == '__main__':
    fetch_and_generate_sql()
