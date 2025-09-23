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
        columns = list(data_list[0].keys())
        column_defs = [f'`{col}` VARCHAR(255)' for col in columns]

        # UNIQUE 제약 조건 정의 (필요한 컬럼만 포함하여 키 길이 제한 해결)
        unique_columns = ['aptNm', 'dealYear', 'dealMonth', 'dealDay', 'excluUseAr', 'floor']
        unique_constraint_cols = [f'`{col}`(191)' for col in unique_columns if f'{col}' in columns]
        unique_constraint = f'UNIQUE({", ".join(unique_constraint_cols)})'

        create_table_sql = f'CREATE TABLE `apartment_trades` ({", ".join(column_defs)}, {unique_constraint});'

        # SQL 파일에 쓰기 시작
        with open(sql_path, 'w', encoding='utf-8') as f:
            f.write(create_table_sql + "\n\n")

            insert_sql_header = f'INSERT INTO `apartment_trades` ({", ".join([f"`{col}`" for col in columns])}) VALUES'
            
            for i, row in enumerate(data_list):
                values = []
                for col in columns:
                    val = row.get(col)
                    if val is None:
                        values.append('NULL')
                    else:
                        escaped_val = val.replace("'", "''")
                        values.append(f"'{escaped_val}'")

                insert_line = f'({", ".join(values)})'
                if i < len(data_list) - 1:
                    insert_line += ','
                else:
                    insert_line += ';'
                
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
