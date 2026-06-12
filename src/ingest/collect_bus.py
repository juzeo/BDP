from datetime import datetime, timedelta
import requests
import pandas as pd
import time
import os
import subprocess
from dotenv import load_dotenv
import calendar
import sys

# 환경 변수 로드
load_dotenv()

# 경로 설정
current_path =os.getcwd()
root = os.path.dirname(os.path.dirname(current_path))
seoul_bus_key = os.environ.get("SEOUL_BUS_API_KEY")
current_path =os.getcwd()
raw_folder = os.path.join(os.getcwd(),"data","raw")

# 특정 날짜 데이터 가져오기
def get_bus_data(target_day):

    start=1
    end = 1000
    all_data = []
    while True:
        url = f"http://openapi.seoul.go.kr:8088/{seoul_bus_key}/json/CardBusStatisticsServiceNew/{start}/{end}/{target_day}"
        try:
            response = requests.get(url)
            data = response.json()

            # 척페이지 호출시 전체 데이터 개수 출력
            if start ==1:
                total_count=data["CardBusStatisticsServiceNew"]['list_total_count']
                print(f"총 데이터 수: {total_count}")
            
            # 실제 데이터 추출 및 리스트 추가
            rows = data["CardBusStatisticsServiceNew"]['row']
            all_data.extend(rows)
            
            # 가져온 데이터가 1000건 미만이거나 이미 다 가져왔으면 반복문 중단
            if len(rows)<1000 or (start +len(rows)-1)>=total_count:
                break

            # 다음 1000건을 위해 인덱스 증가
            start += 1000
            end += 1000

            time.sleep(0.2)
        except Exception as e:
            print(f"에러 {e}")
            time.sleep(5)
            continue

    return pd.DataFrame(all_data)
    

# 시각일부터 종료일까지 날짜별로 get_bus_data() 함수 호출하고 하나의 DataFrame으로 병합하여 반환
def get_bus_data_range(start_date, end_date):
    start = datetime.strptime(start_date, '%Y%m%d')
    end = datetime.strptime(end_date, '%Y%m%d')

    # 시작일부터 종료일까지 날짜 리스트 생성
    dates = [(start+timedelta(days=i)).strftime("%Y%m%d")for i in range((end - start).days+1)]
    
    df_list=[]

    for date in dates:
        df = get_bus_data(date)

        if df is not None and not df.empty:
            df_list.append(df)
            print(f"{date} 수집완료")
        else:
            print(f"{date} 데이터 형식 오류, {df}")
    return pd.concat(df_list, ignore_index=True) if df_list else None
    
# 월 단위로 범위를 입력 받아 월별 데이터를 수집한 뒤 로컬 csv 저장 및 HDFS에 적재
def get_bus_data_month(start_date, end_date):
    start = datetime.strptime(str(start_date), "%Y%m")
    end = datetime.strptime(str(end_date), "%Y%m")

    
    while start <= end:
        year = start.year
        month = start.month
        date = start.strftime("%Y%m")

        last_day = calendar.monthrange(year, month)[1]
        request_start = f"{date}01"
        request_end = f"{date}{last_day}"
        print(start)
        df = get_bus_data_range(request_start, request_end)

        if df is not None and not df.empty:
            file_path = os.path.join(raw_folder,f"BUS_STATION_BOARDING_MONTH_{date}.csv")
            df.to_csv(file_path, index=False, encoding='utf-8')
            print(f"{date} 저장완료")

            # HDFS 적재
            hdfs_dir = "user/maria_dev/BDP/data/raw"

            hdfs_commnad=f"hdfs dfs -put {file_path} {hdfs_dir}"
            
            try:
                # 셀 명령어로 put
                subprocess.run(hdfs_commnad, shell=True, check=True)
                print("HDFS 적재완료")

            except subprocess.CalledProcessError as e:
                print(f"HDFS 적재 실패: {e}")
        # 다음 년도로 이동 계산
        if month == 12:
            start = start.replace(year = year +1, month =1)
        else:
            start = start.replace(month = month+1)


if __name__ == "__main__":
    target_month = sys.argv[1]
    print(f"{target_month} 버스 데이터 수집 시작")
    get_bus_data_month(target_month,target_month)
    print(f"{target_month} 버스 데이터 수집 완료")
