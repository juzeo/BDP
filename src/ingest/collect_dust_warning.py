import pandas as pd
import requests
import os
from datetime import datetime, timedelta
import io
from dotenv import load_dotenv
import calendar
import subprocess
import sys


load_dotenv()

weather_key = os.environ.get("WEATHER_API_KEY")
current_path =os.getcwd()
root = os.path.dirname(os.path.dirname(current_path))
raw_folder = os.path.join(os.getcwd(),"data","raw")


cols = ["TM","STN_ID","PM10","FLAG"]


def get_dust_data_range(start_date,end_date):
    try:
        start = str(start_date)
        end = str(end_date)
        url = f"https://apihub.kma.go.kr/api/typ01/url/kma_pm10.php?tm1={start}0000&tm2={end}2359&authKey={weather_key}"
        response = requests.get(url)
        content = response.text.strip()

        if not content or all(line.startswith('#') for line in content.splitlines()):
                return None

        if not content:
            return None
        data = [line.split() for line in content.splitlines() if not line.startswith('#') and line.strip()]

        result =[]
        for row in data:
            if len(row)  >=5:
                result.append(row[:4])
            else:
                result.append(row)
            print(f"{row[0]} 수집완료")
        df = pd.DataFrame(result, columns = cols)
        print(f"result : {result}")
        return df
        
    except Exception as e:
        print(f"날씨 데이터 요청 중 오류 발생: {e}")



def get_dust_data_month(start_date, end_date):
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
        df = get_dust_data_range(request_start, request_end)

        if df is not None and not df.empty:
            file_path = os.path.join(raw_folder,f"dust_data_{date}.csv")
            df.to_csv(file_path, index=False, encoding='utf-8')
            print(f"{date} 저장완료")

            hdfs_dir = "user/maria_dev/BDP/data/raw"

            hdfs_commnad=f"hdfs dfs -put {file_path} {hdfs_dir}"
            
            try:
                subprocess.run(hdfs_commnad, shell=True, check=True)
                print("HDFS 적재완료")

            except subprocess.CalledProcessError as e:
                print(f"HDFS 적재 실패: {e}")

        if month == 12:
            start = start.replace(year = year +1, month =1)
        else:
            start = start.replace(month = month+1)

if __name__ == "__main__":
    target_month = sys.argv[1]
    print(f"{target_month} 황사 데이터 수집 시작")
    get_dust_data_month(target_month,target_month)
    print(f"{target_month} 황사 데이터 수집 완료")
