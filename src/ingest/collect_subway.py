from datetime import datetime, timedelta
import requests
import pandas as pd
import time
import os
import subprocess
from dotenv import load_dotenv
import calendar
import sys

load_dotenv()

current_path =os.getcwd()
root = os.path.dirname(os.path.dirname(current_path))

seoul_subway_key = os.environ.get("SEOUL_SUBWAY_API_KEY")
current_path =os.getcwd()
raw_folder = os.path.join(os.getcwd(),"data","raw")


COLUMN_MAPPING = {
    "사용일자": "USE_YMD",        # API에서는 실제 일자 데이터가 이 키에 담김
    "노선명": "LINE_NUM",
    "역명": "SUB_STA_NM",
    "등록일자": "REG_DATE",
    "승차총승객수": "GTON_TNOPE",
    "하차총승객수": "GTOFF_TNOPE"
}

def get_subway_data(target_day):

    start=1
    end = 1000
    all_data = []
    while True:
        url = f"http://openapi.seoul.go.kr:8088/{seoul_subway_key}/json/CardSubwayStatsNew/1/1000/{target_day}"
        try:
            response = requests.get(url)
            data = response.json()

            
            if start ==1:
                total_count=data["CardSubwayStatsNew"]['list_total_count']
                print(f"총 데이터 수: {total_count}")
            
            rows = data["CardSubwayStatsNew"]['row']
            all_data.extend(rows)
            
            if len(rows)<1000 or (start +len(rows)-1)>=total_count:
                break

            start += 1000
            end += 1000

            time.sleep(0.2)
        except Exception as e:
            print(f"에러 {e}")
            time.sleep(5)
            continue

    return pd.DataFrame(all_data)
    
def get_subway_data_range(start_date, end_date):
    start = datetime.strptime(start_date, '%Y%m%d')
    end = datetime.strptime(end_date, '%Y%m%d')
    

    dates = [(start+timedelta(days=i)).strftime("%Y%m%d")for i in range((end - start).days+1)]
    
    three_month_ago = (datetime.now() - timedelta(days=90)).strftime("%Y%m")
    target_month = start_date[:6]

    if target_month < three_month_ago:
        downloaded_file = os.path.join(raw_folder, f"CARD_SUBWAY_MONTH_{target_month}.csv")
        
        if os.path.exists(downloaded_file):
            old_df = pd.read_csv(downloaded_file, encoding='utf-8')

            old_df = old_df.rename(columns=COLUMN_MAPPING)

            cols = ["USE_YMD", "LINE_NUM","SUB_STA_NM", "REG_DATE","GTON_TNOPE","GTOFF_TNOPE"]
            old_df = old_df[cols]
            return  old_df[cols]
        else:
            print(f"로컬에 {downloaded_file}이 없음")
            return None
    else:
        df_list=[]
        for date in dates:
            df = get_subway_data(date)

            if df is not None:
                if df is not None and not df.empty:
                    df_list.append(df)
                
                print(f"{date} 수집완료")
            else:
                print(f"{date} 데이터 형식 오류, {df}")
        return pd.concat(df_list, ignore_index=True) if df_list else None

    


def get_subway_data_month(start_date, end_date):
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
        df = get_subway_data_range(request_start, request_end)

        if df is not None and not df.empty:
            file_path = os.path.join(raw_folder,f"CARD_SUBWAY_MONTH_{date}.csv")
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
    print(f"{target_month} 지하철 데이터 수집 시작")
    get_subway_data_month(target_month,target_month)
    print(f"{target_month} 지하철 데이터 수집 완료")

