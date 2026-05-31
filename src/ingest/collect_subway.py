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
# raw_folder = os.path.join("hdfs:///user/maria_dev/BDP/data/raw")
seoul_subway_key = os.environ.get("SEOUL_SUBWAY_API_KEY")
current_path =os.getcwd()
raw_folder = os.path.join(os.getcwd(),"data","raw")
# file_path = os.path.join(raw_folder,f"weather_data_{year}.csv")


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
                total_count=data["CardBusStatisticsServiceNew"]['list_total_count']
                print(f"총 데이터 수: {total_count}")
            
            rows = data["CardBusStatisticsServiceNew"]['row']
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
    
# def get_bus_data_range(start_date, end_date, weekend_only = False):
    start = datetime.strptime(start_date, '%Y%m%d')
    end = datetime.strptime(end_date, '%Y%m%d')
    
    date_list=[]
    df_list =[]

    for i in range((end-start).days +1):
        date_list.append((start+timedelta(days=i)).strftime("%Y%m%d"))

    for date in date_list:
        if weekend_only:
            date_obj = datetime.strptime(date, "%Y%m%d")
            #weekday()의 결과는 0(월)부터 6(일)
            if date_obj.weekday()<5:
                continue
        df = get_bus_data(date)

        if df is not None:
            df_list.append(df)
            print(f"{date} 수집완료")
        else:
            print(f"{date} 데이터 형식 오류, {df}")
    if(df_list):
        result_df = pd.concat(df_list, ignore_index=True)
        return result_df
    else:
        return None

def get_subway_data_range(start_date, end_date):
    start = datetime.strptime(start_date, '%Y%m%d')
    end = datetime.strptime(end_date, '%Y%m%d')
    
    # df_list =[]

    # for i in range((end-start).days +1):
    #     date = (start+timedelta(days=i)).strftime("%Y%m%d")

    #     df = get_bus_data(date)

    #     if df is not None:
    #         df_list.append(df)
    #         print(f"{date} 수집완료")
    #     else:
    #         print(f"{date} 데이터 형식 오류, {df}")
    # if df_list:
    #     result_df = pd.concat(df_list, ignore_index=True)
    #     return result_df
    # else:
    #     return None
    dates = [(start+timedelta(days=i)).strftime("%Y%m%d")for i in range((end - start).days+1)]
    
    month_data = {}

    for date in dates:
        month_name = date[:6]
        df = get_subway_data(date)

        if df is not None:
            if month_name not in month_data:
                month_data[month_name]=[]
            month_data[month_name].append(df)
            print(f"{date} 수집완료")
        else:
            print(f"{date} 데이터 형식 오류, {df}")
    for month, df_list in month_data.items():
        if df_list:
            result = pd.concat(df_list, ignore_index=True)
            file_path = os.path.join(raw_folder,f"CARD_SUBWAY_MONTH_{month}.csv")
            result.to_csv(file_path, index=False, encoding='utf-8')
            hdfs_dir = "user/maria_dev/BDP/data/raw"

            hdfs_commnad=f"hdfs dfs -put {file_path} {hdfs_dir}"
            
            try:
                subprocess.run(hdfs_commnad, shell=True, check=True)
                print("HDFS 적재완료")

            except subprocess.CalledProcessError as e:
                print(f"HDFS 적재 실패: {e}")
                
            
            print(f"{month} 저장 완료")

    

def get_subway_data_daily():
    target_date = (datetime.now()-timedelta(days=1)).strftime("%Y%m%d")
    year_month = target_date[:6]
    file_path = os.path.join(raw_folder,f"bus_data_{year_month}.csv")

    df = get_subway_data(target_date)

    if df is not None:
        file_exists = os.path.isfile(file_path)
        df.to_csv(file_path, mode='a',index=False, header=not file_exists, enccoding='utf-8')
        
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
        df = get_subway_data_range(request_start, request_end)

        if df is not None and not df.empty:
            file_path = os.path.join(raw_folder,f"bus_data_{date}.csv")
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
    get_bus_data_month(target_month,target_month)
    print(f"{target_month} 지하철 데이터 수집 완료")
# get_bus_data_month(202601,202601)

# file_path = os.path.join(raw_folder,f"BUS_STATION_BOARDING_MONTH_202501.csv")

# df.to_csv(f"{file_path}",index=False, encoding='utf-8')
# print("저장완료")
