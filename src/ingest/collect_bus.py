from datetime import datetime, timedelta
import requests
import pandas as pd
import time
import os
from dotenv import load_dotenv

load_dotenv()

current_path =os.getcwd()
root = os.path.dirname(os.path.dirname(current_path))
# raw_folder = os.path.join("hdfs:///user/maria_dev/BDP/data/raw")
seoul_bus_key = os.environ.get("SEOUL_BUS_API_KEY")
current_path =os.getcwd()
raw_folder = os.path.join(os.getcwd(),"data","raw")
# file_path = os.path.join(raw_folder,f"weather_data_{year}.csv")


def get_bus_data(target_day):

    start=1
    end = 1000
    all_data = []
    while True:
        url = f"http://openapi.seoul.go.kr:8088/{seoul_bus_key}/json/CardBusStatisticsServiceNew/{start}/{end}/{target_day}"
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

def get_bus_data_range(start_date, end_date):
    start = datetime.strptime(start_date, '%Y%m%d')
    end = datetime.strptime(end_date, '%Y%m%d')
    
    df_list =[]

    for i in range((end-start).days +1):
        date = (start+timedelta(days=i)).strftime("%Y%m%d")

        df = get_bus_data(date)

        if df is not None:
            df_list.append(df)
            print(f"{date} 수집완료")
        else:
            print(f"{date} 데이터 형식 오류, {df}")
    if df_list:
        result_df = pd.concat(df_list, ignore_index=True)
        return result_df
    else:
        return None

def get_bus_data_daily():
    target_date = (datetime.now()-timedelta(days=1)).strftime("%Y%m%d")
    year_month = target_date[:6]
    file_path = os.path.join(raw_folder,f"bus_data_{year_month}.csv")

    df = get_bus_data(target_date)

    if df is not None:
        file_exists = os.path.isfile(file_path)
        df.to_csv(file_path, mode='a',index=False, header=not file_exists, enccoding='utf-8')
        


# if __name__ == "__main__":
#     get_bus_data_daily()
df = get_bus_data_range("20241201","20241231")
file_path = os.path.join(raw_folder,f"BUS_STATION_BOARDING_MONTH_202412.csv")

df.to_csv(f"{file_path}",index=False, encoding='utf-8')
print("저장완료")
