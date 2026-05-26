from datetime import datetime, timedelta
import requests
import pandas as pd
import time
import os
from dotenv import load_dotenv

load_dotenv()


def get_bus_data(target_day):

    start=1
    end = 1000
    all_data = []
    while True:
        url = base_url = f"http://openapi.seoul.go.kr:8088/{seoul_bus_key}/json/CardBusStatisticsServiceNew/{start}/{end}/{target_day}"
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

        # time.sleep(0.5)
    
    if all_data:
        df = pd.DataFrame(all_data)
        return df
    
def get_bus_data_range(start_date, end_date, weekend_only = False):
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

df = get_bus_data_range("20250101","20250102", weekend_only=False)
df.to_csv(f"{__file__}/data/raw/bus_data_202501.csv",index=False, encoding='utf-8')
print("저장완료")

