import io
import os
import requests
import pandas as pd
from datetime import datetime, timedelta

current_path =os.getcwd()
root = os.path.dirname(os.path.dirname(current_path))
raw_folder = os.path.join("hdfs:///user/maria_dev/BDP/data/raw")
weather_key = os.environ.get("WEATHER_API_KEY")


cols = ["TM","STN","WS_AVG","WR_DAY","WD_MAX","WS_MAX","WS_MAX_TM","WD_INS",
        "WS_INS","WS_INS_TM","TA_AVG","TA_MAX","TA_MAX_TM","TA_MIN","TA_MIN_TM",
        "TD_AVG","TS_AVG","TG_MIN","HM_AVG","HM_MIN","HM_MIN_TM","PV_AVG","EV_S",
        "EV_L","FG_DUR","PA_AVG","PS_AVG","PS_MAX","PS_MAX_TM","PS_MIN","PS_MIN_TM",
        "CA_TOT","SS_DAY","SS_DUR","SS_CMB","SI_DAY","SI_60M_MAX","SI_60M_MAX_TM",
        "RN_DAY","RN_D99","RN_DUR","RN_60M_MAX","RN_60M_MAX_TM","RN_10M_MAX","RN_10M_MAX_TM",
        "RN_POW_MAX","RN_POW_MAX_TM","SD_NEW","SD_NEW_TM","SD_MAX","SD_MAX_TM",
        "TE_05","TE_10","TE_15","TE_30","TE_50"
]
#일자료
def get_weather_data(target_date):
    
    try:
        url = f"https://apihub.kma.go.kr/api/typ01/url/kma_sfcdd.php?tm={target_date}&stn=0&help=1&authKey={weather_key}"
        response = requests.get(url)
        content = response.text.strip()

        if not content or all(line.startswith('#') for line in content.splitlines()):
            return None

    except Exception as e:
        print(f"날씨 데이터 요청 중 오류 발생: {e}")
        
    
    df = pd.read_csv(io.StringIO(response.text), comment='#', header=None)
    if df.iloc[:,-1].isnull().all():
        df = df.iloc[:,:-1] #마지막열 삭제(결측치)
    df.columns = cols
    # df =response.text

    return df
def get_weather_data_range(start_date, end_date):
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    date_list =[]
    df_list=[]
    for i in range((end-start).days+1):
        date_list.append((start+timedelta(days=i)).strftime("%Y%m%d"))

    for date in date_list:
        df =get_weather_data(date)
        if df is not None and len(df.columns) == len(cols):
            df_list.append(df)
            print(f"{date} 수집완료")
        else:
            print(f"{date} 데이터 형식 오류, {df}")
    if(df_list):
        result_df = pd.concat(df_list, ignore_index=True)
        return result_df
    

#전날 데이터 가져오기
def get_weather_data_daily():
    target_date=(datetime.now()-timedelta(days=1)).strftime("%Y%m%d")
    year = target_date[:4]
    file_path = os.path.join(raw_folder,f"weather_data_{year}.csv")

    df = get_weather_data(target_date)
    df.to_csv(file_path, mode='a',index=False, header=not file_exists, encoding='utf-8')

if __name__ =="__main__":
    get_weather_data_daily()
# result = get_weather_data_range("20260527", "20260527")
# print(result.head())
# result.to_csv("weather_data_2026.csv", index=False, encoding='utf-8')
