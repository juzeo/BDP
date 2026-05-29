import pandas as pd
import requests
import os
from datetime import datetime, timedelta
import io
from dotenv import load_dotenv


load_dotenv()

weather_key = os.environ.get("WEATHER_API_KEY")
current_path =os.getcwd()
root = os.path.dirname(os.path.dirname(current_path))
# raw_folder = os.path.join("hdfs:///user/maria_dev/BDP/data/raw")
raw_folder = os.path.join(os.getcwd(),"data","raw")
# file_path = os.path.join(raw_folder,f"weather_data_{year}.csv")

# cols = ["TM_FC", "TM_EF", "TM_IN", "STN", "REG_ID", "WRN", "LVL", "CMD", "GRD", "CNT", "RPT"]
cols = ["TM","STN_ID","PM10","FLAG"]

#전날 데이터 가져오기
def get_dust_data_daily():
    target_date=(datetime.now()-timedelta(days=1)).strftime("%Y%m%d")
    year = target_date[:4]
    file_path = os.path.join(raw_folder,f"dust_data_{year}.csv")

    df = get_dust_data_range(target_date,target_date)
    df.to_csv(file_path, mode='a',index=False, header=not file_exists, encoding='utf-8')

# if __name__ =="__main__":
#     # get_dust_data_daily()
#     get_dust_data_range(20250101,20251231)


def get_dust_data_range(start_date,end_date):
    try:
        start = str(start_date)
        end = str(end_date)
        url = f"https://apihub.kma.go.kr/api/typ01/url/kma_pm10.php?tm1={start}0000&tm2={end}2359&authKey={weather_key}"
        response = requests.get(url)
        content = response.text.strip()

        # if not content or all(line.startswith('#') for line in content.splitlines()):
        #         return None
        lines = response.text.strip().splitlines()
        # print(lines)
        if not lines:
            return None
        data = [line.split() for line in lines if not line.startswith('#') and line.strip()]
        
        result =[]
        for row in data:
            if len(row)  >=5:
                result.append(row[:4])
            else:
                result.append(row)
            print(f"{row[0]} 수집완료")
        print(f"result : {result}")
        
    except Exception as e:
        print(f"날씨 데이터 요청 중 오류 발생: {e}")

    df = pd.DataFrame(result, columns=cols)

    return df

result = get_dust_data_range(20230101,20231231)
file_path = os.path.join(raw_folder,f"dust_data_2023.csv")
print(result.head())
file_exists = os.path.isfile(file_path)

result.to_csv(file_path, mode='a',index=False, header=not file_exists, encoding='utf-8')
