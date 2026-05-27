# -*- coding: utf-8 -*-
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def auto_encoding(path):
    with open(path, 'rb') as file:
        raw_data = file.read(100000)
    result = chardet.detect(raw_data)
    return result['encoding']
        

current_path =os.getcwd()
root = os.path.dirname(os.path.dirname(current_path))
raw_folder = os.path.join("hdfs:///user/maria_dev/BDP/data/raw")
processed_folder = os.path.join(root, 'data','processed')

spark = SparkSession.builder.appName("spark_processing").getOrCreate()

spark.conf.set

bus_df = spark.read.option("encoding","cp949").csv(f"{raw_folder}/BUS_STATION_BOARDING_MONTH_*.csv", header=True, inferSchema=True)
subway_df = spark.read.option("encoding","utf-8").csv(f"{raw_folder}/CARD_SUBWAY_MONTH_*.csv", header=True, inferSchema=True)
weather_df = spark.read.option("encoding","cp949").csv(f"{raw_folder}/weather_data_*.csv", header=True, inferSchema=True)

# bus_df = spark.read.csv(f"{raw_folder}/BUS_STATION_BOARDING_MONTH_*.csv", header = True, inferSchema=True)
# subway_df = spark.read.csv(f"{raw_folder}/CARD_SUBWAY_MONTH_*.csv", header = True, inferSchema=True)
# weather_df = spark.read.csv(f"{raw_folder}/weather_data_*.csv", header = True, inferSchema=True)


# 다운 데이터는 사용일자 api는 USE_YMD
bus_df = bus_df.withColumn("사용일자",to_date(col("사용일자").cast("string"),"yyyyMMdd"))
subway_df = subway_df.withColumn("사용일자",to_date(col("사용일자").cast("string"),"yyyyMMdd"))
weather_df = weather_df.withColumn("TM",to_date(col("TM").cast("string"),"yyyyMMdd"))

#서울 지점 번호 108
seoul_weather = weather_df.filter(col("STN")==108).na.fill({"RN_DAY":0})

day_bus = bus_df.groupBy("사용일자").agg(
    sum("승차총승객수").alias("승차총승객수"),
    sum("하차총승객수").alias("하차총승객수")
).withColumn("버스승객수",  col("승차총승객수")+col("하차총승객수"))

day_subway = subway_df.groupBy("사용일자").agg(
    sum("승차총승객수").alias("승차총승객수"),
    sum("하차총승객수").alias("하차총승객수")
).withColumn("지하철승객수",  col("승차총승객수")+col("하차총승객수"))
merged_df = day_bus.join(day_subway,"사용일자","inner")\
                    .join(seoul_weather,day_bus.사용일자==seoul_weather.TM,"inner")

merged_df = merged_df.withColumn("IS_RAINY",
                                 when(col("RN_DAY")>=20, "많이 옴")
                                 .when(col("RN_DAY") > 0 ,"조금 옴")
                                 .otherwise("안 옴")
                                 ).withColumn("평일여부",
                                              when(dayofweek(col("사용일자")).isin([1,7]),"주말").otherwise("평일")
                                              )
result_pandas_df = merged_df.select("사용일자","RN_DAY","IS_RAINY","평일여부","버스승객수","지하철승객수").toPandas()

save_path  = os.path.join(processed_folder, "Weather_PT_Correlation.csv")
result_pandas_df.to_csv(save_path, index=False, encoding = 'utf-8-sig')

print("processing complete")
spark.stop()
