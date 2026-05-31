# -*- coding: utf-8 -*-
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

if len(sys.argv) < 2:
    print("YYYYMM형식 필요")
    sys.exit(1)
def auto_encoding(path):
    with open(path, 'rb') as file:
        raw_data = file.read(100000)
    result = chardet.detect(raw_data)
    return result['encoding']
        

current_path =os.getcwd()
root = os.path.dirname(os.path.dirname(current_path))
raw_folder = "hdfs:///user/maria_dev/BDP/data/raw"
processed_folder = os.path.join(root, 'data','processed')

target_ym = sys.argv[1]
spark = SparkSession.builder.appName(f"spark_processing_{target_ym}").config("spark.sql.catalogImplementation","hive").enableHiveSupport().getOrCreate()

#spark.conf.set

bus_df = spark.read.option("encoding","cp949").csv(f"{raw_folder}/BUS_STATION_BOARDING_MONTH_{target_ym}.csv", header=True, inferSchema=True)
subway_df = spark.read.option("encoding","utf-8").csv(f"{raw_folder}/CARD_SUBWAY_MONTH_{target_ym}.csv", header=True, inferSchema=True)
weather_df = spark.read.option("encoding","cp949").csv(f"{raw_folder}/weather_data_{target_ym}.csv", header=True, inferSchema=True)
dust_df = spark.read.option("encoding","cp949").csv(f"{raw_folder}/dust_data_{target_ym}.csv", header=True, inferSchema=True)


# bus_df = spark.read.csv(f"{raw_folder}/BUS_STATION_BOARDING_MONTH_*.csv", header = True, inferSchema=True)
# subway_df = spark.read.csv(f"{raw_folder}/CARD_SUBWAY_MONTH_*.csv", header = True, inferSchema=True)
# weather_df = spark.read.csv(f"{raw_folder}/weather_data_*.csv", header = True, inferSchema=True)


# 다운 데이터는 사용일자 api는 USE_YMD
bus_df = bus_df.withColumn("사용일자",to_date(col("USE_YMD").cast("string"),"yyyyMMdd"))
subway_df = subway_df.withColumn("사용일자",to_date(col("사용일자").cast("string"),"yyyyMMdd"))
weather_df = weather_df.withColumn("TM",to_date(col("TM").cast("string"),"yyyyMMdd"))
dust_df = dust_df.withColumn("TM",to_date(substring(col("TM"), 1,8),"yyyyMMdd"))\
                                .withColumn("PM10",col("PM10").cast(IntegerType()))

#서울 지점 번호 108
seoul_weather = weather_df.filter(col("STN")==108).na.fill({"RN_DAY":0})
seoul_dust = dust_df.filter(col("STN_ID")==108).na.fill({"PM_10":0})

day_bus = bus_df.groupBy("사용일자").agg(
    sum("GTON_TNOPE").alias("GTON_TNOPE"),
    sum("GTOFF_TNOPE").alias("GTOFF_TNOPE")
).withColumn("버스승객수",  col("GTON_TNOPE")+col("GTOFF_TNOPE"))

day_subway = subway_df.groupBy("사용일자").agg(
    sum("승차총승객수").alias("승차총승객수"),
    sum("하차총승객수").alias("하차총승객수")
).withColumn("지하철승객수",  col("승차총승객수")+col("하차총승객수"))

day_dust = dust_df.groupBy("TM").agg(avg("PM10").alias("일평균PM10"))

merged_df = day_bus.join(day_subway,"사용일자","inner")\
                    .join(seoul_weather,day_bus.사용일자==seoul_weather.TM,"inner")\
                    .join(day_dust, "사용일자","left")

merged_df = merged_df.withColumn("IS_RAINY",
                                 when(col("RN_DAY")>=20, "많이 옴")
                                 .when(col("RN_DAY") > 0 ,"조금 옴")
                                 .otherwise("안 옴")
                                 ).withColumn("평일여부",
                                    when(dayofweek(col("사용일자")).isin([1,7]),"주말").otherwise("평일")
                                ).withColumn("황사등급",
                                    when(col("일평균PM10")<=30,"좋음")
                                    .when(col("일평균P10")<=80,"보통")
                                    .when(col("일평균PM10")<=150,"나쁨")
                                    .otherwise("매우 나쁨")
                                ).withColumn("악천후",
                                    when(col("TA_MAX")>=35,"폭염")
                                    .when(col("TA_MIN")<=-15,"한파")
                                    .otherwise("정상"))

merged_df = merged_df.withColumn("YYYYMM",date_format(col("사용일자"),"yyyyMM"))
result_df = merged_df.select(
    col("사용일자").alias("use_date"),
    col("RN_DAY").alias("rain_day"),
    col("IS_RAINY").alias("is_rainy"),
    col("버스승객수").alias("bus_passenger"),
    col("지하철승객수").alias("subway_passenger"),
    col("일평균PM10").alias("avg_pm10"),
    col("황사등급").alias("dust_grade"),
    col("평일여부").alias("is_weekday"),
    col("YYYYMM")
)

hive_db = "public_transport_weather"
hive_table="weather_pt_correlation"
full_table_name=f"{hive_db}.{hive_table}"

result_df.write.mode("append").format("csv").option("header","false").partitionBy("YYYYMM").saveAsTable(full_table_name)


save_path  = os.path.join(processed_folder, "Weather_PT_Correlation.csv")
result_df.to_csv(save_path, index=False, encoding = 'utf-8-sig')

print("processing complete")
spark.stop()
