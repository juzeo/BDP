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

app_name_str = "spark_processing_{}".format(target_ym)

spark = (SparkSession.builder
    .appName(app_name_str)
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.hadoop.hive.metastore.uris", "thrift://sandbox-hdp.hortonworks.com:9083")
    .enableHiveSupport()
    .getOrCreate())

#    .config("javax.jdo.option.ConnectionURL", "jdbc:mysql://localhost:3306/hive?createDatabaseIfNotExist=true") \
#    .config("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver") \
#    .config("javax.jdo.option.ConnectionUserName", "hive") \
#    .config("javax.jdo.option.ConnectionPassword", "hive") \
#    .config("spark.sql.warehouse.dir", "hdfs:///apps/hive/warehouse") \

spark.sql("CREATE DATABASE IF NOT EXISTS public_transport_weather")
spark.sql("USE public_transport_weather")

#spark = SparkSession.builder.appName(f"spark_processing_ALL)").config("spark.sql.catalogImplementation", "hive").enableHiveSupport().getOrCreate()

bus_df = spark.read.csv(f"{raw_folder}/BUS_STATION_BOARDING_MONTH_{target_ym}.csv", header=True, inferSchema=True)
subway_df = spark.read.csv(f"{raw_folder}/CARD_SUBWAY_MONTH_{target_ym}.csv", header=True, inferSchema=True)
weather_df = spark.read.csv(f"{raw_folder}/weather_data_{target_ym}.csv", header=True, inferSchema=True)
dust_df = spark.read.csv(f"{raw_folder}/dust_data_{target_ym}.csv", header=True, inferSchema=True)


# bus_df = spark.read.csv(f"{raw_folder}/BUS_STATION_BOARDING_MONTH_*.csv", header = True, inferSchema=True)
# subway_df = spark.read.csv(f"{raw_folder}/CARD_SUBWAY_MONTH_*.csv", header = True, inferSchema=True)
# weather_df = spark.read.csv(f"{raw_folder}/weather_data_*.csv", header = True, inferSchema=True)


# 다운 데이터는 사용일자 api는 USE_YMD
if "USE_YMD" in subway_df.columns:
    subway_df = subway_df.withColumn("사용일자", to_date(col("USE_YMD").cast("string"), "yyyyMMdd"))
elif "사용일자" in subway_df.columns:
    subway_df = subway_df.withColumn("사용일자", to_date(col("사용일자").cast("string"), "yyyyMMdd"))

bus_df = bus_df.withColumn("사용일자",to_date(col("USE_YMD").cast("string"),"yyyyMMdd"))
#subway_df = subway_df.withColumn("사용일자",to_date(col("USE_YMD").cast("string"),"yyyyMMdd"))
weather_df = weather_df.withColumn("TM",to_date(col("TM").cast("string"),"yyyyMMdd"))
weather_df = weather_df.withColumn("TA_MAX", col("TA_MAX").cast("double")) \
                       .withColumn("TA_MIN", col("TA_MIN").cast("double"))
dust_df = dust_df.withColumn("TM", to_date(
    substring(col("TM").cast("string"), 1, 8), 
    "yyyyMMdd"
)).withColumn("PM10", regexp_extract(col("PM10").cast("string"), r"(\d+)", 1).cast(IntegerType()))

#서울 지점 번호 108
seoul_weather = weather_df.filter(col("STN")==108).na.fill({"RN_DAY":0})
seoul_dust = dust_df.filter(col("STN_ID")=="108,").na.fill({"PM10":0})

day_bus = bus_df.groupBy("사용일자").agg(
    sum("GTON_TNOPE").alias("GTON_TNOPE"),
    sum("GTOFF_TNOPE").alias("GTOFF_TNOPE")
).withColumn("버스승객수",  col("GTON_TNOPE")+col("GTOFF_TNOPE"))

if "GTON_TNOPE" in subway_df.columns:
    day_subway = subway_df.groupBy("사용일자").agg(
        sum("GTON_TNOPE").alias("지하철_승차"),
        sum("GTOFF_TNOPE").alias("지하철_하차")
    ).withColumn("지하철승객수", col("지하철_승차") + col("지하철_하차"))
elif "승차총승객수" in subway_df.columns:
    day_subway = subway_df.groupBy("사용일자").agg(
        sum("승차총승객수").alias("지하철_승차"),
        sum("하차총승객수").alias("지하철_하차")
    ).withColumn("지하철승객수", col("지하철_승차") + col("지하철_하차"))

day_dust = seoul_dust.groupBy("TM").agg(avg("PM10").alias("일평균PM10"))

base_df = day_bus.join(day_subway, "사용일자", "inner")
weather_condition = (col("사용일자") == seoul_weather["TM"])
dust_condition = (col("사용일자") == day_dust["TM"])
merged_df = (base_df.join(seoul_weather, weather_condition, "inner")
                    .join(day_dust, dust_condition, "left"))
#merged_df = day_bus.join(day_subway,"사용일자","inner")\
#                    .join(seoul_weather,day_bus.사용일자==seoul_weather.TM,"inner")\
#                    .join(day_dust, day_bus.사용일자==day_dust.TM,"left")

merged_df = merged_df.withColumn("IS_RAINY",
                                 when(col("RN_DAY")>=20, "많이 옴")
                                 .when(col("RN_DAY") > 0 ,"조금 옴")
                                 .otherwise("안 옴")
                                 ).withColumn("평일여부",
                                    when(dayofweek(col("사용일자")).isin([1,7]),"주말").otherwise("평일")
                                ).withColumn("황사등급",
                                    when(col("일평균PM10")<=30,"좋음")
                                    .when(col("일평균PM10")<=80,"보통")
                                    .when(col("일평균PM10")<=150,"나쁨")
                                    .otherwise("매우 나쁨")
                                ).withColumn("악천후",
                                    when(col("TA_MAX")>=33,"폭염")
                                    .when(col("TA_MIN")<=-12,"한파")
                                    .otherwise("정상"))

merged_df = merged_df.withColumn("yyyymm",date_format(col("사용일자"),"yyyyMM"))
result_df = merged_df.select(
    col("사용일자").alias("use_ymd"),
    col("RN_DAY").alias("rn_day"),
    col("IS_RAINY").alias("is_rainy"),
    col("버스승객수").alias("bus_passenger"),
    col("지하철승객수").alias("subway_passenger"),
    col("일평균PM10").alias("avg_pm10"),
    col("황사등급").alias("dust_grade"),
    col("평일여부").alias("is_weekday"),
    col("악천후").alias("severe_weather"),
    col("yyyymm")
)

hdfs_output_path = "hdfs:///warehouse/tablespace/external/hive/weather_pt_correlation"
result_df.write.mode("append").format("csv").option("header","false").partitionBy("yyyymm").save(hdfs_output_path)
#result_df.write.mode("append").insertInto(full_table_name)

save_path  = os.path.join(processed_folder, "Weather_PT_Correlation.csv")
#result_df.to_csv(save_path, index=False, encoding = 'utf-8-sig')
try:
    spark.sql("ALTER TABLE weather_pt_correlation ADD IF NOT EXISTS PARTITION (yyyymm='{}')".format(target_ym))
except Exception as e:
    pass


spark.stop()
