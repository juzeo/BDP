# -*- coding: utf-8 -*-
import os
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from matplotlib.ticker import FuncFormatter


def comma_formatter(x, pos):
	return f"{int(x):,}"

plt.rcParams['font.family'] = 'NanumGothic'
#spark = SparkSession.builder.appName(f"Hive_Data_Visualization").config("spark.sql.catalogImplementation","hive").enableHiveSupport().getOrCreate()

spark = (SparkSession.builder
    .appName("Hive_Data_Visualization")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.hadoop.hive.metastore.uris", "thrift://sandbox-hdp.hortonworks.com:9083")
    .enableHiveSupport()
    .getOrCreate())

save_path = os.path.join(os.getcwd(),"data","output")
if not os.path.exists(save_path):
    os.makedirs(save_path)

spark.sql("USE public_transport_weather")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS weather_pt_correlation (
  use_ymd STRING,
  rn_day DOUBLE,
  is_rainy STRING,
  bus_passenger BIGINT,
  subway_passenger BIGINT,
  avg_pm10 DOUBLE,
  dust_grade STRING,
  is_weekday STRING,
  severe_weather STRING
) PARTITIONED BY (yyyymm STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'
LOCATION 'hdfs:///warehouse/tablespace/external/hive/weather_pt_correlation'
""")

spark.sql("MSCK REPAIR TABLE weather_pt_correlation")

df = spark.table("weather_pt_correlation")
# 평일 기준 비랑 눈은 대중교통 승하차량에 영향을 줄까
analysis_1 = df.filter(col("is_weekday")=="평일")\
            .groupBy("is_rainy")\
            .agg(round(avg('bus_passenger'),0).alias('avg_bus'),
                 round(avg('subway_passenger'),0).alias('avg_subway'))
analysis_1 = analysis_1.withColumn('avg_total', col('avg_bus') + col('avg_subway'))
select = analysis_1.select(
    col('is_rainy'),
    expr("stack(3,'bus',avg_bus,'subway',avg_subway,'total',avg_total) as (transport,passenger)")
)

result_df = select.toPandas()


result_df['is_rainy']=pd.Categorical(result_df['is_rainy'],categories=['많이 옴','조금 옴','안 옴'])

result_df.to_csv(os.path.join(save_path, "rainy_transport_data.csv"), index=False, encoding="utf-8")

plt.figure(figsize=(11,6))
sns.barplot(x='is_rainy',y='passenger', hue='transport',data=result_df)

#ax.yaxis.set_major_formatter(FuncFormatter(comma_formatter))
plt.gca().yaxis.set_major_formatter(FuncFormatter(comma_formatter))
plt.title('평일 기준 비랑 눈은 대중교통 승하차량에 영향을 줄까')
plt.xlabel('비 상태')
plt.ylabel('평균 승객')

file_name = "rainy_transport_plot.png"
result_path = os.path.join(save_path,file_name)
plt.savefig(result_path,dpi=300)
plt.close()

# 주말 기준 폭염과 한파는 대중교통 승하차량에 영향을 줄까
analysis_2 = df.filter(col("is_weekday")=="주말")\
            .groupBy("severe_weather")\
            .agg(round(avg('bus_passenger'),0).alias('avg_bus'),
                 round(avg('subway_passenger'),0).alias('avg_subway'))
analysis_2 = analysis_2.withColumn('avg_total', col('avg_bus') + col('avg_subway'))
select = analysis_2.select(
    col('severe_weather'),
    expr("stack(3,'bus',avg_bus,'subway',avg_subway,'total',avg_total) as (transport,passenger)")
)

result_df = select.toPandas()

result_df.to_csv(os.path.join(save_path, "severe_weather_transport_data.csv"), index=False, encoding="utf-8")
plt.figure(figsize=(11,6))
sns.barplot(x='severe_weather',y='passenger', hue='transport',data=result_df)

#ax.yaxis.set_major_formatter(FuncFormatter(comma_formatter))
plt.gca().yaxis.set_major_formatter(FuncFormatter(comma_formatter))
plt.title('주말 기준 폭염과 한파는 대중교통 승하차량에 영향을 줄까')
plt.xlabel('날씨 상태')
plt.ylabel('평균 승객')

file_name = "severe_weather_transport_plot.png"
result_path = os.path.join(save_path,file_name)
plt.savefig(result_path,dpi=300)
plt.close()


# 주말 기준 미세먼지는 주말 승하차량에 영향을 미칠까
analysis_3 = df.filter(col("is_weekday")=="주말")\
            .groupBy("dust_grade")\
            .agg(round(avg('bus_passenger'),0).alias('avg_bus'),
                 round(avg('subway_passenger'),0).alias('avg_subway'))
analysis_3 = analysis_3.withColumn('avg_total', col('avg_bus') + col('avg_subway'))
select = analysis_3.select(
    col('dust_grade'),
    expr("stack(3,'bus',avg_bus,'subway',avg_subway,'total',avg_total) as (transport,passenger)")
)

result_df = select.toPandas()

result_df['dust_grade']=pd.Categorical(result_df['dust_grade'],categories=['좋음','보통','나쁨','매우 나쁨'])

result_df.to_csv(os.path.join(save_path, "dust_transport_data.csv"), index=False, encoding="utf-8")
plt.figure(figsize=(11,6))
sns.barplot(x='dust_grade',y='passenger', hue='transport',data=result_df)

#ax.yaxis.set_major_formatter(FuncFormatter(comma_formatter))
plt.gca().yaxis.set_major_formatter(FuncFormatter(comma_formatter))
plt.title('주말 기준 미세먼지는 주말 승하차량에 영향을 미칠까')
plt.xlabel('황사 상태')
plt.ylabel('평균 승객')

file_name = "dust_transport_plot.png"
result_path = os.path.join(save_path,file_name)
plt.savefig(result_path,dpi=300)
print(result_path)
plt.close()

spark.stop()
