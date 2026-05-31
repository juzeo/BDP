import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import *



plt.rcParams['font.family'] = 'Malgun Gothic'
spark = SparkSession.builder.appName(f"Hive_Data_Visualization").config("spark.sql.catalogImplementation","hive").enableHiveSupport().getOrCreate()

df = spark.table("public_transport_weather.weather_pt_correlation")
# 평일 기준 비랑 눈은 대중교통 승하차량에 영향을 줄까
analysis_1 = df.filter(col("is_weekday")=="평일")\
            .groupBy("is_rainy")\
            .agg(round(avg('bus_passenger'),0).alias('avg_bus'),
                 round(avg('subway_passenger'),0).alias('avg_subway'))
select = analysis_1.select(
    col('is_rainy'),
    expr("stack(2,'bus',avg_bus,'subway',avg_subway) as (transport,passenger)")
)

result_df = select.toPandas()


result_df['is_rainy']=pd.Categorical(result_df['is_rainy'],categories=['많이 옴','조금 옴','안 옴'])

plt.figure(figsize=(11,6))
sns.barplot(x='is_rainy',y='passenger', hue='transport',data=result_df)

plt.xlabel('비 상태')
plt.ylabel('평균 승객')

file_name = "rainy_transport_plot.png"
save_path = os.path.join(os.getcwd(),file_name)
plt.savefig(save_path,dpi=300)
plt.close()

# 평일 기준 비랑 눈은 대중교통 승하차량에 영향을 줄까
analysis_2 = df.filter(col("is_weekday")=="주말")\
            .groupBy("severe_weather")\
            .agg(round(avg('bus_passenger'),0).alias('avg_bus'),
                 round(avg('subway_passenger'),0).alias('avg_subway'))
select = analysis_2.select(
    col('severe_weather'),
    expr("stack(2,'bus',avg_bus,'subway',avg_subway) as (transport,passenger)")
)

result_df = select.toPandas()

plt.figure(figsize=(11,6))
sns.barplot(x='severe_weather',y='passenger', hue='transport',data=result_df)

plt.xlabel('날씨 상태')
plt.ylabel('평균 승객')

file_name = "severe_weather_transport_plot.png"
save_path = os.path.join(os.getcwd(),file_name)
plt.savefig(save_path,dpi=300)
plt.close()


# 주말 기준 미세먼지는 주말 승하차량에 영향을 미칠까
analysis_3 = df.filter(col("is_weekday")=="주말")\
            .groupBy("dust_grade")\
            .agg(round(avg('bus_passenger'),0).alias('avg_bus'),
                 round(avg('subway_passenger'),0).alias('avg_subway'))
select = analysis_3.select(
    col('dust_grade'),
    expr("stack(2,'bus',avg_bus,'subway',avg_subway) as (transport,passenger)")
)

result_df = select.toPandas()

result_df['dust_grade']=pd.Categorical(result_df['dust_grade'],categories=['좋음','보통','나쁨','매우 나쁨'])

plt.figure(figsize=(11,6))
sns.barplot(x='dust_grade',y='passenger', hue='transport',data=result_df)

plt.xlabel('황사 상태')
plt.ylabel('평균 승객')

file_name = "dust_transport_plot.png"
save_path = os.path.join(os.getcwd(),file_name)
plt.savefig(save_path,dpi=300)
plt.close()

spark.stop()