# 데이터 출처
서울시 지하철 호선별 역별 승하차 인원 정보
- 서울 열린데이터 광장
- https://data.seoul.go.kr/dataList/OA-12914/S/1/datasetView.do
서울시 버스노선별 정류장별 승하차 인원 정보
- 서울 열린데이터 광장
- https://data.seoul.go.kr/dataList/OA-12912/S/1/datasetView.do
종합기상관측 일별 데이터 & 서울시 대기질 정보
- 기상청
- https://apihub.kma.go.kr/


# 스키마
DB : public_transport_weather
Table : weather_pt_correlation
Storage Location : hdfs:///warehouse/tablespace/external/hive/weather_pt_correlation
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