#!/bin/bash

export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8
export PYTHONIOENCODING=utf-8
export PYSPARK_PYTHON=python3.6
export PYSPARK_DRIVER_PYTHON=python3.6

set -e

TARGET_YM=$1

cd /home/maria_dev/BDP

HIVE_DB="public_transport_weather"
HIVE_TABLE="weather_pt_correlation"
LOCAL_RAW_DIR="/home/maria_dev/BDP/data/raw"
HDFS_RAW_DIR="/user/maria_dev/BDP/data/raw"

HDFS_PROCESSED_DIR="/warehouse/tablespace/external/hive/weather_pt_correlation"
THREE_MONTH=$(date -d "3 months ago" +%Y%m 2>/dev/null || data -v-3m +%Y%m 2>/dev/null)
hdfs dfs -mkdir -p "${HDFS_PROCESSED_DIR}" 2>/dev/null || true
hdfs dfs -chmod 777 "${HDFS_PROCESSED_DIR}" 2>/dev/null || true

hive -e "CREATE DATABASE IF NOT EXISTS ${HIVE_DB};"

hive -e "
CREATE EXTERNAL TABLE IF NOT EXISTS ${HIVE_DB}.${HIVE_TABLE} (
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
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
LOCATION '${HDFS_PROCESSED_DIR}';
"
sleep 3

check_partition_file_exist() {
    local YM=$1
    if hdfs dfs -ls "${HDFS_PROCESSED_DIR}/yyyymm=${YM}" 2>/dev/null | grep -q -E 'part-|csv'; then
        return 0
    fi
    return 1
}

if [ -z "$TARGET_YM" ]; then
    echo "입력된 연월이 없습니다. 자료를 확인합니다"
    TABLE_CHECK=$(hive -e "SHOW TABLES IN ${HIVE_DB} LIKE '${HIVE_TABLE}';" 2>/dev/null ||true)
    
    if [[ "$TABLE_CHECK"  != *"${HIVE_TABLE}"* ]]; then
        echo "HIVE 테이블이 존재하지 않습니다"
        

        HDFS_FILES=$(hdfs dfs -ls "${HDFS_RAW_DIR}"/*.csv 2>/dev/null || true)
        
        if [ -z "$HDFS_FILES" ]; then
            echo "HDFS에 파일이 존재하지 않습니다"

            LOCAL_FILES=$(ls "${LOCAL_RAW_DIR}"/*.csv 2>/dev/null || true)
            if [ -n "$LOCAL_FILES" ]; then
                echo "로컬 파일 HDFS에 업로드"
                hdfs dfs -mkdir -p "${HDFS_RAW_DIR}" 2>/dev/null || true
                hdfs dfs -put "${LOCAL_RAW_DIR}/*.csv" "${HDFS_RAW_DIR}" 2>/dev/null || true
            else
                echo "로컬 경로에도 데이터가 없습니다 API 수집을 진행해주세요"
                exit 1
            fi
        else
            echo "HDFS에 데이터 존재"
        fi
        echo "Spark 분산 처리 및 Hive 테이블 적재 중"
        YMS=$(hdfs dfs -ls "${HDFS_RAW_DIR}/BUS_STATION_BOARDING_MONTH_"*.csv 2>/dev/null | grep -o 'MONTH_[0-9]\{6\}' | sed 's/MONTH_//' || true)

        for YM in $YMS; do
            
            if check_partition_file_exsit "$YM"; then
                echo "$YM 테이블 존재"
            else
                echo "HIVE 테이블 생성 $YM"
                spark-submit --master local[*] src/pipeline/spark_preprocessing.py "$YM"
            fi
        done
    else
        echo "HIVE 테이블 ${HIVE_DB}.${HIVE_TABLE}"
    fi

    echo "Spark SQL 분석 및 시각화 PNG 생성 중"
    spark-submit --master local[*] src/analyze/visualize.py
    echo "작업 완료"
    exit 0
fi

if check_partition_file_exist "$TARGET_YM" ; then
    echo "${TARGET_YM}데이터가 이미 Hive테이블에 적재되어 있습니다"
else
    echo "HIVE 테이블 ${HIVE_DB}.${HIVE_TABLE} 확인 중"
    hdfs dfs -mkdir -p "${HDFS_RAW_DIR}" 2>/dev/null || true
#    PARTITION_CHECK=$(hive -e "SHOW PARTITIONS ${HIVE_DB}.${HIVE_TABLE} PARTITION(yyyymm='$TARGET_YM');" 2>/dev/null ||true)
#
#    if [ -n "$PARTITION_CHECK" ]; then
#        echo "${TARGET_YM}데이터가 이미 Hive테이블에 적재되어 있습니다 수집 및 전처리 과정을 건너뛰고 시각화 단계로 진행합니다"
#    else
#        echo  "${TARGET_YM}데이터가 HIVE에 없습니다 파이프라인을 가동합니다"
#        echo " API 수집 및 업로드 중"
#        
#        hdfs dfs -mkdir -p "${HDFS_RAW_DIR}" 2>/dev/null || true

        check_and_upload(){
            local FILE_PREFIX=$1
            local SCRIPT_NAME=$2
            local EXPECT_FILE="${FILE_PREFIX}_${TARGET_YM}.csv"

            if hdfs dfs -test -s "${HDFS_RAW_DIR}/${EXPECT_FILE}" 2>/dev/null; then
                echo "{$EXPECT_FILE} 파일이 이미 존재합니다. api 수집을 패스합니다"
            elif [ -f "${LOCAL_RAW_DIR}/${EXPECT_FILE}" ]; then
                echo "로컬에 파일이 존재합니다. HDFS로 복사합니다"
                hdfs dfs -put "${LOCAL_RAW_DIR}/${EXPECT_FILE}" "${HDFS_RAW_DIR}/"
            else
                echo "{$EXPECT_FILE} 파일이 누락되었습니다. api 수집"
                if [ "$FILE_PREFIX" = "CARD_SUBWAY_MONTH" && [ "$TARGET_YM" -lt "$THREE_MONTH" ] ]; then
                    echo "서울 지하철 데이터를 3개월 전까지만 API로 반환합니다"
                    echo "해당 연월은 지원하지 않으므로 ${LOCAL_RAW_DIR}/${EXPECT_FILE} 경로에 저장해 주신 후 다시 실행해주세요"
                    exit 1
                fi
                python3.6 src/ingest/"${SCRIPT_NAME}" "$TARGET_YM"
                hdfs dfs -put "${LOCAL_RAW_DIR}/${EXPECT_FILE}" "${HDFS_RAW_DIR}/"
            fi
        }
        
        check_and_upload "CARD_SUBWAY_MONTH" "collect_subway.py"
        check_and_upload "BUS_STATION_BOARDING_MONTH" "collect_bus.py"
        check_and_upload "dust_data" "collect_dust_warning.py"
        check_and_upload "weather_data" "collect_weather.py"
        echo "Spark 분산 처리 및 Hive 테이블 적재 중"
        spark-submit --master local[*] src/pipeline/spark_preprocessing.py "$TARGET_YM"
        hive -e "MSCK REPAIR TABLE ${HIVE_DB}.${HIVE_TABLE};"
    fi
    echo "Spark SQL 분석 및 시각화 PNG 생성 중"
    spark-submit --master local[*] src/analyze/visualize.py
    hive -e "MSCK REPAIR TABLE ${HIVE_DB}.${HIVE_TABLE};"
    echo "완료"

