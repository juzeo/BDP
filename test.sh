#!/bin/bash

export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8
export PYTHONIOENCODING=utf-8
export PYSPARK_PYTHON=python3.6
export PYSPARK_DRIVER_PYTHON=python3.6

set -e

TARGET_YM=${1:}

cd /home/maria_dev/BDP

HIVE_DB = "public_transport_weather"
HIVE_TABLE = "weather_pt_correlation"
LOCAL_RAW_DIR = "/home/maria_dev/BDP/data/raw"
HDFS_RAW_DIR = "/user/maria_dev/BDP/data/raw"


if [ -z "$TARGET_YM" ]; then
    echo "입력된ㄴ 연월이 없습니다. 수집/적재를 건너뛰고 기존 Hive 데이터로 전체 시각화"
    spark-submit --master local[*] src/analyze/visualize.py

else
    echo "HIVE 테이블 ${HIVE.DB}.${HIVE_TABLE}"
    PARTITION_CHECK=$(hive -e "SHOW PARTITIONS ${HIVE_DB}.${HIVE_TABLE} PARTITION(yyyymm='$TARGET_YM');" 2>/dev/null ||true)

    if [ -n "$PARTITION_CHECK"]; then
        echo "${TARGET_YM}데이터가 이미 Hive테이블에 적재되어 있습니다 수집 및 전처리 과정을 건너뛰고 시각화 단계로 진행합니다"
    else
        echo  "${TARGET_YM}데이터가 HIVE에 없습니다 파이프라인을 가동합니다"
        echo " API 수집 및 업로드 중"
        THREE_MONTH = $(data -d "3 months ago" +%Y%m 2>/dev/null || data -v-3m +%Y%m 2>/dev/null)
        

        if [ "$TARGET_YM" -lt "$THREE_MONTH"]; then
            SUBWAY_FILE="${LOCAL_RAW_DIR}/CARD_SUBWAY_MONTH_${TARGET_YM}.csv"

            if [-f "$SUBWAY_FILE"]; then
                python3.6 src/ingest/collect_subway.py "$TARGET_YM"
            else
                echo "서울 지하철 데이터를 3개월 전까지만 API로 반환합니다"
                echo "해당 연월은 지원하지 않으므로 ${SUBWAY_FILE} 경로에 저장해 주신 후 다시 실행해주세요"
                exit 1
            fi
                python3.6 src/ingest/collect_bus.py "$TARGET_YM"
                python3.6 src/ingest/collect_dust_warning.py "$TARGET_YM"
                python3.6 src/ingest/collect_weather.py "$TARGET_YM"     
        else
            python3.6 src/ingest/collect_bus.py "$TARGET_YM"
            python3.6 src/ingest/collect_dust_warning.py "$TARGET_YM"
            python3.6 src/ingest/collect_weather.py "$TARGET_YM"
            python3.6 src/ingest/collect_subway.py "$TARGET_YM"  
           
        fi
            echo "Spark 분산 처리 및 Hive 테이블 적재 중"
            spark-submit --master local[*] src/pipeline/spark_preprocessing.py "$TARGET_YM"

        echo "Spark SQL 분석 및 시각화 PNG 생성 중"
        spark-submit --master local[*] src/analyze/visualize.py
        echo "완료"
    fi
fi







