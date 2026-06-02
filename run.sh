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
THREE_MONTH=$(date -d "3 months ago" +%Y%m 2>/dev/null || data -v-3m +%Y%m 2>/dev/null)
        

if [ -z "$TARGET_YM" ]; then
    echo "입력된 연월이 없습니다. 수집/적재를 건너뛰고 기존 Hive 데이터로 전체 시각화"
    spark-submit --master local[*] src/analyze/visualize.py

else
    echo "HIVE 테이블 ${HIVE_DB}.${HIVE_TABLE} 확인 중"

    PARTITION_CHECK=$(hive -e "SHOW PARTITIONS ${HIVE_DB}.${HIVE_TABLE} PARTITION(yyyymm='$TARGET_YM');" 2>/dev/null ||true)

    if [ -n "$PARTITION_CHECK" ]; then
        echo "${TARGET_YM}데이터가 이미 Hive테이블에 적재되어 있습니다 수집 및 전처리 과정을 건너뛰고 시각화 단계로 진행합니다"
    else
        echo  "${TARGET_YM}데이터가 HIVE에 없습니다 파이프라인을 가동합니다"
        echo " API 수집 및 업로드 중"
        
        hdfs dfs -mkdir -p "{HDFS_RAW_DIR}" 2>/dev/null || true

        check_and_upload(){
            local FILE_PREFIX=$1
            local SCRIPT_NAME=$2
            local EXPECT_FILE = "${FILE_PREFIX}_${TARGET_YM}.csv"

            if hdfs dfs -test -s "${HDFS_RAW_DIR}/{EXPECT_FILE}.csv" 2>/dev/null; then
                echo "{$EXPECT_FILE} 파일이 이미 존재합니다. api 수집을 패스합니다"
            elif [ -f "${LOCAL_RAW_DIR}/${EXPECT_FILE}" ]; then
                echo "로컬에 파일이 존재합니다. HDFS로 복사합니다"
            else
                echo "{$EXPECT_FILE} 파일이 누락되었습니다. api 수집"
                if [ "$FILE_PREFIX" = "CARD_SUBWAY_MONTH" && [ "$TARGET_YM" -lt "$THREE_MONTH" ] ]; then
                    echo "서울 지하철 데이터를 3개월 전까지만 API로 반환합니다"
                    echo "해당 연월은 지원하지 않으므로 ${LOCAL_RAW_DIR}/${EXPECT_FILE} 경로에 저장해 주신 후 다시 실행해주세요"
                    exit 1
                fi
                python3.6 src/ingest/"${SCRIPT_NAME}" "$TARGET_YM"
            fi
        }
        
        check_and_upload "CARD_SUBWAY_MONTH" "collect_subway.py"
        check_and_upload "BUS_STATION_BOARDING_MONTH" "collect_bus.py"
        check_and_upload "dust_data" "collect_dust_warning.py"
        check_and_upload "weather_data" "collect_weather.py"
        echo "Spark 분산 처리 및 Hive 테이블 적재 중"
        spark-submit --master local[*] src/pipeline/spark_preprocessing.py "$TARGET_YM"
    fi
    echo "Spark SQL 분석 및 시각화 PNG 생성 중"
    spark-submit --master local[*] src/analyze/visualize.py
    echo "완료"
fi