#!/bin/bash

set -e

TARGET_YM=${1:-"202601"}

cd /home/maria_dev/BDP

echo " API 수집 및 업로드 중"
python3.6 src/ingest/collect_bus.py "$TARGET_YM"
python3.6 src/ingest/collect_dust_warning.py "$TARGET_YM"
python3.6 src/ingest/collect_weather.py "$TARGET_YM"

echo "Spark 분산 처리 및 Hive 테이블 적재 중"
spark-submit --master local[*] src/pipeline/spark_preprocessing.py "$TARGET_YM"

echo "Spark SQL 분석 및 시각화 PNG 생성 중"
spark-submit --master local[*] src/analyze/visualize.py

echo "완료"