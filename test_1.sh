#!/bin/bash

export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8
export PYTHONIOENCODING=utf-8
export PYSPARK_PYTHON=python3.6
export PYSPARK_DRIVER_PYTHON=python3.6

set -e

cd /home/maria_dev/BDP

spark-submit --master "local[*]" src/pipeline/spark_preprocessing.py "*"
