from pyspark.sql import SparkSession
from pyspark.sql.functions import col


if __name__ == "__main__":
    spark = SparkSession.builder.appName("test_spark").getOrCreate()

    spark.conf.set

    raw_df = spark.read.csv("hdfs:///user/maria_Dev/data/weather/raw/*.csv",header = True, inferSchema = True)

    processed_df = raw_df.select("TM","STN","TA_AVG","RN_DAY")

    processed_df = processed_df.fillna(0, subseet=["RN_DAY"])

    out_path = "hdfs:///user/maria_Dev/data/weather/processed"
    processed_df.write.csv(out_path, header = True, mode = "overwrite")
    