from pyspark.sql import SparkSession
from pyspark import SparkConf


def get_spark_session():
    spark_conf = SparkConf()
    spark_conf.set("spark.jars","E:\Machine Learning Engineer\Pyspark\ddlh\spark-sql-kafka-0-10_2.12-3.1.2.jar, E:\Machine Learning Engineer\Pyspark\ddlh\scala-library-2.12.10.jar")
    return SparkSession.builder \
                            .appName("ddlh") \
                            .config(conf=spark_conf) \
                            .getOrCreate()


def adjust_time(time_str):
    """adjust for the time overflow in pyspark, for example time 10:15:60 will be adjusted to 10:16:00"""
    hh, mm, ss = map(int, time_str.split(':'))
    if ss >= 60:
        mm += ss // 60
        ss = ss % 60
    if mm >= 60:
        hh += mm // 60
        mm = mm % 60
    return f"{hh:02}:{mm:02}:{ss:02}"