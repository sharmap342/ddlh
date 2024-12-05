from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, TimestampType
from pyspark.sql.functions import col, when, count, to_timestamp, concat, lit


def schema_accounts():
    """get schema for accounts table"""
    schema_accounts = StructType([StructField("account_id", StringType(), False),
                                  StructField("district_id", StringType(), True),
                                  StructField("frequency", StringType(), True),
                                  StructField("parseddate", DateType(), True),
                                  StructField("year", IntegerType(), True),
                                  StructField("month", IntegerType(), True),
                                  StructField("day", IntegerType(), True),
                                  StructField("date", DateType(), True)])
    return schema_accounts


def schema_clients():
    """get schema for clients table"""
    schema_clients = StructType([StructField("client_id", StringType(), False),
                                 StructField("acc_id", StringType(), False),
                                 StructField("sex", StringType(), True),
                                 StructField("fulldate", DateType(), True),
                                 StructField("day", IntegerType(), True),
                                 StructField("month", IntegerType(), True),
                                 StructField("year", IntegerType(), True),
                                 StructField("age", IntegerType(), True),
                                 StructField("social", StringType(), True),
                                 StructField("first", StringType(), True),
                                 StructField("middle", StringType(), True),
                                 StructField("last", StringType(), True),
                                 StructField("phone", StringType(), True),
                                 StructField("email", StringType(), True),
                                 StructField("address_1", StringType(), True),
                                 StructField("address_2", StringType(), True),
                                 StructField("city", StringType(), True),
                                 StructField("state", StringType(), True),
                                 StructField("zipcode", StringType(), True),
                                 StructField("district_id", StringType(), True),
                                 StructField("date", DateType(), True)])
    return schema_clients


def schema_transactions():
    """get the schema for transactions table"""
    schema_transactions = StructType([StructField("column_a", IntegerType(), False),
                                      StructField("trans_id", StringType(), False),
                                      StructField("account_id", StringType(), False),
                                      StructField("type", StringType(), True),
                                      StructField("operation", StringType(), True),
                                      StructField("amount", DoubleType(), True),
                                      StructField("balance", DoubleType(), True),
                                      StructField("k_symbol", StringType(), True),
                                      StructField("bank", StringType(), True),
                                      StructField("account", StringType(), True),
                                      StructField("year", IntegerType(), True),
                                      StructField("month", IntegerType(), True),
                                      StructField("day", IntegerType(), True),
                                      StructField("fulldate", DateType(), True),
                                      StructField("fulltime", StringType(), True),
                                      StructField("fulldatewithtime", TimestampType(), True),
                                      StructField("date", StringType(), True)])
    return schema_transactions


def ingest_accounts(spark, enable_hive, hive_db, path):
    """ingest data from accounts hive table or csv file"""
    if enable_hive:
        return spark.sql("select * from " + hive_db + ".accounts")
    else:
        return spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(schema_accounts()) \
            .load(f"{path}" + r"/accounts/accounts.csv")


def ingest_clients(spark, enable_hive, hive_db, path):
    """ingest data from clients hive table or csv file"""
    if enable_hive:
        return spark.sql("select * from " + hive_db + ".clients")
    else:
        return spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(schema_clients()) \
            .load(f"{path}" + r"/clients/clients.csv")


def ingest_transactions(spark, enable_hive, hive_db, path):
    """ingest data from transactions hive table or csv file"""
    if enable_hive:
        return spark.sql("select * from " + hive_db + ".transactions")
    else:
        return spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(schema_transactions()) \
            .load(f"{path}" + r"/transactions/transactions.csv")

