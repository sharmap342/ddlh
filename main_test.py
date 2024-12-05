from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, TimestampType
from pyspark.sql.functions import col, when, count, to_timestamp, concat, lit

spark = SparkSession.builder \
                .appName("ddlh") \
                .getOrCreate()

# read accounts to dataframe
df_ac = spark.read \
                    .csv(path="data/accounts/accounts.csv",
                         header=True,
                         inferSchema=True)

# read clients to dataframe
df_cl = spark.read \
                    .csv(path="data/clients/clients.csv",
                         header=True,
                         inferSchema=True)

# read transactions to dataframe
df_tran = spark.read \
                        .csv(path="data/transactions/transactions.csv",
                             header=True,
                             inferSchema=True)


"""The infer schema was able to infer schema for accounts and clients table correctly but it was not able to 
do it for transaction table. it did not infer correct data types for columns full time and fulldatewithtime.
Therefore, it is always a good idea to define the schema for tables at first hand"""

# define schema for accounts table

schema_accounts = StructType([StructField("account_id", StringType(), False),
                              StructField("district_id", StringType(), True),
                              StructField("frequency", StringType(), True),
                              StructField("parseddate", DateType(), True),
                              StructField("year", IntegerType(), True),
                              StructField("month", IntegerType(), True),
                              StructField("day", IntegerType(), True),
                              StructField("date", DateType(), True)])

# define schema for client table

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

# define schema for the transaction table

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
                                  StructField("fulldate", StringType(), True),
                                  StructField("fulltime", StringType(), True),
                                  StructField("fulldatewithtime", StringType(), True),
                                  StructField("date", DateType(), True)])

df_accounts = spark.read \
                    .csv(path="data/accounts/accounts.csv",
                         header=True,
                         schema=schema_accounts)

df_clients = spark.read \
                    .csv(path="data/clients/clients.csv",
                         header=True,
                         schema=schema_clients)

df_transactions = spark.read \
                        .csv(path="data/transactions/transactions.csv",
                             header=True,
                             schema=schema_transactions)

df_accounts.printSchema()
df_clients.printSchema()
df_transactions.printSchema()

"""select only the required columns from dataframes"""

df_accounts_selected = df_accounts.select("account_id", "district_id", "frequency", "date")
df_clients_selected = df_clients.select("client_id", "acc_id", "sex", "fulldate", "age", "social", "first", "middle", "last", "phone", "email",
                                        "address_1", "address_2", "city", "state", "zipcode", "district_id")
df_transactions_selected = df_transactions.select("trans_id", "account_id", "type", "operation", "amount", "balance", "k_symbol", "bank", "account",
                                                  "fulldate", "fulltime", "fulldatewithtime")

"""check for null values"""
df_accounts_selected.select([count(when(col(c).isNull(), c)).alias(c) for c in df_accounts_selected.columns])
df_clients_selected.select([count(when(col(c).isNull(), c)).alias(c) for c in df_clients_selected.columns])
df_transactions_selected.select([count(when(col(c).isNull(), c)).alias(c) for c in df_transactions_selected.columns]).show()

"""we have null values in address_2 in clients and in operation, k_symbol, bank, account adn fulldatewithtime"""
#check why is fulldatewithtime is null while fulldate and fulltime is not null
df_transactions_selected.filter(df_transactions_selected['fulldatewithtime'].isNull())
"""let's replace the null values in fulldatewithtime with fulldate and time"""
df_transactions_selected = df_transactions_selected.withColumn("combined_datetime", concat(col("fulldate"), lit(" "), col("fulltime"))) \
                        .withColumn("datetime",  to_timestamp(col("combined_datetime"), "yyyy-MM-dd HH:mm:ss"))
#df_transactions_selected.withColumn("datetime", to_timestamp(col('fulldate'), 'yyyy-MM-dd') + col('fulltime')).show()
#df_transactions_selected = df_transactions_selected.withColumn("datetime", to_timestamp(col('fulldatewithtime'), "yyyy-MM-dd'T'HH:mm:ss"))
df_transactions_selected.filter(df_transactions_selected['datetime'].isNull()).show()
"""no null values in datetime"""


