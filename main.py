from pyspark.sql.functions import col, when, count, to_timestamp, concat, lit, struct, to_json
from lib import data_loader
from lib import data_transformer
from lib import utils


# create spark session
spark = utils.get_spark_session()
path = "../data"

# ingest data to pyspark tables
df_accounts = data_loader.ingest_accounts(spark=spark, enable_hive=False, hive_db=None, path=path)
df_clients = data_loader.ingest_clients(spark=spark, enable_hive=False, hive_db=None, path=path)
df_transactions = data_loader.ingest_transactions(spark=spark, enable_hive=False, hive_db=None, path=path)

# transform data
df_clients_transformed = data_transformer.clients(df_clients)
df_accounts_transformed = data_transformer.accounts(df_accounts)
df_transactions_transformed = data_transformer.transactions(df_transactions)

#join data
df_clients_accounts = data_transformer.join_clients_accounts(df_clients_transformed, df_accounts_transformed)
df_clients_accounts_transactions = data_transformer.join_clients_accounts_transactions(df_clients_accounts,\
                                                                                          df_transactions_transformed)

# join event information to processed dataframe
df_events = data_transformer.event_information(spark, df_clients_accounts_transactions)

df_events.toPandas().to_json("../data/test_data/events.json", orient="records")

df_kafka_events = df_events.select(col("client_id").alias("key"),
                                   to_json(struct("*")).alias("value"))


df_events.printSchema()

# df_kafka_events.show()
# df_kafka_events.printSchema()
#
# api_key = 'api key'
# api_secret = 'api secret'
#
# df_kafka_events.write \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "pkc-56d1g.eastus.azure.confluent.cloud:9092") \
#     .option("topic", "bank_clients") \
#     .option("kafka.security.protocol", "SASL_SSL") \
#     .option("kafka.sasl.jaas.config", f"org.apache.kafka.common.security.plain.PlainLoginModule required username={api_key} password={api_secret};") \
#     .option("kafka.sasl.mechanism", "PLAIN") \
#     .option("kafka.client.dns.lookup", "use_all_dns_ips") \
#     .save()










