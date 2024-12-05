from pyspark.sql.functions import struct, lit, col, array, when, isnull, filter, current_timestamp, date_format, expr, \
    collect_list, udf, to_timestamp, concat
from pyspark.sql.types import StringType
from lib import utils
import uuid
# from pyspark.sql import SparkSession


def insert_update_operation(column, alias):
    """insert or update operation to either insert a new column or update the existing one"""
    return struct(lit("INSERT").alias("operation"),
                  column.alias("newValue"),
                  lit(None).alias("oldValue")).alias(alias)


def clients(df_clients):
    """returns a dataframe with columns of clients table packed in few categories"""
    name = struct(col("first").alias("firstName"),
                  col("middle").alias("middleName"),
                  col("last").alias("lastName")).alias("name")

    personal_details = struct(col("sex").alias("sex"),
                             col("fulldate").alias("birthdate"),
                             col("age").alias("age"),
                             col("social").alias("social"),
                             name).alias("personalDetails")

    contact_details = struct(col("phone").alias("phone"),
                             col("email").alias("email")).alias("contactDetails")

    address_details = struct(col("address_1").alias("address_1"),
                             col("address_2").alias("address_2"),
                             col("city").alias("city"),
                             col("state").alias("state"),
                             col("zipcode").alias("zipcode"),
                             col("district_id").alias("district_id")).alias("addressDetails")

    return df_clients.select("client_id",
                            insert_update_operation(col("acc_id"), "accountIdentifier"),
                            insert_update_operation(personal_details, "personalDetails"),
                            insert_update_operation(contact_details, "contactDetails"),
                            insert_update_operation(address_details, "addressDetails"))


def accounts(df_accounts):
    """packs columns of accounts tables in categories"""
    return df_accounts.select("account_id",
                              insert_update_operation(col("district_id"), "districtID"),
                              insert_update_operation(col("frequency"), "frequency"),
                              insert_update_operation(col("date"), "parsedDate"))


adjust_time_udf = udf(utils.adjust_time, StringType())


def transactions(df_transactions):
    """packs columns of transactions tables in categories"""
    account_information = struct("account_id",
                                 col("balance").alias("accountBalance"),
                                 col("bank").alias("bank")).alias("accountInformation")

    transaction_details = struct("trans_id",
                                 col("type").alias("transactionType"),
                                 col("operation").alias("transactionOperation"),
                                 col("amount").alias("transactionAmount"),
                                 col("k_symbol").alias("transactionPurpose")).alias("transactionDetails")
    #adjust the time column
    df_transactions = df_transactions.withColumn("adjustedtime", adjust_time_udf(df_transactions["fulltime"]))
    #create datetime column
    df_transactions = df_transactions.withColumn("datetime", to_timestamp(concat(col("date"), lit(" "), col("adjustedtime")), "yyyy-MM-dd HH:mm:ss"))

    transaction_time = struct(col("adjustedtime").alias("transactionTime"),
                              col("date").alias("transactionDate"),
                              col("datetime").alias("transactionDatetime")).alias("transactionTime")

    return df_transactions.select("trans_id",
                                  insert_update_operation(account_information, "accountInformation"),
                                  insert_update_operation(transaction_details, "transactionDetails"),
                                  insert_update_operation(transaction_time, "transactionTime"))


# join clients and accounts tables
def join_clients_accounts(df_clients, df_accounts):
    """left joins the clients dataframe with accounts dataframe"""
    return df_clients.join(df_accounts,
                           df_clients.accountIdentifier.newValue == df_accounts.account_id, how="left")


def join_clients_accounts_transactions(df_clients_accounts, df_transactions):
    """left joins the clients_accounts dataframe with transactions dataframe"""
    return df_clients_accounts.join(df_transactions,
                                    df_clients_accounts.account_id == df_transactions.accountInformation.newValue.account_id,
                                    how="left").\
                                groupBy("client_id").\
                                agg(collect_list(struct("accountIdentifier",
                                                 "personalDetails",
                                                 "contactDetails",
                                                 "addressDetails",
                                                 "frequency",
                                                 "parsedDate",
                                                 "accountInformation",
                                                 "transactionDetails",
                                                 "transactionTime").alias("clientData")).alias("clientData"))


# register uuid function
uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())


def event_information(spark, df_client_processed):
    """attaches event information to dataframe"""
    event_info = [("ddlh-clients", 1, 0),]
    df_event = spark.createDataFrame(event_info)\
                    .toDF("eventType", "majorSchemaVersion", "minorSchemaVersion")
    #join event and processed client datafreames
    return df_event.hint("broadcast").crossJoin(df_client_processed) \
        .select(struct(uuid_udf().alias("evnetID"),
                       col("eventType"),
                       col("majorSchemaVersion"),
                       col("minorSchemaVersion"),
                       lit(date_format(current_timestamp(), "yyyy-MM-dd' 'HH:mm:ssZ").alias("eventDateTime"))).alias("eventInfo"),
                col("client_id"),
                col("clientData"))