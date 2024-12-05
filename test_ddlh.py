import pytest
from lib import utils
from lib import data_loader
from lib import data_transformer
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, TimestampType, ArrayType, NullType

path = "data"


@pytest.fixture(scope='session')
def spark():
    return utils.get_spark_session()


def get_events_df():
    event_schema =  StructType([
    StructField("client_id", StringType(), True),
        StructField("clientData", ArrayType(StructType([
            StructField("accountIdentifier", StructType([
                StructField("operation", StringType(), False),
                StructField("newValue", StringType(), True),
                StructField("oldValue", NullType(), True)
            ]), False),
            StructField("personalDetails", StructType([
                StructField("operation", StringType(), False),
                StructField("newValue", StructType([
                    StructField("sex", StringType(), True),
                    StructField("birthdate", DateType(), True),
                    StructField("age", IntegerType(), True),
                    StructField("social", StringType(), True),
                    StructField("name", StructType([
                        StructField("firstName", StringType(), True),
                        StructField("middleName", StringType(), True),
                        StructField("lastName", StringType(), True)
                    ]), False)
                ]), False),
                StructField("oldValue", NullType(), True)
            ]), False),
            StructField("contactDetails", StructType([
                StructField("operation", StringType(), False),
                StructField("newValue", StructType([
                    StructField("phone", StringType(), True),
                    StructField("email", StringType(), True)
                ]), False),
                StructField("oldValue", NullType(), True)
            ]), False),
            StructField("addressDetails", StructType([
                StructField("operation", StringType(), False),
                StructField("newValue", StructType([
                    StructField("address_1", StringType(), True),
                    StructField("address_2", StringType(), True),
                    StructField("city", StringType(), True),
                    StructField("state", StringType(), True),
                    StructField("zipcode", StringType(), True),
                    StructField("district_id", StringType(), True)
                ]), False),
                StructField("oldValue", NullType(), True)
            ]), False),
            StructField("frequency", StructType([
                StructField("operation", StringType(), False),
                StructField("newValue", StringType(), True),
                StructField("oldValue", NullType(), True)
            ]), True),
            StructField("parsedDate", StructType([
                StructField("operation", StringType(), False),
                StructField("newValue", DateType(), True),
                StructField("oldValue", NullType(), True)
            ]), True),
            StructField("accountInformation", StructType([
                StructField("operation", StringType(), False),
                StructField("newValue", StructType([
                    StructField("account_id", StringType(), True),
                    StructField("accountBalance", DoubleType(), True),
                    StructField("bank", StringType(), True)
                ]), False),
                StructField("oldValue", NullType(), True)
            ]), True),
            StructField("transactionDetails", StructType([
                StructField("operation", StringType(), False),
                StructField("newValue", StructType([
                    StructField("trans_id", StringType(), True),
                    StructField("transactionType", StringType(), True),
                    StructField("transactionOperation", StringType(), True),
                    StructField("transactionAmount", DoubleType(), True),
                    StructField("transactionPurpose", StringType(), True)
                ]), False),
                StructField("oldValue", NullType(), True)
            ]), True),
            StructField("transactionTime", StructType([
                StructField("operation", StringType(), False),
                StructField("newValue", StructType([
                    StructField("transactionTime", StringType(), True),
                    StructField("transactionDate", StringType(), True),
                    StructField("transactionDatetime", TimestampType(), True)
                ]), False),
                StructField("oldValue", NullType(), True)
            ]), True)
        ]), False), False)
    ])
    return event_schema


def test_total_records(spark):
    """tests the total number of records in the three tables"""
    accounts_records = data_loader.ingest_accounts(spark=spark, enable_hive=False, hive_db=None, path=path).count()
    clients_records = data_loader.ingest_clients(spark=spark, enable_hive=False, hive_db=None, path=path).count()
    transaction_records = data_loader.ingest_transactions(spark=spark, enable_hive=False, hive_db=None, path=path).count()
    assert accounts_records == 10
    assert clients_records == 10
    assert transaction_records == 2361


def test_accounts_output():
    """tests output from the ingest_account table"""

    pass


def test_clients_output():
    """tests output from the ingest_clients table"""
    pass


def test_transactions_output():
    """tests output from the ingest_transactions table"""
    pass
