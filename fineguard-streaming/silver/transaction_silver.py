from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import json
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType
from pyspark.sql import functions as F

@dp.table(
    name = "fineguard.silver.transactions",
    comment = "Parse and clean transaction databricks"
)
@dp.expect_or_drop(
    "valid_transaction_id", "transaction_id IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_customer_id", "customer_id IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_merchant_id", "merchant_id IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_merchant_name", "merchant_name IS NOT NULL"
)
@dp.expect(
    "valid amount", "amount>0"
)

def transaction_silver():
    bronze_df = spark.readStream.table('fineguard.bronze.transactions')

    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("card_number", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("payment_channel", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("transaction_timestamp", TimestampType(), True),
        StructField("is_international", BooleanType(), True),
        StructField("status", StringType(), True)
    ])
    transform_df = bronze_df.select(
        from_json(col('value'), schema).alias('data'),
        col('topic').alias('kafka_topic'),
        col('partition').alias('kafka_partition'),
        # col('offset').alias('kafka_offset'),
        col('timestamp').alias('kafka_timestamp'),
        col('ingesttime').alias('bronze_ingest_timestamp')
    ).select('data.*', 'kafka_topic', 'kafka_partition', 'kafka_timestamp', 'bronze_ingest_timestamp',F.current_timestamp().alias('silver_ingest_timestamp'))
    return transform_df