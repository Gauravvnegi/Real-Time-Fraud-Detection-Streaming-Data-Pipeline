from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

@dp.table(
    name="fineguard.silver.fraud_watchlist",
    comment="Clean Fraud watchlist data"
)
def fraud_watchlist_silver() -> DataFrame:
    bronze_df = spark.readStream.table("fineguard.bronze.fraud_watchlist")

    cleaned_df = bronze_df.select(
        F.upper(F.col("action")).alias("action"),
        F.col("city").alias("city"),
        F.col("country").alias("country"),
        F.to_timestamp(F.col("effective_from"), "dd-MMM-yyyy HH:mm:ss").alias("effective_from"),
        F.upper(F.col("entity_id")).alias("entity_id"),
        F.col("reason_code").alias("reason_code"),
        F.col("reason_description").alias("reason_description"),
        F.col("reported_by").alias("reported_by"),
        F.col("reported_source").alias("reported_source"),
        F.upper(F.col("risk_level")).alias("risk_level"),
        F.col("status").alias("status"),
        F.col("watch_type").alias("watch_type"),
        F.upper(F.col("watchlist_id")).alias("watchlist_id"),
        F.col("source_path"),
        F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        F.current_timestamp().alias("silver_ingestion_timestamp")
    )

    return cleaned_df
