from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, concat_ws, current_timestamp, lit, to_date
from pyspark.sql import functions as F

@dp.table(
    name="fineguard.gold.transaction_count_by_every_min",
    comment="Transaction count by every minute"
)
def transaction_count_by_every_min() -> DataFrame:
    transactions_df = spark.readStream.table("fineguard.silver.transactions")
    transaction_with_watermark_df = transactions_df.withWatermark("transaction_timestamp", "5 minutes")


    transaction_count_df = transaction_with_watermark_df.groupBy(
        F.window("transaction_timestamp","1 minutes")
    ).agg(
        F.count("*").alias("transaction_count")
    ).select(
        F.date_format("window.start", "yyyy-MM-dd HH:mm:ss").alias("window_start"),
        F.date_format("window.end", "yyyy-MM-dd HH:mm:ss").alias("window_end"),
        F.col("transaction_count")
    )

    return transaction_count_df

