from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import json
from pyspark.sql.functions import col
from pyspark.sql import functions as F

@dp.table(
    name = "fineguard.bronze.fraud_watchlist",
    comment = "Fraud watchlist data ingested by auto loader"
)

def fraud_watchlist_bronze() -> DataFrame:
    source_path = '/Volumes/fineguard/source/fraud_watchlist/source_data/'
    input_stream = (
            spark.readStream.format('cloudFiles')
            .option('cloudFiles.format', 'json')
            # .option('cloudFiles.schemaLocation', '/Volumes/fineguard/source/fraud_watchlist/schema/')
            .option('cloudFiles.inferColumnTypes', True)
            .load(source_path)
    )

    parsed_stream = input_stream.select(
            F.col('action').alias('action'),
            F.col('city').alias('city'),
            F.col('country').alias('country'),
            F.col('effective_from').alias('effective_from'),
            F.col('entity_id').alias('entity_id'),
            F.col('reason_code').alias('reason_code'),
            F.col('reason_description').alias('reason_description'),
            F.col('reported_by').alias('reported_by'),
            F.col('reported_source').alias('reported_source'),
            F.col('risk_level').alias('risk_level'),
            F.col('status').alias('status'),
            F.col('watch_type').alias('watch_type'),
            F.col('watchlist_id').alias('watchlist_id'),
            F.col('_rescued_data').alias('_rescued_data'),
            F.col("_metadata.file_path",).alias("source_path"),
            F.current_timestamp().alias("ingestion_timestamp")
     )   
        

    
    return parsed_stream

    # streaming_data = parse_streaming.writeStream.format("delta").outputMode("Append")\
    # .option('checkpointLocation', '/Volumes/fineguard/source/transactions/checkpoint/')\
    # .trigger(availableNow=True)\
    # .toTable('fineguard.bronze.transaction_streaming_test')

    # print("Query id is :",streaming_data.id)


