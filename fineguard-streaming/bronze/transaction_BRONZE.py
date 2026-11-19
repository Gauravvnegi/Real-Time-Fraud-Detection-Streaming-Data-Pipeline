from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import json
from pyspark.sql.functions import col
from pyspark.sql import functions as F

@dp.table(
    name = "fineguard.bronze.transactions",
    comment = "transaction raw strem data from kafka"
)

def transaction_bronze() -> DataFrame:
    kafka_connection_json = dbutils.secrets.get(scope = 'fineguard-scope', key = 'kafka_connection_details')
    kafka_config = json.loads(kafka_connection_json)
    bootstrap_servers = kafka_config['bootstrap_servers']
    api_key = kafka_config['api_key']
    api_secret = kafka_config['api_secret']
    topic = kafka_config['topic']
    jaas_config = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";'
    

    
    
    sample_streaming = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("startingOffsets", "earliest")
        .load()
    )

    
    

    parse_streaming = sample_streaming.select(
    col('key').cast('string'),
    col("value").cast("string"),
    col('partition'),
    col('topic'),
    col('timestamp').cast('timestamp'),
    F.current_timestamp().alias('ingesttime')   
    )
    
    return parse_streaming

    # streaming_data = parse_streaming.writeStream.format("delta").outputMode("Append")\
    # .option('checkpointLocation', '/Volumes/fineguard/source/transactions/checkpoint/')\
    # .trigger(availableNow=True)\
    # .toTable('fineguard.bronze.transaction_streaming_test')

    # print("Query id is :",streaming_data.id)


