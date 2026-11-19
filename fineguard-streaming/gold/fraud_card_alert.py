from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, concat_ws, current_timestamp, lit, to_date

@dp.table(
    name="fineguard.gold.fraud_card_alert",
    comment="Alert details where transactions match a card present on the fraud watchlist"
)
def fraud_card_alert() -> DataFrame:
    transactions_df = spark.readStream.table("fineguard.silver.transactions")
    fraud_watchlist_df = spark.readStream.table("fineguard.silver.fraud_watchlist")
    customer_df = spark.read.table("fineguard.silver.customers")

    transactions_with_watermark = transactions_df.withWatermark("transaction_timestamp", "5 minutes")
    fraud_watchlist_with_watermark = fraud_watchlist_df.withWatermark("effective_from", "5 minutes")

    fraud_detection = (
        transactions_with_watermark.join(
            fraud_watchlist_with_watermark,
            on=(
                (transactions_with_watermark["card_number"] == fraud_watchlist_with_watermark["entity_id"])
                & (transactions_with_watermark["transaction_timestamp"] >= fraud_watchlist_with_watermark["effective_from"])
            ),
            how = "inner"
        )
        .join(
            customer_df,
            transactions_with_watermark["customer_id"] == customer_df["customer_id"],
            "left"
        )
        .select(
            concat_ws("-", lit("FRAUD"), transactions_with_watermark["transaction_id"], fraud_watchlist_with_watermark["watchlist_id"]).alias("alert_id"),
            lit("FRAUD_WATCHLIST_MATCH").alias("alert_type"),
            current_timestamp().alias("alert_timestamp"),

            # Transaction details
            transactions_with_watermark["transaction_id"],
            transactions_with_watermark["customer_id"],
            customer_df["email"].alias("customer_email_id"),
            concat_ws(" ", customer_df["first_name"], customer_df["last_name"]).alias("customer_name"),
            transactions_with_watermark["card_number"],
            transactions_with_watermark["amount"].alias("transaction_amount"),
            transactions_with_watermark["currency"].alias("transaction_currency"),
            transactions_with_watermark["merchant_id"],
            transactions_with_watermark["merchant_name"],
            transactions_with_watermark["merchant_category"],
            transactions_with_watermark["transaction_type"],
            transactions_with_watermark["payment_channel"],
            transactions_with_watermark["device_id"],
            transactions_with_watermark["city"].alias("transaction_city"),
            transactions_with_watermark["country"].alias("transaction_country"),
            transactions_with_watermark["transaction_timestamp"],
            transactions_with_watermark["is_international"],
            transactions_with_watermark["status"].alias("transaction_status"),

            # Fraud watchlist details
            fraud_watchlist_with_watermark["watchlist_id"],
            fraud_watchlist_with_watermark["watch_type"],
            fraud_watchlist_with_watermark["risk_level"],
            fraud_watchlist_with_watermark["action"],
            fraud_watchlist_with_watermark["reason_code"],
            fraud_watchlist_with_watermark["reason_description"],
            fraud_watchlist_with_watermark["effective_from"].alias("watchlist_effective_from"),
            fraud_watchlist_with_watermark["reported_by"],
            fraud_watchlist_with_watermark["reported_source"],
            fraud_watchlist_with_watermark["city"].alias("watchlist_city"),
            fraud_watchlist_with_watermark["country"].alias("watchlist_country")
        )
    )

    return fraud_detection

