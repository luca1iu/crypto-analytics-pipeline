from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ── Schema ─────────────────────────────────────────────────────────────────────
# Must exactly match what your Python producers write into Kafka.
# block_timestamp and ingested_at come in as unix epoch integers (LongType)
# — we convert them to TimestampType after parsing.
TRANSACTION_SCHEMA = StructType([
    StructField("chain",           StringType(),  nullable=False),
    StructField("tx_hash",         StringType(),  nullable=False),
    StructField("block_number",    LongType(),    nullable=False),
    StructField("block_timestamp", LongType(),    nullable=False),
    StructField("from_address",    StringType(),  nullable=True),
    StructField("to_address",      StringType(),  nullable=True),
    StructField("value_eth",       DoubleType(),  nullable=True),
    StructField("gas_limit",       LongType(),    nullable=True),
    StructField("gas_price_gwei",  DoubleType(),  nullable=True),
    StructField("tx_fee_eth",      DoubleType(),  nullable=True),
    StructField("input_data",      StringType(),  nullable=True),
    StructField("ingested_at",     LongType(),    nullable=True),
])


def parse_kafka_messages(raw_df: DataFrame) -> DataFrame:
    """
    Kafka delivers every message as raw bytes in a 'value' column.
    Steps:
      1. Cast bytes → string → parse JSON using TRANSACTION_SCHEMA
      2. Convert unix epoch integers → TimestampType for BigQuery
      3. Drop malformed records (null tx_hash or chain)
    """

    # Step 1 — parse JSON bytes into typed struct columns
    parsed = raw_df.select(
        F.from_json(
            F.col("value").cast("string"),
            TRANSACTION_SCHEMA
        ).alias("data")
    ).select("data.*")

    # Step 2 — convert unix epoch seconds → Timestamp
    transformed = parsed \
        .withColumn(
            "block_timestamp",
            F.to_timestamp(F.col("block_timestamp").cast(LongType()))
        ) \
        .withColumn(
            "ingested_at",
            F.to_timestamp(F.col("ingested_at").cast(LongType()))
        )

    # Step 3 — drop malformed records
    clean = transformed.filter(
        F.col("tx_hash").isNotNull() & F.col("chain").isNotNull()
    )

    return clean