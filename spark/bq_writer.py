import os

from dotenv import load_dotenv
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

load_dotenv()

GCP_PROJECT  = os.getenv("GCP_PROJECT_ID", "crypto-analytics-495919")
BQ_DATASET   = os.getenv("BQ_DATASET",     "raw")
BQ_TABLE     = os.getenv("BQ_TABLE",       "transactions")
GCS_BUCKET   = os.getenv("GCS_TEMP_BUCKET")
CREDENTIALS  = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "./gcp-key.json")

BQ_TABLE_REF = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"


def write_to_bigquery(batch_df: DataFrame, batch_id: int) -> None:
    """
    Called by Spark once per micro-batch (every 30 seconds).

    We use foreachBatch because:
    - Full control over write behaviour per batch
    - Can log row counts, add deduplication, handle errors
    - BigQuery connector is most stable in batch (not streaming) mode

    Write flow:
      Spark DataFrame → GCS temp bucket (parquet) → BigQuery load job
    This is how the official Spark-BQ connector works under the hood.
    """
    row_count = batch_df.count()

    if row_count == 0:
        print(f"[BQ-WRITER] Batch {batch_id}: empty, skipping.")
        return

    print(f"[BQ-WRITER] Batch {batch_id}: writing {row_count} rows → {BQ_TABLE_REF}")

    batch_df.write \
        .format("bigquery") \
        .option("table",                BQ_TABLE_REF) \
        .option("temporaryGcsBucket",   GCS_BUCKET) \
        .option("credentialsFile",      CREDENTIALS) \
        .option("location",             "EU") \
        .mode("append") \
        .save()

    print(f"[BQ-WRITER] Batch {batch_id}: ✓ done.")


def start_streaming_query(transformed_df: DataFrame) -> StreamingQuery:
    """
    Attaches the BigQuery writer to the streaming DataFrame.

    processingTime='30 seconds' — Spark collects messages for 30 seconds
    then writes them all as one batch. More efficient than writing each
    message individually. Tune this up/down based on latency needs.

    checkpointLocation — Spark writes its progress here after every batch.
    On restart, it reads this and resumes from the exact Kafka offset it
    left off at. No data loss, no duplicates.
    """
    return transformed_df \
        .writeStream \
        .foreachBatch(write_to_bigquery) \
        .option("checkpointLocation", "/tmp/spark-checkpoints/transactions") \
        .trigger(processingTime="30 seconds") \
        .start()