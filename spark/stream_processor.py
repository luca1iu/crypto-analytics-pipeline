import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession

# When running inside Docker, these files are at /opt/spark-jobs/
# When running locally, they're in the spark/ folder.
# sys.path ensures Python finds them either way.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from transformations import parse_kafka_messages
from bq_writer import start_streaming_query

load_dotenv()

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ETH_TOPIC     = os.getenv("ETH_TOPIC",  "eth_raw_transactions")
BASE_TOPIC    = os.getenv("BASE_TOPIC", "base_raw_transactions")
CREDENTIALS   = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "./gcp-key.json")


def create_spark_session() -> SparkSession:
    """
    Build SparkSession with Kafka and BigQuery connectors.

    spark.jars.packages tells Spark to download these JARs automatically
    on first run (cached after that). They provide:
      - spark-sql-kafka: readStream.format("kafka")
      - spark-bigquery:  write.format("bigquery")
    """
    return SparkSession.builder \
        .appName("CryptoAnalyticsPipeline") \
        .config("spark.sql.streaming.checkpointLocation",
                "/tmp/spark-checkpoints") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable",
                "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                CREDENTIALS) \
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()


def read_from_kafka(spark: SparkSession):
    """
    Subscribe to both ETH and Base topics as a single unified stream.
    Spark merges them into one infinite DataFrame — the 'chain' column
    inside each message tells them apart.

    startingOffsets=latest  → only process new messages from now on.
    Change to 'earliest'    → reprocess everything stored in Kafka.
    failOnDataLoss=false    → don't crash if Kafka deletes old messages
                              (happens after 7-day retention window).
    """
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("subscribe",               f"{ETH_TOPIC},{BASE_TOPIC}") \
        .option("startingOffsets",         "latest") \
        .option("failOnDataLoss",          "false") \
        .load()


def main():
    print("=" * 60)
    print("  Crypto Analytics — Spark Streaming Job")
    print("=" * 60)
    print(f"  Kafka:    {KAFKA_SERVERS}")
    print(f"  Topics:   {ETH_TOPIC}, {BASE_TOPIC}")
    print(f"  BQ Table: {os.getenv('GCP_PROJECT_ID')}"
          f".{os.getenv('BQ_DATASET')}.{os.getenv('BQ_TABLE')}")
    print("=" * 60)
    print()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # suppress verbose Spark INFO logs

    # Step 1 — read raw bytes from Kafka
    raw_df = read_from_kafka(spark)

    # Step 2 — parse JSON, enforce schema, convert timestamps
    transformed_df = parse_kafka_messages(raw_df)

    # Step 3 — write to BigQuery every 30 seconds
    query = start_streaming_query(transformed_df)

    print("Streaming job running. Writing to BigQuery every 30 seconds.")
    print("Press Ctrl+C to stop.\n")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping streaming job...")
        query.stop()
        spark.stop()
        print("Done.")


if __name__ == "__main__":
    main()