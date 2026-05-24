import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "eth_topic": os.getenv("ETH_TOPIC", "eth_raw_transactions"),
    "base_topic": os.getenv("BASE_TOPIC", "base_raw_transactions"),
}

ALCHEMY_CONFIG = {
    "eth_key": os.getenv("ALCHEMY_ETH_KEY"),
    "base_key": os.getenv("ALCHEMY_BASE_KEY"),
    "eth_ws_url": f"wss://eth-mainnet.g.alchemy.com/v2/{os.getenv('ALCHEMY_ETH_KEY')}",
    "base_ws_url": f"wss://base-mainnet.g.alchemy.com/v2/{os.getenv('ALCHEMY_BASE_KEY')}",
}

BIGQUERY_CONFIG = {
    "project_id": os.getenv("GCP_PROJECT_ID", "crypto-analytics"),
    "dataset": os.getenv("BQ_DATASET", "raw"),
    "table": os.getenv("BQ_TABLE", "transactions"),
}