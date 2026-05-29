import asyncio
import json
import logging
import os
import sys
import time

import requests
import websockets
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ETH-PRODUCER] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
ALCHEMY_KEY   = os.getenv("ALCHEMY_ETH_KEY")
WS_URL        = f"wss://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_KEY}"
HTTP_URL      = f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_KEY}"
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC   = os.getenv("ETH_TOPIC", "eth_raw_transactions")
CHAIN         = "ethereum"


# ── Kafka producer ─────────────────────────────────────────────────────────────
def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",               # wait for broker to confirm
        retries=3,
        linger_ms=50,             # small batching window — reduces requests
    )

# ── Fetch full block via HTTP ──────────────────────────────────────────────────
def fetch_block(block_hex: str) -> dict | None:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getBlockByNumber",
        "params": [block_hex, True],   # True = include full tx objects
    }
    try:
        resp = requests.post(HTTP_URL, json=payload, timeout=10)
        resp.raise_for_status()
        return resp.json().get("result")
    except Exception as e:
        log.error(f"Failed to fetch block {block_hex}: {e}")
        return None

# ── Normalise a raw transaction ────────────────────────────────────────────────
def normalise_tx(tx: dict, block_timestamp_hex: str) -> dict:
    def hex_to_int(val):
        try:
            return int(val, 16) if val and val != "0x" else 0
        except (ValueError, TypeError):
            return 0

    value_wei    = hex_to_int(tx.get("value"))
    gas_price    = hex_to_int(tx.get("gasPrice"))
    gas_limit    = hex_to_int(tx.get("gas"))
    block_number = hex_to_int(tx.get("blockNumber"))
    timestamp    = hex_to_int(block_timestamp_hex)

    value_eth       = value_wei / 1e18
    gas_price_gwei  = gas_price / 1e9
    tx_fee_eth      = (gas_price * 21_000) / 1e18   # base fee estimate

    return {
        "chain":            CHAIN,
        "tx_hash":          tx.get("hash"),
        "block_number":     block_number,
        "block_timestamp":  timestamp,                 # unix epoch (seconds)
        "from_address":     tx.get("from", "").lower(),
        "to_address":       (tx.get("to") or "").lower(),  # None for contract deploys
        "value_eth":        round(value_eth, 10),
        "gas_limit":        gas_limit,
        "gas_price_gwei":   round(gas_price_gwei, 4),
        "tx_fee_eth":       round(tx_fee_eth, 10),
        "input_data":       tx.get("input", "0x"),
        "ingested_at":      int(time.time()),           # when YOUR pipeline processed it
    }

# ── Send a block's transactions to Kafka ──────────────────────────────────────
def send_block_to_kafka(producer: KafkaProducer, block: dict) -> int:
    transactions = block.get("transactions", [])
    timestamp    = block.get("timestamp", "0x0")
    sent         = 0

    for tx in transactions:
        normalised = normalise_tx(tx, timestamp)
        try:
            producer.send(
                KAFKA_TOPIC,
                key=normalised["tx_hash"],
                value=normalised,
            )
            sent += 1
        except KafkaError as e:
            log.error(f"Kafka send failed for tx {normalised['tx_hash']}: {e}")

    producer.flush()
    return sent

# ── WebSocket listener ─────────────────────────────────────────────────────────
async def stream_blocks(producer: KafkaProducer):
    subscribe_msg = json.dumps({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_subscribe",
        "params": ["newHeads"],   # subscribe to new block headers only
    })

    log.info(f"Connecting to Alchemy WebSocket ({CHAIN})...")

    async with websockets.connect(WS_URL, ping_interval=20) as ws:
        await ws.send(subscribe_msg)
        response = await ws.recv()
        log.info(f"Subscribed: {response}")

        log.info("Listening for new blocks. Press Ctrl+C to stop.\n")

        async for raw_msg in ws:
            msg = json.loads(raw_msg)

            # Skip subscription confirmation messages
            if "params" not in msg:
                continue

            block_data = msg["params"]["result"]
            block_hex  = block_data.get("number")
            block_num  = int(block_hex, 16)

            log.info(f"New block: #{block_num} — fetching transactions...")

            block = fetch_block(block_hex)
            if not block:
                log.warning(f"Empty block response for #{block_num}, skipping.")
                continue

            tx_count = len(block.get("transactions", []))
            sent     = send_block_to_kafka(producer, block)

            log.info(f"Block #{block_num}: {tx_count} transactions → {sent} sent to Kafka [{CHAIN}]")

# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    if not ALCHEMY_KEY:
        log.error("ALCHEMY_ETH_KEY not set in .env — aborting.")
        sys.exit(1)

    producer = make_producer()
    log.info(f"Kafka producer ready → topic: {KAFKA_TOPIC}")

    try:
        asyncio.run(stream_blocks(producer))
    except KeyboardInterrupt:
        log.info("Shutting down ETH producer.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()