import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
from confluent_kafka import Consumer

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DUCKDB_PATH = DATA_DIR / "order_intelligence.duckdb"
TOPICS = [
    "orderserver.public.customers",
    "orderserver.public.products",
    "orderserver.public.orders",
    "orderserver.public.order_items",
    "orderserver.public.payments",
]


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(Decimal(str(value)))
    except Exception:
        try:
            return float(value)
        except Exception:
            return None




def to_timestamp(value: Any):
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value

    # Debezium temporal fields can arrive either as ISO strings or epoch numbers
    if isinstance(value, (int, float)):
        iv = int(value)
        digits = len(str(abs(iv)))
        try:
            if digits >= 18:
                return datetime.fromtimestamp(iv / 1_000_000_000, tz=timezone.utc).replace(tzinfo=None)
            if digits >= 15:
                return datetime.fromtimestamp(iv / 1_000_000, tz=timezone.utc).replace(tzinfo=None)
            if digits >= 12:
                return datetime.fromtimestamp(iv / 1_000, tz=timezone.utc).replace(tzinfo=None)
            return datetime.fromtimestamp(iv, tz=timezone.utc).replace(tzinfo=None)
        except Exception:
            return None

    s = str(value).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:19], fmt)
        except Exception:
            continue
    return None


def setup_db(con: duckdb.DuckDBPyConnection) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_cdc_events (
            entity VARCHAR,
            kafka_topic VARCHAR,
            kafka_partition INTEGER,
            kafka_offset BIGINT,
            op VARCHAR,
            event_ts_ms BIGINT,
            payload JSON,
            processed_at TIMESTAMP DEFAULT current_timestamp
        )
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS customers_current (
            customer_id INTEGER PRIMARY KEY,
            customer_name VARCHAR,
            city VARCHAR,
            signup_ts TIMESTAMP,
            loyalty_tier VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS products_current (
            product_id INTEGER PRIMARY KEY,
            product_name VARCHAR,
            category VARCHAR,
            unit_price DOUBLE
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS orders_current (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            order_ts TIMESTAMP,
            status VARCHAR,
            total_amount DOUBLE
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS order_items_current (
            order_item_id INTEGER PRIMARY KEY,
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            unit_price DOUBLE,
            line_amount DOUBLE
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS payments_current (
            payment_id INTEGER PRIMARY KEY,
            order_id INTEGER,
            payment_ts TIMESTAMP,
            payment_status VARCHAR,
            amount DOUBLE
        )
        """
    )


def entity_from_topic(topic: str) -> str:
    return topic.split('.')[-1]


def insert_raw(con: duckdb.DuckDBPyConnection, topic: str, partition: int, offset: int, payload_dict: dict, payload_text: str) -> None:
    con.execute(
        """
        INSERT INTO raw_cdc_events (entity, kafka_topic, kafka_partition, kafka_offset, op, event_ts_ms, payload)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            entity_from_topic(topic),
            topic,
            partition,
            offset,
            payload_dict.get("op"),
            payload_dict.get("ts_ms"),
            payload_text,
        ],
    )


def upsert_customer(con: duckdb.DuckDBPyConnection, after: dict) -> None:
    con.execute(
        """
        INSERT OR REPLACE INTO customers_current (customer_id, customer_name, city, signup_ts, loyalty_tier)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            int(after["customer_id"]),
            after.get("customer_name"),
            after.get("city"),
            to_timestamp(after.get("signup_ts")),
            after.get("loyalty_tier"),
        ],
    )


def upsert_product(con: duckdb.DuckDBPyConnection, after: dict) -> None:
    con.execute(
        """
        INSERT OR REPLACE INTO products_current (product_id, product_name, category, unit_price)
        VALUES (?, ?, ?, ?)
        """,
        [
            int(after["product_id"]),
            after.get("product_name"),
            after.get("category"),
            to_float(after.get("unit_price")),
        ],
    )


def upsert_order(con: duckdb.DuckDBPyConnection, after: dict) -> None:
    con.execute(
        """
        INSERT OR REPLACE INTO orders_current (order_id, customer_id, order_ts, status, total_amount)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            int(after["order_id"]),
            int(after["customer_id"]),
            to_timestamp(after.get("order_ts")),
            after.get("status"),
            to_float(after.get("total_amount")),
        ],
    )


def upsert_order_item(con: duckdb.DuckDBPyConnection, after: dict) -> None:
    con.execute(
        """
        INSERT OR REPLACE INTO order_items_current (order_item_id, order_id, product_id, quantity, unit_price, line_amount)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            int(after["order_item_id"]),
            int(after["order_id"]),
            int(after["product_id"]),
            int(after.get("quantity", 1)),
            to_float(after.get("unit_price")),
            to_float(after.get("line_amount")),
        ],
    )


def upsert_payment(con: duckdb.DuckDBPyConnection, after: dict) -> None:
    con.execute(
        """
        INSERT OR REPLACE INTO payments_current (payment_id, order_id, payment_ts, payment_status, amount)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            int(after["payment_id"]),
            int(after["order_id"]),
            to_timestamp(after.get("payment_ts")),
            after.get("payment_status"),
            to_float(after.get("amount")),
        ],
    )


def apply_change(con: duckdb.DuckDBPyConnection, topic: str, payload_dict: dict) -> None:
    entity = entity_from_topic(topic)
    op = payload_dict.get("op")
    before = payload_dict.get("before")
    after = payload_dict.get("after")

    if entity == "customers":
        if op == "d" and before:
            con.execute("DELETE FROM customers_current WHERE customer_id = ?", [int(before["customer_id"])])
        elif after:
            upsert_customer(con, after)
    elif entity == "products":
        if op == "d" and before:
            con.execute("DELETE FROM products_current WHERE product_id = ?", [int(before["product_id"])])
        elif after:
            upsert_product(con, after)
    elif entity == "orders":
        if op == "d" and before:
            con.execute("DELETE FROM orders_current WHERE order_id = ?", [int(before["order_id"])])
        elif after:
            upsert_order(con, after)
    elif entity == "order_items":
        if op == "d" and before:
            con.execute("DELETE FROM order_items_current WHERE order_item_id = ?", [int(before["order_item_id"])])
        elif after:
            upsert_order_item(con, after)
    elif entity == "payments":
        if op == "d" and before:
            con.execute("DELETE FROM payments_current WHERE payment_id = ?", [int(before["payment_id"])])
        elif after:
            upsert_payment(con, after)


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:29092",
            "group.id": "order-intelligence-consumer",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(TOPICS)

    con = duckdb.connect(str(DUCKDB_PATH))
    setup_db(con)

    print("CDC consumer started. Listening on localhost:29092 ...")
    print("Leave this window open. Press Ctrl+C to stop.")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            raw_value = msg.value()
            if raw_value is None:
                continue

            payload_text = raw_value.decode("utf-8")
            try:
                payload_dict = json.loads(payload_text)
            except json.JSONDecodeError:
                print(f"Skipping invalid JSON from {msg.topic()} offset {msg.offset()}")
                continue

            try:
                insert_raw(con, msg.topic(), msg.partition(), msg.offset(), payload_dict, payload_text)
                apply_change(con, msg.topic(), payload_dict)
                print(f"Processed {msg.topic()} offset={msg.offset()} op={payload_dict.get('op')}")
            except Exception as e:
                print(f"Failed on topic={msg.topic()} offset={msg.offset()} op={payload_dict.get('op')}: {e}")
                print(payload_text[:1000])
                continue
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()
        con.close()


if __name__ == "__main__":
    main()
