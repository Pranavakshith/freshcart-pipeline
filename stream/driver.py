#!/usr/bin/env python3
"""
FreshCart Event Streaming - Driver Consumer (Part D)
Author: Pranavakshith (23BCS054)

Consumes order request messages from RabbitMQ topic exchange 'rapido-dispatch'
matching a driver-specific topic binding pattern.

Implements Part D3 ACK / NACK protocol:
- Malformed payloads (missing 'zone' attribute or invalid JSON) are rejected
  without requeuing using `basic_nack(requeue=False)`.
- Valid messages are processed and acknowledged using `basic_ack()`.
"""

from __future__ import annotations
import os
import sys
import json
import argparse
from typing import Optional

try:
    import pika
except ImportError:
    pika = None

EXCHANGE_NAME = "rapido-dispatch"
EXCHANGE_TYPE = "topic"


def get_connection(amqp_url: Optional[str] = None) -> pika.BlockingConnection:
    """Establish a robust blocking AMQP connection to RabbitMQ."""
    url = amqp_url or os.environ.get("AMQP_URL", "amqp://guest:guest@localhost:5672/")
    params = pika.URLParameters(url)
    return pika.BlockingConnection(params)


def process_message(driver_name: str, ch, method, body: bytes) -> None:
    """
    Evaluate incoming order message against quality and schema rules.
    - Successfully parsed messages with a valid 'zone' field are ACKed.
    - Messages failing JSON decoding or lacking 'zone' are NACKed (requeue=False).
    """
    tag = method.delivery_tag
    rk = method.routing_key

    # 1. JSON parsing check
    try:
        decoded_str = body.decode("utf-8")
        event = json.loads(decoded_str)
    except Exception as parse_err:
        print(f"[{driver_name}] [NACK] Failed to parse message body as JSON: {parse_err}")
        ch.basic_nack(delivery_tag=tag, requeue=False)
        return

    # 2. Schema validation check (Part D3 requirement)
    zone_val = event.get("zone")
    if not zone_val:
        order_id = event.get("id", "UNKNOWN")
        print(f"[{driver_name}] [NACK - REJECTED] Order {order_id} missing mandatory 'zone' attribute! Dropping without requeue.")
        ch.basic_nack(delivery_tag=tag, requeue=False)
    else:
        order_id = event.get("id", "N/A")
        print(f"[{driver_name}] [ACK - ACCEPTED] Order {order_id} ({rk}) accepted for fulfillment by {driver_name}.")
        ch.basic_ack(delivery_tag=tag)


def start_driver_consumer(driver_name: str, binding_pattern: str, amqp_url: Optional[str] = None) -> None:
    """Initialize driver consumer queue and attach message callback."""
    conn = get_connection(amqp_url)
    channel = conn.channel()

    # Declare the topic exchange
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE, durable=False)

    # Declare exclusive, transient queue for this driver
    q_result = channel.queue_declare(queue="", exclusive=True, auto_delete=True)
    queue_name = q_result.method.queue

    # Bind queue to exchange using the driver's routing pattern
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key=binding_pattern)

    print(f"[*] Driver '{driver_name}' active.")
    print(f"    Exchange : {EXCHANGE_NAME}")
    print(f"    Queue    : {queue_name}")
    print(f"    Binding  : {binding_pattern}")
    print("    Awaiting incoming dispatch events (Ctrl+C to quit)...\n")

    def consumer_callback(ch, method, properties, body):
        process_message(driver_name, ch, method, body)

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=consumer_callback,
        auto_ack=False
    )

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print(f"\n[*] Driver '{driver_name}' disconnecting...")
    finally:
        if conn.is_open:
            conn.close()
        print(f"[*] Driver '{driver_name}' connection terminated.")


def main():
    # Support both positional args and flag args
    if len(sys.argv) >= 3 and not sys.argv[1].startswith("-"):
        driver_name = sys.argv[1]
        binding_pattern = sys.argv[2]
        amqp_url = sys.argv[3] if len(sys.argv) > 3 else None
    else:
        parser = argparse.ArgumentParser(description="FreshCart Driver Event Consumer")
        parser.add_argument("name", nargs="?", default="Driver_1", help="Driver Identifier Name")
        parser.add_argument("pattern", nargs="?", default="bangalore.#", help="AMQP Topic Binding Pattern")
        parser.add_argument("--url", type=str, default=None, help="RabbitMQ AMQP Broker URL")
        args = parser.parse_args()
        driver_name = args.name
        binding_pattern = args.pattern
        amqp_url = args.url

    start_driver_consumer(driver_name, binding_pattern, amqp_url)


if __name__ == "__main__":
    main()
