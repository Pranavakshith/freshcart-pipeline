#!/usr/bin/env python3
"""
FreshCart Event Streaming - Order Dispatcher (Part D)
Author: Pranavakshith (23BCS054)

Publishes real-time order requests to RabbitMQ topic exchange 'rapido-dispatch'.
Routing keys follow the topic hierarchy: `<city>.<zone>.<service_type>`.
Every 10th message is intentionally malformed (missing the 'zone' attribute)
to trigger and test Part D3 ACK/NACK dead-letter / rejection handling in consumers.
"""

from __future__ import annotations
import os
import sys
import time
import json
import random
import argparse
from typing import Tuple, Dict, Any, Optional

try:
    import pika
except ImportError:
    pika = None

EXCHANGE_NAME = "rapido-dispatch"
EXCHANGE_TYPE = "topic"

# Available routing key dimensions
CITIES = ["bangalore", "chennai"]
ZONES = {
    "bangalore": ["koramangala", "indiranagar", "whitefield"],
    "chennai": ["tnagar", "adyar", "velachery"]
}
TYPES = ["ride", "food"]


def get_connection(amqp_url: Optional[str] = None) -> pika.BlockingConnection:
    """Establish a robust blocking AMQP connection to RabbitMQ."""
    url = amqp_url or os.environ.get("AMQP_URL", "amqp://guest:guest@localhost:5672/")
    params = pika.URLParameters(url)
    return pika.BlockingConnection(params)


def build_message(sequence_number: int) -> Tuple[str, Dict[str, Any]]:
    """
    Construct a structured event payload and corresponding AMQP routing key.
    
    Normal messages:
      - Routing Key: <city>.<zone>.<type> (3 parts)
      - Payload contains: id, city, zone, type, ts
      
    Every 10th message (Part D3 intentional anomaly):
      - Missing 'zone' attribute
      - Routing Key: <city>.<type> (2 parts)
      - Used to verify that downstream drivers reject with basic_nack(requeue=False).
    """
    selected_city = random.choice(CITIES)
    selected_type = random.choice(TYPES)
    is_anomaly = (sequence_number % 10 == 0)

    if is_anomaly:
        # Deliberately malformed event missing 'zone'
        routing_key = f"{selected_city}.{selected_type}"
        payload = {
            "id": f"REQ-{sequence_number:04d}",
            "city": selected_city,
            "type": selected_type,
            "ts": round(time.time(), 3),
            "malformed": True
        }
    else:
        selected_zone = random.choice(ZONES[selected_city])
        routing_key = f"{selected_city}.{selected_zone}.{selected_type}"
        payload = {
            "id": f"REQ-{sequence_number:04d}",
            "city": selected_city,
            "zone": selected_zone,
            "type": selected_type,
            "ts": round(time.time(), 3)
        }

    return routing_key, payload


def run_dispatcher(
    exchange: str = EXCHANGE_NAME,
    interval_seconds: float = 2.0,
    max_count: Optional[int] = None,
    amqp_url: Optional[str] = None
) -> None:
    """Run the main event publishing loop."""
    conn = get_connection(amqp_url)
    channel = conn.channel()
    channel.exchange_declare(exchange=exchange, exchange_type=EXCHANGE_TYPE, durable=False)

    print(f"[*] Dispatcher online. Publishing to exchange '{exchange}' (interval: {interval_seconds}s).")
    print("    Press Ctrl+C to stop publishing.\n")

    counter = 0
    try:
        while True:
            counter += 1
            if max_count and counter > max_count:
                print(f"[*] Reached maximum message quota ({max_count}). Terminating gracefully.")
                break

            rk, msg_body = build_message(counter)
            json_payload = json.dumps(msg_body)

            if msg_body.get("malformed", False):
                print(f"[!] [MSG #{counter:04d} - ANOMALY] Published invalid message (missing zone): RK='{rk}'")
            else:
                print(f"[+] [MSG #{counter:04d}] Published order event: RK='{rk}' -> ID={msg_body['id']}")

            channel.basic_publish(
                exchange=exchange,
                routing_key=rk,
                body=json_payload
            )

            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        print("\n[*] Dispatcher shutdown signal received (Ctrl+C).")
    finally:
        if conn.is_open:
            conn.close()
        print("[*] RabbitMQ broker connection closed.")


def main():
    parser = argparse.ArgumentParser(description="FreshCart Order Dispatcher Publisher")
    parser.add_argument("--interval", type=float, default=2.0, help="Interval between dispatches in seconds")
    parser.add_argument("--count", type=int, default=None, help="Maximum number of messages to dispatch")
    parser.add_argument("--url", type=str, default=None, help="RabbitMQ AMQP connection URL")
    args = parser.parse_args()

    run_dispatcher(interval_seconds=args.interval, max_count=args.count, amqp_url=args.url)


if __name__ == "__main__":
    main()
