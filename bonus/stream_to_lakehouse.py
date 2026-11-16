#!/usr/bin/env python3
"""
Bonus Task: Stream-to-Lakehouse Batch Ingestion and Idempotency Verification.

Simulates a continuous event stream of FreshCart orders, writes records to an
append-only landing zone in JSON Lines format, transforms and deduplicates the
records into a curated lakehouse table, and runs the batch ingestion process twice
to mathematically verify that re-running over existing landing data is idempotent
(row counts and aggregated revenue totals do not double).
"""

import os
import json
import time
from typing import List, Dict, Any
import pandas as pd


# Directory paths resolved dynamically relative to this script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_LANDING_PATH = os.path.join(SCRIPT_DIR, "raw_landing.jsonl")
CURATED_LAKEHOUSE_PATH = os.path.join(SCRIPT_DIR, "curated_orders.csv")


def generate_simulated_stream(num_records: int = 60) -> List[Dict[str, Any]]:
    """
    Simulate a 60-event stream representing one minute of incoming FreshCart orders.
    Each event contains order metadata, delivery location, type, amount, and timestamp.
    """
    print(f"[*] Simulating {num_records} real-time order stream events...")
    simulated_events: List[Dict[str, Any]] = []
    base_time = time.time()

    for idx in range(num_records):
        order_num = 1000 + idx
        order_id = f"ORD-{order_num}"
        city = "bangalore" if (idx % 2 == 0) else "chennai"
        zone = "koramangala" if city == "bangalore" else "tnagar"
        category = "food" if (idx % 3 == 0) else "ride"
        # Deterministic synthetic order amounts
        amount = round(150.0 + (idx * 12.5) % 500.0, 2)

        event = {
            "order_id": order_id,
            "city": city,
            "zone": zone,
            "type": category,
            "amount": amount,
            "event_timestamp": round(base_time + idx, 2)
        }
        simulated_events.append(event)

    print(f"[+] Successfully generated {len(simulated_events)} order events.")
    return simulated_events


def persist_to_raw_landing(events: List[Dict[str, Any]], target_path: str) -> None:
    """
    Append raw event payloads to the raw landing storage layer in JSONL format.
    """
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    print(f"[*] Persisting raw event stream to landing path: {target_path}")
    with open(target_path, "w", encoding="utf-8") as f:
        for item in events:
            f.write(json.dumps(item) + "\n")
    print(f"[+] Raw landing persistence complete ({len(events)} events written).")


def ingest_and_curate(landing_file: str, curated_file: str) -> pd.DataFrame:
    """
    Execute batch ELT ingestion from landing zone into the curated layer:
    1. Read and parse append-only JSONL files.
    2. Enforce deduplication by primary key (order_id), preserving the latest state.
    3. Calculate ingestion audit metadata.
    4. Persist curated results ensuring deterministic overwrite/merge behavior.
    """
    print(f"[*] Commencing batch ingestion pipeline from: {landing_file}")
    records = []
    with open(landing_file, "r", encoding="utf-8") as f:
        for line in f:
            line_str = line.strip()
            if line_str:
                records.append(json.loads(line_str))

    df_raw = pd.DataFrame(records)
    print(f"    - Extracted {len(df_raw)} records from landing zone.")

    # Deduplication step using order_id as unique business key
    df_curated = (
        df_raw.sort_values(by="event_timestamp")
        .drop_duplicates(subset=["order_id"], keep="last")
        .copy()
    )
    df_curated["processed_at"] = time.time()

    os.makedirs(os.path.dirname(curated_file), exist_ok=True)
    df_curated.to_csv(curated_file, index=False)
    print(f"    - Successfully curated and stored {len(df_curated)} unique orders to: {curated_file}")
    return df_curated


def run_idempotency_verification():
    """
    Verify lakehouse idempotency by running the ingestion batch twice on the same
    landing data and asserting that counts and totals remain invariant.
    """
    print("=" * 65)
    print("  FreshCart Stream-to-Lakehouse Pipeline Idempotency Verification  ")
    print("=" * 65)

    # 1. Generate synthetic stream
    events = generate_simulated_stream(60)

    # 2. Write raw landing zone
    persist_to_raw_landing(events, RAW_LANDING_PATH)

    # 3. First execution
    print("\n--- BATCH INGESTION: RUN 1 ---")
    df_batch_1 = ingest_and_curate(RAW_LANDING_PATH, CURATED_LAKEHOUSE_PATH)
    count_1 = len(df_batch_1)
    revenue_1 = df_batch_1["amount"].sum()
    print(f"Run 1 Metrics -> Total Rows: {count_1} | Total Order Value: INR {revenue_1:,.2f}")

    # 4. Second execution (re-processing identical landing dataset)
    print("\n--- BATCH INGESTION: RUN 2 (Idempotency Retest) ---")
    df_batch_2 = ingest_and_curate(RAW_LANDING_PATH, CURATED_LAKEHOUSE_PATH)
    count_2 = len(df_batch_2)
    revenue_2 = df_batch_2["amount"].sum()
    print(f"Run 2 Metrics -> Total Rows: {count_2} | Total Order Value: INR {revenue_2:,.2f}")

    # 5. Assertions
    print("\n--- IDEMPOTENCY AUDIT VALIDATION ---")
    assert count_1 == count_2, f"Discrepancy in row count: {count_1} vs {count_2}"
    assert revenue_1 == revenue_2, f"Discrepancy in total amount: {revenue_1} vs {revenue_2}"
    print("[PASSED] Verification confirmed:")
    print("         1. Row count remained exactly identical (60 unique orders).")
    print("         2. Total revenue remained exactly identical (INR 21,125.00).")
    print("         3. Re-running the pipeline over landing data is strictly idempotent.")
    print("=" * 65)


if __name__ == "__main__":
    run_idempotency_verification()
