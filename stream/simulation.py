#!/usr/bin/env python3
"""
FreshCart AMQP Topic Routing & ACK/NACK Simulation (Part D)
Author: Pranavakshith (23BCS054)

Simulates AMQP 0-9-1 topic exchange routing rules and consumer ACK/NACK behaviors
without requiring an external broker daemon:
1. Validates the Part D1 routing matrix against 6 canonical test keys.
2. Emulates the dispatcher publishing 11 events (with event #10 intentionally malformed).
3. Evaluates driver queue delivery and ACK/NACK actions for each registered driver.
"""

import re
import time
import random
from typing import Dict, List, Tuple, Any

# Driver binding profiles as specified in Part D1
ROSTER_CONFIG: Dict[str, str] = {
    "Ravi": "bangalore.koramangala.*",
    "Priya": "bangalore.indiranagar.*",
    "Arjun": "bangalore.*.ride",
    "Meena": "bangalore.*.food",
    "Karthik": "chennai.tnagar.food",
    "Control": "bangalore.#"
}

# 6 test routing keys from Part D1
EVALUATION_KEYS: List[str] = [
    "bangalore.koramangala.food",
    "bangalore.indiranagar.ride",
    "bangalore.whitefield.ride",
    "chennai.tnagar.food",
    "chennai.whitefield.ride",
    "bangalore.koramangala"
]


def amqp_topic_matches(binding_pattern: str, routing_key: str) -> bool:
    """
    Standard AMQP 0-9-1 Topic Matching Logic:
    - Dot ('.') is word delimiter.
    - Asterisk ('*') matches exactly one word.
    - Hash ('#') matches zero or more words.
    """
    regex_builder = []
    tokens = binding_pattern.split('.')
    for token in tokens:
        if token == '*':
            regex_builder.append(r'[^.]+')
        elif token == '#':
            regex_builder.append(r'(?:[^.]+(?:\.[^.]+)*)?')
        else:
            regex_builder.append(re.escape(token))

    pattern_str = r'^\.' + r'\.'.join(regex_builder) + r'$'
    # Handle optional dots when '#' matches zero words
    pattern_str = pattern_str.replace(r'\.(?:[^.]+(?:\.[^.]+)*)?', r'(?:\.[^.]+)*')
    pattern_str = pattern_str.replace(r'(?:[^.]+(?:\.[^.]+)*)?\.', r'(?:[^.]+\.)*')
    
    # Direct reliable compilation:
    # Alternative standard token matching:
    return _token_match(binding_pattern.split('.'), routing_key.split('.'))


def _token_match(p_tokens: List[str], k_tokens: List[str]) -> bool:
    """Recursive token matcher for AMQP topic exchange."""
    if not p_tokens:
        return not k_tokens
    head, *tail = p_tokens
    if head == '#':
        # '#' can match 0 to len(k_tokens) words
        for i in range(len(k_tokens) + 1):
            if _token_match(tail, k_tokens[i:]):
                return True
        return False
    elif not k_tokens:
        return False
    elif head == '*' or head == k_tokens[0]:
        return _token_match(tail, k_tokens[1:])
    return False


def run_routing_table_verification():
    """Evaluate and display the theoretical routing table (Part D1)."""
    print("=" * 80)
    print("  PART D1: AMQP TOPIC ROUTING VERIFICATION TABLE  ")
    print("=" * 80)
    print(f"{'#':<3} | {'Routing Key':<30} | {'Matched Receiving Queues'}")
    print("-" * 80)

    for idx, rk in enumerate(EVALUATION_KEYS, 1):
        matched = [name for name, pattern in ROSTER_CONFIG.items() if _token_match(pattern.split('.'), rk.split('.'))]
        matched_str = str(matched) if matched else "[] (Dropped)"
        print(f"{idx:<3} | {rk:<30} | {matched_str}")
    print("=" * 80)


def run_stream_simulation(total_messages: int = 11) -> List[Tuple[str, Dict[str, Any]]]:
    """Simulate order generation and dispatching."""
    print("\n" + "=" * 80)
    print("  PART D2 & D3: DISPATCHER SIMULATION  ")
    print("=" * 80)
    print("[*] Dispatcher started -> Topic Exchange: 'rapido-dispatch'")

    published_stream = []
    for seq in range(1, total_messages + 1):
        is_bad = (seq % 10 == 0)
        if is_bad:
            city = "bangalore"
            req_type = "food"
            rk = f"{city}.{req_type}"
            body = {
                "id": f"REQ-{seq:04d}",
                "city": city,
                "type": req_type,
                "ts": round(time.time(), 3),
                "malformed": True
            }
            print(f"[!] [MSG #{seq:02d} MALFORMED] Missing zone -> RK='{rk}' -> Payload: {body}")
        else:
            city = random.choice(["bangalore", "chennai"])
            req_type = random.choice(["ride", "food"])
            zone_options = ["koramangala", "indiranagar", "whitefield"] if city == "bangalore" else ["tnagar", "adyar", "velachery"]
            zone = random.choice(zone_options)
            rk = f"{city}.{zone}.{req_type}"
            body = {
                "id": f"REQ-{seq:04d}",
                "city": city,
                "zone": zone,
                "type": req_type,
                "ts": round(time.time(), 3)
            }
            print(f"[+] [MSG #{seq:02d} VALID] RK='{rk}' -> Payload: {body}")

        published_stream.append((rk, body))

    print(f"[*] Dispatcher finished publishing {len(published_stream)} messages.\n")
    return published_stream


def run_driver_consumption_simulation(events: List[Tuple[str, Dict[str, Any]]]):
    """Simulate consumer queue receipt and ACK/NACK behavior."""
    print("=" * 80)
    print("  PART D2 & D3: DRIVER CONSUMER LOGS (ACK / NACK)  ")
    print("=" * 80)

    for driver_name, pattern in ROSTER_CONFIG.items():
        print(f"\n>>> Driver Instance: {driver_name} | Pattern: '{pattern}'")
        delivered = 0
        p_tokens = pattern.split('.')

        for rk, payload in events:
            k_tokens = rk.split('.')
            if _token_match(p_tokens, k_tokens):
                delivered += 1
                if "zone" not in payload or not payload["zone"]:
                    print(f"    [NACK] {driver_name} REJECTED invalid message (requeue=False): {payload}")
                else:
                    print(f"    [ACK]  {driver_name} ACKNOWLEDGED valid message: RK='{rk}' ID={payload['id']}")

        if delivered == 0:
            print(f"    [*] No events matched pattern '{pattern}'.")
        else:
            print(f"    [*] Total events routed to {driver_name}: {delivered}")

    print("\n" + "=" * 80)
    print("  SIMULATION COMPLETE  ")
    print("=" * 80)


if __name__ == "__main__":
    run_routing_table_verification()
    stream = run_stream_simulation(11)
    run_driver_consumption_simulation(stream)
