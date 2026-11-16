# FreshCart Event Streaming (RabbitMQ)

This package implements the event streaming component (Part D) of the FreshCart pipeline architecture.

## Overview
- **`dispatcher.py`**: Simulates the central order routing service. Generates and publishes real-time order events every 2 seconds to the `rapido-dispatch` AMQP topic exchange with hierarchical routing keys: `<city>.<zone>.<service_type>`. Every 10th event is intentionally malformed (omitting the `zone` attribute) to trigger error handling.
- **`driver.py`**: Simulates delivery driver consumers. Declares exclusive, auto-delete queues bound to specific AMQP topic binding patterns. Enforces Part D3 ACK/NACK protocol: valid events are acknowledged via `basic_ack()`; malformed events are dropped without requeue via `basic_nack(requeue=False)`.
- **`simulation.py`**: Standalone AMQP simulation harness that evaluates the Part D1 routing matrix against the 6 canonical test keys and emulates real-time dispatcher publishing and multi-driver consumption.

## Usage
Run the standalone simulation:
```bash
python stream/simulation.py
```

Run against a live RabbitMQ broker:
```bash
# Terminal 1 - Start Driver
python stream/driver.py Ravi "bangalore.koramangala.*"

# Terminal 2 - Start Dispatcher
python stream/dispatcher.py
```
