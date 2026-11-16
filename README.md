# FreshCart Pipelines: From Git to Gold

**Student Name:** Pranavakshith  
**Roll Number:** 23BCS054  
**Course:** Data Engineering · Pipeline Orchestration  
**GitHub Repository:** [https://github.com/Pranavakshith/freshcart-pipeline.git](https://github.com/Pranavakshith/freshcart-pipeline.git)  

---

## 1. Project Overview

This repository houses the end-to-end implementation of the **FreshCart Data Engineering and Pipeline Orchestration Platform**. The project unites version control, continuous integration, cloud data warehousing via ELT on Snowflake, orchestrated MLOps pipelines using Mage AI, real-time distributed event streaming with RabbitMQ, and lakehouse batch ingestion idempotency.

---

## 2. Repository Layout

```
freshcart-pipelines/
├── .github/
│   └── workflows/
│       └── ci.yml               # GitHub Actions CI workflow (Python 3.12 + Pytest)
├── .gitignore                   # Ignores .env, *.pkl, *.csv, mage.log, __pycache__, etc.
├── requirements.txt             # Project dependencies (pytest, scikit-learn, pandas, pika, tqdm)
├── README.md                    # Project documentation and architecture guide
├── REPORT.md                    # Comprehensive technical report covering Parts A–E & Bonus
├── 23BCS054_Pranavakshith_Pipelines.pdf # Formatted submission PDF report
├── elt/
│   ├── freshcart_elt.ipynb      # Baseline FreshCart ELT notebook (Snowflake Bronze -> Silver -> Gold)
│   └── freshcart_elt_edited.ipynb # Updated notebook with validation rules, SCD-2, and quality gates
├── iris_project/                # Mage AI ML pipeline project
│   ├── metadata.yaml
│   ├── README.md
│   ├── data_loaders/
│   │   └── load_iris.py         # Fisher's Iris dataset loader
│   ├── transformers/
│   │   ├── validate_iris.py     # Custom data-quality & schema validator block (Part C4)
│   │   ├── prepare_data.py      # Stratified 80/20 train-test partitioner
│   │   ├── train_model.py       # DecisionTreeClassifier with max_depth & accuracy test gate
│   │   └── evaluate_model.py    # Multi-class metrics & confusion matrix evaluator (Part C3)
│   ├── data_exporters/
│   │   └── save_results.py      # Exports test predictions to CSV
│   └── pipelines/
│       └── iris_classifier/
│           └── metadata.yaml    # DAG topology & dependency definitions
├── stream/                      # RabbitMQ real-time event streaming (Part D)
│   ├── dispatcher.py            # Order event publisher with intentional 10th-message anomaly
│   ├── driver.py                # Delivery driver consumer with ACK / NACK handling
│   ├── simulation.py            # AMQP topic exchange routing and ACK/NACK simulator
│   └── README.md
├── tests/                       # Automated unit tests for CI/CD
│   ├── conftest.py
│   ├── test_prepare_data.py     # Pytest assertion for Mage prepare_data block (Part A3)
│   └── test_stream.py           # Unit tests for dispatcher & driver ACK/NACK logic
├── bonus/                       # Stream-to-Lakehouse idempotency extension
│   ├── stream_to_lakehouse.py   # 1-minute streaming buffer, landing, and idempotent batch curation
│   └── raw_landing.jsonl        # Raw landing zone append-only event stream
└── screenshots/                 # Execution evidence for Snowflake, Mage, CI, and Streaming
```

---

## 3. Pipeline Modules Summary

### Part A: Git & Continuous Integration
- Multi-commit incremental version control tracking each developmental milestone.
- Comprehensive analysis of Git command mechanics across the 4-box architecture (Working Directory → Staging Area → Local Repository → Remote Repository).
- Continuous Integration pipeline configured via `.github/workflows/ci.yml` running unit tests on Python 3.12 across all pushes and pull requests.

### Part B: FreshCart ELT on Snowflake
- **Bronze Layer (`bronze.orders_raw`)**: Raw append-only ingestion of heterogeneous payloads (flat web JSON and nested mobile JSON).
- **Silver Layer (`silver.orders_clean`, `quarantine.orders`)**: Unified schemas, strict validation (email regex, 6-digit postal code, parseable timestamps, INR/USD currency check), and deduplication (`QUALIFY ROW_NUMBER()`).
- **Gold Layer (`gold.fct_orders`)**: Point-in-time SCD Type 2 dimension joins (`dim.customers_scd2`), FX currency normalization, loyalty point multiplier arithmetic, and idempotent `MERGE` via deterministic `row_hash`.
- **Quality Gates**: Pipeline assertions halting propagation upon negative loyalty points or invalid records.

### Part C: Orchestrated ML Pipeline with Mage
- DAG pipeline executing `load_iris → validate_iris → prepare_data → train_model → (save_results, evaluate_model)`.
- Quality gates using Mage `@test` decorators preventing corrupted models or datasets from propagating downstream.
- Runtime hyperparameter evaluation (`max_depth = 1..5`) proving quality gate efficacy.
- Custom validation transformer ensuring data consistency (150 rows, zero nulls, 3 balanced classes).

### Part D: Real-Time Event Streaming with RabbitMQ
- AMQP 0-9-1 Topic Exchange `rapido-dispatch` with hierarchical routing keys: `<city>.<zone>.<service_type>`.
- Topic matching using `*` (exactly 1 word) and `#` (0 or more words).
- Consumer ACK / NACK protocol: valid messages receive `basic_ack()`; malformed payloads (missing `zone`) are rejected without requeue via `basic_nack(requeue=False)` to prevent poison pill loops.

### Bonus: Stream into Lakehouse Idempotency
- Ingestion of real-time stream into append-only landing storage (`raw_landing.jsonl`).
- Deduplication and curation by business key (`order_id`).
- Idempotency verification: re-running batch curation twice produces identical row counts (60) and revenue totals (₹21,125.00).

---

## 4. Getting Started & Testing

### Installation
Clone repository and install dependencies:
```bash
git clone https://github.com/Pranavakshith/freshcart-pipeline.git
cd freshcart-pipeline
pip install -r requirements.txt
```

### Running Test Suites
Execute the automated test suite locally:
```bash
pytest
```
or via standard Python unittest:
```bash
python -m unittest discover -s tests
```

### Running Simulations
Test the streaming routing table and ACK/NACK behavior:
```bash
python stream/simulation.py
```

Test the stream-to-lakehouse batch ingestion and idempotency proof:
```bash
python bonus/stream_to_lakehouse.py
```
