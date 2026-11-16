# From Git to Gold: End-to-End Data Engineering Pipelines

**Student Name:** Pranavakshith  
**Roll Number:** 23BCS054  
**Course:** Data Engineering · Pipeline Orchestration  
**Date:** 2026-11-16  
**GitHub Repository:** [https://github.com/Pranavakshith/freshcart-pipeline.git](https://github.com/Pranavakshith/freshcart-pipeline.git)  

---

## Executive Summary

Modern enterprise data architectures require seamless interoperability across source versioning, continuous integration and deployment (CI/CD), scalable cloud data warehousing via Extract-Load-Transform (ELT), automated machine learning orchestration (MLOps), and event-driven distributed streaming. 

This technical report presents the complete end-to-end design, implementation, testing, and validation of the **FreshCart Data Platform**. The platform integrates:
1. **Source Versioning & CI/CD**: A Git repository structure governed by multi-commit discipline and automated testing via GitHub Actions.
2. **Cloud ELT on Snowflake**: A multi-tiered medallion warehouse (Bronze → Silver → Gold) featuring schema unification, data sanitization, custom currency validation, point-in-time Slowly Changing Dimensions (SCD Type 2), and idempotent `MERGE` publishing with deterministic row hashing.
3. **MLOps Orchestration with Mage AI**: A modular decision-tree pipeline incorporating schema validation gates, runtime hyperparameter optimization, automated model evaluation branching, and fail-fast quality checks.
4. **Real-Time Event Streaming via RabbitMQ**: An AMQP 0-9-1 topic exchange routing architecture handling ride and food requests across cities and zones, complete with dead-letter / rejection handling (`basic_ack` vs `basic_nack(requeue=False)`).
5. **Bonus Lakehouse Integration**: An event streaming landing zone demonstrating append-only persistence, batch deduplication, and mathematical proof of idempotent lakehouse ingestion.

---

## Part A: Git, CI/CD, and Test Automation (15 Marks)

### A1. Repository Architecture & Commit History (5 Marks)

The project repository was established under the prescribed organizational hierarchy:
```
freshcart-pipelines/
├── README.md                          <- Project documentation and runtime guidelines
├── .gitignore                         <- Ignores .env, *.pkl, *.csv, mage.log, __pycache__/
├── requirements.txt                   <- System dependencies (pytest, scikit-learn, etc.)
├── elt/                               <- FreshCart Snowflake ELT Jupyter notebooks
│   ├── freshcart_elt.ipynb            <- Baseline ELT pipeline
│   └── freshcart_elt_edited.ipynb     <- Updated ELT pipeline with validation & SCD2
├── iris_project/                      <- Mage AI MLOps orchestrator project
│   ├── data_loaders/load_iris.py
│   ├── transformers/validate_iris.py  <- Custom data-quality gate
│   ├── transformers/prepare_data.py   <- Stratified 80/20 train/test splitter
│   ├── transformers/train_model.py    <- Classifier with test gate
│   ├── transformers/evaluate_model.py <- Metrics & confusion matrix evaluator
│   ├── data_exporters/save_results.py <- Exporter block
│   └── pipelines/iris_classifier/metadata.yaml
├── stream/                            <- Real-time messaging scripts
│   ├── dispatcher.py                  <- Event publisher with intentional anomalies
│   ├── driver.py                      <- Consumer client implementing ACK/NACK
│   ├── simulation.py                  <- Standalone AMQP simulation harness
│   └── README.md
├── tests/                             <- Test automation suite
│   ├── conftest.py
│   ├── test_prepare_data.py           <- Pytest unit test for Mage data splitter
│   └── test_stream.py                 <- Unit tests for dispatcher & driver ACK/NACK
├── bonus/                             <- Stream-to-Lakehouse extension
│   ├── stream_to_lakehouse.py         <- Batch curation and idempotency validator
│   └── raw_landing.jsonl              <- Append-only landing event storage
├── screenshots/                       <- Verified visual execution evidence
└── .github/workflows/ci.yml           <- GitHub Actions CI automation workflow
```

Rather than a single monolithic commit, development was partitioned into distinct, meaningful commits reflecting each milestone.

**Output of `git log --oneline`:**
```text
2850d3d docs: complete comprehensive technical report and verification artifacts
e2c5a59 docs: add execution screenshots and verification evidence
5d0c75e feat(bonus): add stream-to-lakehouse batch ingestion and idempotency verification
0ec95c9 fix(ci): restore test_size to 0.2 to fix prepare_data test gate
2150245 test(ci): trigger red run by setting test_size to 0.3 in prepare_data
7129f6c test(ci): configure GitHub Actions CI workflow and Pytest test suites
ac8eafb feat(stream): implement RabbitMQ event streaming dispatcher, driver consumers, and simulation
005b2d0 feat(mlops): create Mage ML pipeline with Iris classifier, evaluation branch, and test gates
4e06178 feat(elt): implement custom currency validation, SCD-2 dimension joins, and quality checks
f5ea5d8 feat(elt): add FreshCart ELT pipeline notebook and raw-to-gold transforms
fd31ea7 chore: initial repository structure, .gitignore, and dependencies
```

---

### A2. Git Command Internal Mechanics (4 Marks)

Understanding how Git operates across its four storage domains (Working Directory, Staging Area / Index, Local Repository, and Remote Repository) is critical for team collaboration and reliable CI/CD pipelines.

| Command | Where the change happens | What exactly happens? |
| :--- | :--- | :--- |
| `git init` | Working Directory & `.git/` folder | Instantiates an empty local Git repository by generating the internal metadata directory (`.git/`) with object stores, hooks, and default branch references. |
| `git add train.py` | Staging Area (Git Index) | Computes a SHA-1 blob hash of `train.py`, saves the compressed blob into `.git/objects`, and registers the file path and hash into the index cache. |
| `git commit -m "msg"` | Local Repository (`HEAD` ref) | Writes a new commit object pointing to the staged tree object and previous commit parent, updating the local branch pointer to this commit. |
| `git remote add origin URL` | Local Configuration (`.git/config`) | Appends a remote reference mapping under the alias `origin` pointing to the designated remote repository URL. |
| `git push -u origin main` | Remote Git Server (GitHub) | Uploads local commit objects to GitHub, aligns the remote `main` branch with local `HEAD`, and establishes upstream tracking. |
| `git pull origin main` | Remote Server → Local Repo → Working Directory | Fetches remote commits and immediately merges them into the current active branch in the working tree. |

**Collaborative Code Visibility Question:**  
*After running `git commit`, can your teammate see your code on GitHub? Why or why not?*

**Answer:**  
No, teammates cannot see the committed code on GitHub. Git operates on a strictly distributed architecture where `git commit` writes changes exclusively to the developer's **local repository** on disk (within `.git/objects`). The commit remains isolated on the local workstation until an explicit network transmission occurs via `git push`. Only `git push` serializes the new commit objects and pushes them across the network to update the remote references on GitHub where collaborators can view them.

---

### A3. Continuous Integration Workflow & Pytest Gate (6 Marks)

To safeguard production pipelines against regressions, a Continuous Integration (CI) workflow was authored in `.github/workflows/ci.yml`. On every code push or pull request to `main`, GitHub Actions provisions an `ubuntu-latest` container, initializes Python 3.12, installs dependencies from `requirements.txt`, and triggers `pytest`.

Unit test `tests/test_prepare_data.py` validates that the Mage `prepare_data` transformer reliably partitions the 150-sample Iris dataset into an exact 80/20 train/test split:

```python
from sklearn.datasets import load_iris
from iris_project.transformers.prepare_data import transform

def test_split_is_80_20():
    df = load_iris(as_frame=True).frame
    out = transform(df)
    assert (out["split"] == "train").sum() == 120
    assert (out["split"] == "test").sum() == 30
```

#### CI Test Execution Screenshots:

1. **Failing Run (Red):**  
   Simulated by temporarily modifying `test_size=0.3` in `prepare_data.py`. The resulting 70/30 split produced 105 training rows, triggering an assertion failure in CI.  
   ![A3 Red Run](screenshots/A3-red_run.png)

2. **Passing Run (Green):**  
   Restoring `test_size=0.2` produced exactly 120 training samples and 30 test samples, restoring the green checkmark across the automated CI test pipeline.  
   ![A3 Green Run](screenshots/A3-green_run.png)

---

## Part B: FreshCart Cloud ELT on Snowflake (30 Marks)

### B1. Baseline Pipeline Run Verification (5 Marks)

The FreshCart ELT pipeline was executed on a cloud Snowflake instance. The pipeline ingests raw multi-channel e-commerce orders, standardizes data across staging layers, quarantines anomalies, resolves Slowly Changing Dimensions (SCD Type 2), and merges verified facts into business-ready Gold tables.

| Metric / Check | Value | Verification Notes |
| :--- | :--- | :--- |
| Raw order rows in Bronze after one run | **8** | 6 web records (`bronze.orders_raw`) + 2 mobile records |
| Rows passing cleaning (`silver.orders_clean`) | **6** | Orders with valid emails, dates, and non-corrupt fields |
| Rows quarantined (`quarantine.orders`) | **2** | `ORD-1004` (negative amount) and `ORD-1006` (invalid email & pincode) |
| Orders after deduplication (`silver.orders_dedup`) | **4** | Unique order entities: `ORD-1001`, `ORD-1002`, `ORD-1003`, `ORD-1005` |
| Final Gold Table line (`gold.fct_orders`) | **4 orders, ₹7,785.00, 126 pts** | Aggregated across all 4 verified transactions |
| Share of rows rejected | **25.0%** | $\frac{2 \text{ quarantined}}{8 \text{ total raw}} \times 100 = 25.0\%$ |

#### Baseline Run Verification Screenshots:
- **Snowflake Operations Run Log (`ops.run_log`):**  
  ![Ops Run Log](screenshots/ops.run_log.png)
- **Baseline Gold Table Aggregations:**  
  ![Baseline Gold Totals](screenshots/B-base_gold.png)

---

### B2. Hand Tracing of Pipeline Records (6 Marks)

#### 1. Detailed Trace: `ORD-1003`

- **Bronze Layer Payloads & Schema Divergence:**  
  `ORD-1003` arrives via two distinct API ingestion feeds:
  1. *Web Channel Export*: Flat JSON schema  
     `{"order_id": "ORD-1003", "cust_email": "meera@outlook.com", "cust_name": "MEERA S", "order_date": "2026-09-02", "amount": null, "currency": "INR", "pincode": "400050", "updated_at": "2026-09-02 09:10:00"}`
  2. *Mobile Channel Export*: Nested JSON schema with camelCase naming and day-first date formatting  
     `{"orderId": "ORD-1003", "customer": {"email": "Meera@Outlook.com", "name": "Meera S"}, "orderDate": "02/09/2026", "total": {"value": "2,100.00", "currency": "INR"}, "deliveryPin": "400050", "updatedAt": "2026-09-02T18:40:00"}`  
  *Root Cause of Divergence*: Client applications were engineered by decoupled frontend and mobile teams. Storing raw VARIANT payloads in Bronze preserves original telemetry without uncoordinated upstream constraints.

- **Silver Layer Column Harmonization (`SQL_10_ORDERS_CLEAN`):**  
  ```sql
  -- Web Mapping
  SELECT raw:order_id::STRING AS order_id, raw:amount::STRING AS amount, raw:updated_at::STRING AS updated_at ...
  UNION ALL
  -- Mobile Mapping
  SELECT raw:orderId::STRING AS order_id, raw:total.value::STRING AS amount, raw:updatedAt::STRING AS updated_at ...
  ```
  Type casting standardizes values:
  - `amount`: `TRY_TO_DECIMAL(REGEXP_REPLACE(amount, '[^0-9.-]', ''), 12, 2)` → Web yields `NULL`, Mobile yields `2100.00`.
  - `order_date`: `COALESCE(TRY_TO_DATE(order_date, 'YYYY-MM-DD'), TRY_TO_DATE(order_date, 'DD/MM/YYYY'))` → Resolves both to `2026-09-02`.

- **Deduplication Survival Logic (`SQL_20_ORDERS_DEDUP`):**  
  ```sql
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id 
    ORDER BY (amount IS NULL) ASC, updated_at DESC, ingest_seq DESC
  ) = 1
  ```
  Evaluation comparison between competing rows:
  - Web Record: `(amount IS NULL) = TRUE` (evaluates to 1), `updated_at = 09:10:00`.
  - Mobile Record: `(amount IS NULL) = FALSE` (evaluates to 0), `updated_at = 18:40:00`.  
  Because `(amount IS NULL) ASC` places records with valid amounts first, and `updated_at DESC` prefers later state updates, the **Mobile record wins rank 1** and is retained in Silver. The Web row with missing amount is pruned.

- **Gold Layer Monetary & Loyalty Arithmetic:**  
  - Currency conversion: Currency is `INR`, matching base rate $1.0$.
    $$\text{amount\_inr} = \text{ROUND}(2100.00 \times 1.0, 2) = ₹2,100.00$$
  - Loyalty point computation: Customer Meera (`meera@outlook.com`, `CUST-03`) is resolved against `dim.customers_scd2`. On `2026-09-02`, Meera holds **Platinum** tier, which maps to a **$3\times$ multiplier** in `ref.tier_multiplier`.
    $$\text{loyalty\_points} = \left\lfloor \frac{2100.00}{100} \right\rfloor \times 3 = 21 \times 3 = \mathbf{63 \text{ points}}$$

---

#### 2. SCD-2 Point-in-Time Trace: `ORD-1005` vs `ORD-1001` (Customer Priya)

Customer Priya (`priya.r@gmail.com`, `CUST-01`) placed two separate orders:
- `ORD-1001` on `2026-09-01` (amount ₹1,250.00)
- `ORD-1005` on `2026-09-03` (amount ₹899.00)

In `dim.customers_scd2`, Priya's historical tier progression is captured as:
1. Row 1: `tier = 'Silver'`, `valid_from = '2023-04-10'`, `valid_to = '2026-09-03'`
2. Row 2: `tier = 'Gold'`, `valid_from = '2026-09-03'`, `valid_to = '9999-12-31'`

The responsible table is **`dim.customers_scd2`**, joined using point-in-time temporal criteria:
```sql
LEFT JOIN dim.customers_scd2 c
  ON c.email = o.email
 AND o.order_date BETWEEN c.valid_from AND c.valid_to
```
- For `ORD-1001` (`2026-09-01`), the order date falls within Priya's Silver window (`2023-04-10` to `2026-09-03`), applying the **Silver multiplier ($1\times$)**:
  $$\text{points} = \lfloor 1250 / 100 \rfloor \times 1 = 12 \times 1 = 12 \text{ points}$$
- For `ORD-1005` (`2026-09-03`), the order date falls on her promotion date, joining to the active Gold record with **Gold multiplier ($2\times$)**:
  $$\text{points} = \lfloor 899 / 100 \rfloor \times 2 = 8 \times 2 = 16 \text{ points}$$

---

### B3. Validation Rule Addition & Quarantine Verification (6 Marks)

To prevent unsupported foreign currencies from entering Silver/Gold layers, an additional validation expression was injected into `ARRAY_CONSTRUCT_COMPACT(...)` inside `SQL_10_ORDERS_CLEAN`:

```sql
IFF(currency NOT IN ('INR', 'USD'), 'unsupported_currency', NULL)
```

#### Diagnostic Evaluation for `ORD-1006`:
- `ORD-1006` arrives with currency `EUR`, an invalid email (`vikram@@mail`), and an invalid pincode (`99999`).
- **Reasons Array Before Modification:** `['bad_email', 'bad_pincode']`
- **Reasons Array After Modification:** `['bad_email', 'bad_pincode', 'unsupported_currency']`

![B3 Quarantine Evidence](screenshots/B3.png)

#### Analytical Questions:
1. *Does the Gold table total change?*  
   **No.** `ORD-1006` was already quarantined in the baseline run due to its failing email and pincode. Appending an additional failure reason keeps it in Quarantine and excludes it from `silver.orders_clean`, leaving Gold totals invariant.
2. *Does the re-run create a duplicate quarantine row for `ORD-1006`?*  
   **No.** Duplicate quarantine insertions are prevented by the `NOT EXISTS` anti-join clause:
   ```sql
   AND NOT EXISTS (
     SELECT 1 FROM quarantine.orders q
     WHERE q.source = c.source
       AND EQUAL_NULL(q.order_id, c.order_id)
       AND EQUAL_NULL(q.row_data:updated_at::TIMESTAMP_NTZ, c.updated_at)
   )
   ```
   Because an entry for `ORD-1006` with identical `source`, `order_id`, and `updated_at` already exists in `quarantine.orders`, the query skips re-insertion.

---

### B4. CRM Change History & Lakehouse Idempotency (7 Marks)

An additional CRM event was simulated: Arjun (`CUST-02`, `arjun.k@yahoo.in`) was promoted to **Gold** with `effective_from = '2026-08-15'`.

#### Pre-Run Predictions vs Actual Execution Results:

| Metric / Quantity | Predicted Value | Actual After Run | Variance |
| :--- | :--- | :--- | :--- |
| Rows for `CUST-02` in `dim.customers_scd2` | **2** | **2** | Exact match |
| Tier used for `ORD-1002` (`2026-09-01`) | **Gold** | **Gold** | Exact match |
| Loyalty points for `ORD-1002` | **70** | **70** | Exact match ($\lfloor 3536 / 100 \rfloor \times 2$) |
| Total loyalty points in Gold | **161** | **161** | Exact match ($126 - 35 + 70$) |
| Gold rows with altered `published_at` | **1** | **1** | Only `ORD-1002` updated |

![Edited Gold Totals](screenshots/B-edited_gold.png)

#### Technical Explanation of Idempotency & Bronze Ingestion:
Snowflake's `MERGE INTO gold.fct_orders` achieves deterministic idempotency by computing a cryptographic hash (`row_hash = HASH(...)`) across all fact and dimension columns. When a pipeline executes, the `WHEN MATCHED AND t.row_hash <> s.row_hash` clause updates existing rows only if an underlying business value has changed, ensuring unaffected records preserve their original `published_at` timestamps. If no source data or dimensions have changed, zero updates occur, guaranteeing that repeated runs leave Gold tables untouched. 

In contrast, Bronze is engineered as an immutable, append-only staging archive. Every execution of `python run_pipeline.py` extracts API payloads and executes `COPY INTO bronze.orders_raw`, assigning auto-incrementing `ingest_seq` keys and appending 8 raw records per batch. This architecture decouples raw data ingestion history from idempotent downstream state curation.

---

### B5. Data Quality Gate & Architectural Trade-offs (6 Marks)

A custom quality assertion was added to `quality_checks()` in `run_pipeline.py`:
```python
("gold orders with negative loyalty points",
 "SELECT COUNT_IF(loyalty_points < 0) FROM gold.fct_orders",
 lambda v: v == 0, "fail")
```

#### Failure Probe Execution:
An anomalous mock order with negative loyalty points was introduced. The assertion halted the deployment and emitted a critical error log:  
`PROBLEM: gold orders with negative loyalty points = 1 (expected 0)`

![B5 Forced Failure](screenshots/B5-forced_fail.png)

Following verification, the mock order was removed, restoring pipeline pass status.

#### ELT vs. ETL Architectural Trade-Off:
For FreshCart, **ELT** is vastly superior to traditional ETL because incoming e-commerce data arrives in disparate, rapidly shifting JSON schemas (e.g. flat web exports vs nested mobile structures). Loading raw data directly into Snowflake's scalable object storage (Bronze) eliminates ingestion bottlenecks and guarantees that schema modifications or new business logic (such as SCD-2 temporal adjustments) can be recalculated retroactively without requiring API re-extractions. 

However, **ETL** remains necessary in scenarios governed by strict privacy or legal compliance standards (such as PCI-DSS payment processing or HIPAA/GDPR health and identification data), where sensitive personally identifiable information (PII) or plaintext credit cards must be scrubbed, masked, or encrypted in-flight *prior* to landing on persistent cloud disks.

---

## Part C: ML Pipeline Orchestration with Mage AI (20 Marks)

### C1. Pipeline DAG Architecture & Execution (4 Marks)

The machine learning pipeline predicts Iris species using orchestrated modular blocks within Mage AI:
- `load_iris` (`data_loader`): Loads Fisher's Iris dataset (150 rows). Test: verifies shape and column schema.
- `validate_iris` (`transformer`): Custom schema and nulls quality gate. Test: asserts zero missing values and 3 balanced classes.
- `prepare_data` (`transformer`): Stratified 80/20 train/test splitter. Test: asserts 120 train and 30 test rows.
- `train_model` (`transformer`): Trains a `DecisionTreeClassifier`. Tests: validates prediction column and enforces validation accuracy $\ge 0.80$.
- Downstream Branches:
  - `save_results` (`data_exporter`): Exports `iris_predictions.csv`.
  - `evaluate_model` (`transformer`): Evaluates classification metrics and confusion matrix.

#### Mage Pipeline Graph & Pass Verification:
- **Pipeline DAG Topology:**  
  ![C-Graph](screenshots/C-graph.png)
- **Data Load & Initial Execution Pass:**  
  ![C1 Pass](screenshots/C1-pass.png)

---

### C2. Quality Gates & Runtime Hyperparameter Tuning (6 Marks)

The pipeline was executed across hyperparameter values `max_depth` $\in \{1, 2, 3, 4, 5\}$ using Mage runtime variables:

| `max_depth` | Test Accuracy | `test_accuracy` Passed? | `iris_predictions.csv` Created? |
| :---: | :---: | :---: | :---: |
| **1** | 0.633 | **FAILED** (0.633 < 0.80) | **NO** (Blocked by test gate) |
| **2** | 0.967 | **PASSED** (0.967 $\ge$ 0.80) | **YES** (Successfully exported) |
| **3** | 1.000 | **PASSED** (1.000 $\ge$ 0.80) | **YES** (Successfully exported) |
| **4** | 1.000 | **PASSED** (1.000 $\ge$ 0.80) | **YES** (Successfully exported) |
| **5** | 1.000 | **PASSED** (1.000 $\ge$ 0.80) | **YES** (Successfully exported) |

![C2 Test Runs](screenshots/C2.png)

#### Importance of Pre-Export Validation Gates:
When `@test` assertions fail inside `train_model` (e.g. at `max_depth=1`), Mage immediately aborts downstream DAG execution. This stops `save_results` from executing, ensuring no corrupted or degraded prediction artifacts are written to storage. This fail-fast paradigm is vastly superior to post-hoc validation, as it prevents downstream microservices, web apps, and automated decision engines from ingesting substandard model outputs.

---

### C3. Evaluation Branch Integration (5 Marks)

An evaluation branch (`evaluate_model.py`) was wired into `metadata.yaml` running parallel to `save_results`:

```yaml
- name: train_model
  uuid: train_model
  type: transformer
  upstream_blocks:
  - prepare_data
  downstream_blocks:
  - save_results
  - evaluate_model
```

#### Diagnostic Evaluation Output:
- **Confusion Matrix:**
  $$\begin{bmatrix} 10 & 0 & 0 \\ 0 & 9 & 0 \\ 0 & 0 & 11 \end{bmatrix}$$
- **Metrics Table:**
  - Accuracy: **1.0000**
  - Macro Precision: **1.0000**
  - Macro Recall: **1.0000**
  - Macro F1: **1.0000**

- **Mage DAG Evaluation Branch View:**  
  ![C3a Evaluation Branch](screenshots/C3a.png)
- **Model Evaluation Logs:**  
  ![C5 Evaluation Metrics](screenshots/C5.png)

---

### C4. Custom Data-Quality Transformer: `validate_iris.py` (5 Marks)

A custom validation block was inserted immediately after data loading. It asserts that:
1. Feature columns (`sepal length`, `sepal width`, `petal length`, `petal width`) are present, non-null, and numeric.
2. The `target` column exists and contains exactly 3 distinct species classes.
3. The total record count equals exactly 150.

```python
@test
def test_output(output: pd.DataFrame, *args, **kwargs) -> None:
    assert output is not None, "Output is undefined"
    assert len(output) == 150, "Output must contain exactly 150 rows"
    assert output['target'].nunique() == 3, "Dataset must feature 3 classes"
    assert not output.isnull().any().any(), "Dataset contains nulls"
```

![C4 Validate Iris Block](screenshots/C4.png)

---

## Part D: Real-Time Event Streaming with RabbitMQ (20 Marks)

### D1. AMQP Topic Exchange Routing Matrix (6 Marks)

The `rapido-dispatch` topic exchange distributes events with hierarchical keys: `<city>.<zone>.<service_type>`.

#### Registered Driver Binding Patterns:
- **Ravi**: `bangalore.koramangala.*`
- **Priya**: `bangalore.indiranagar.*`
- **Arjun**: `bangalore.*.ride`
- **Meena**: `bangalore.*.food`
- **Karthik**: `chennai.tnagar.food`
- **Control**: `bangalore.#`

#### Theoretical Evaluation Matrix:

| # | Routing Key | Receiving Consumer Queues | Disposition |
| :---: | :--- | :--- | :--- |
| **1** | `bangalore.koramangala.food` | **`['Ravi', 'Meena', 'Control']`** | Routed to 3 queues |
| **2** | `bangalore.indiranagar.ride` | **`['Priya', 'Arjun', 'Control']`** | Routed to 3 queues |
| **3** | `bangalore.whitefield.ride` | **`['Arjun', 'Control']`** | Routed to 2 queues |
| **4** | `chennai.tnagar.food` | **`['Karthik']`** | Routed to 1 queue |
| **5** | `chennai.whitefield.ride` | **`[]`** | **Dropped** (No matching pattern) |
| **6** | `bangalore.koramangala` | **`['Control']`** | Routed to 1 queue |

![Routing Table](screenshots/D-routing_table.png)

#### In-Depth Analysis of Key 6 (`bangalore.koramangala`):
Key 6 contains exactly **two words**.
- Patterns using `*` (such as Ravi's `bangalore.koramangala.*`, Meena's `bangalore.*.food`, and Priya's `bangalore.indiranagar.*`) strictly demand **exactly three words**, because each `*` token represents exactly one word. Consequently, none of these bindings match.
- Only **Control** (`bangalore.#`) matches, because the `#` wildcard matches **zero or more words**. Here, `#` matches `koramangala` while satisfying the prefix `bangalore.`.

---

### D2. Dispatcher and Driver Implementation (8 Marks)

- **`stream/dispatcher.py`**: Connects to the topic exchange, dynamically assembling order payloads (`id`, `city`, `zone`, `type`, `ts`) every 2 seconds. Every 10th message is intentionally published without a `zone` attribute.
- **`stream/driver.py`**: Provisions transient exclusive queues bound to designated topic patterns and evaluates payloads with manual acknowledgements (`auto_ack=False`).
- **`stream/simulation.py`**: Replicates AMQP routing semantics locally, verifying that delivery matches theoretical predictions.

#### Simulation & Execution Evidence:
- **Simulation Console Output:**  
  ![D2 Simulation](screenshots/D2-simulation.png)
- **Dispatcher Publishing Stream:**  
  ![Dispatcher Console](screenshots/D-dispatcher_output.png)
- **Driver Consumer Stream:**  
  ![Driver Console](screenshots/D-driver_output.png)

---

### D3. Reliability Protocol: Message ACK and NACK (6 Marks)

The driver consumer inspects message structure before confirmation:
- **Valid Messages**: Handled and acknowledged via `channel.basic_ack(delivery_tag)`.
- **Malformed Messages (Missing `zone` or Invalid JSON)**: Rejected without requeuing via `channel.basic_nack(delivery_tag, requeue=False)`.

```python
if "zone" not in event or not event["zone"]:
    print(f"[{driver_name}] [NACK - REJECTED] Missing zone! Dropping without requeue.")
    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
else:
    print(f"[{driver_name}] [ACK - ACCEPTED] Order {event['id']} processed.")
    ch.basic_ack(delivery_tag=method.delivery_tag)
```

![ACK / NACK Verification](screenshots/D3-ack-nack.png)
![Test Stream](screenshots/D-tests.png)

#### Reliability Analysis Questions:
1. *What happens to a message if the driver crashes before sending an ACK?*  
   RabbitMQ detects channel/socket closure. Because the message was not acknowledged, the broker automatically re-queues it at the head of the queue, allowing an available standby worker to process it. This guarantees **at-least-once** delivery.
2. *What would happen with `requeue=True` for a message that is permanently malformed?*  
   Setting `requeue=True` on a poison message triggers an **infinite crash loop**. The consumer rejects the invalid message, the broker immediately re-delivers it, and the consumer rejects it again, saturating network bandwidth and CPU cycles. Setting `requeue=False` ensures corrupt payloads are permanently dropped or diverted to a Dead Letter Exchange (DLX).

---

## Part E: End-to-End System Integration & Reflection (15 Marks)

### E1. System Architecture Integration Diagram

The entire platform forms an integrated data ecosystem spanning source control, data processing, machine learning, and event-driven messaging:

```mermaid
graph TD
    subgraph VersionControl [1. Source Control & Automation]
        Git[Local Git Repo] -->|git push| GH[GitHub Remote]
        GH -->|Trigger| GHA[GitHub Actions CI]
        GHA -->|Pytest Validation| CIStatus[Green Build]
    end

    subgraph DataWarehouse [2. Snowflake Medallion ELT]
        WebAPI[Web Orders API] -->|Extract| Bronze[bronze.orders_raw]
        MobileAPI[Mobile Orders API] -->|Extract| Bronze
        Bronze -->|Cleanse & Validate| SilverClean[silver.orders_clean]
        Bronze -->|Failures| Quarantine[quarantine.orders]
        SilverClean -->|Deduplicate| SilverDedup[silver.orders_dedup]
        SilverDedup -->|SCD-2 Join| DimCust[dim.customers_scd2]
        SilverDedup -->|FX Join| RefFX[ref.fx_daily]
        DimCust --> GoldFct[gold.fct_orders]
        RefFX --> GoldFct
    end

    subgraph MLOps [3. Mage ML Pipeline]
        GoldFct -.->|Analytical Features| MLData[load_iris]
        MLData --> ValIris[validate_iris]
        ValIris --> PrepData[prepare_data]
        PrepData --> TrainModel[train_model]
        TrainModel -->|Quality Gate Pass| SaveRes[save_results]
        TrainModel -->|Quality Gate Pass| EvalModel[evaluate_model]
    end

    subgraph Streaming [4. Real-Time RabbitMQ Stream]
        GoldFct -.->|Fulfillment Events| Dispatcher[dispatcher.py]
        Dispatcher -->|Topic Publish| AMQP[rapido-dispatch Exchange]
        AMQP -->|bangalore.koramangala.*| RaviQ[Driver: Ravi]
        AMQP -->|bangalore.indiranagar.*| PriyaQ[Driver: Priya]
        AMQP -->|bangalore.#| ControlQ[Driver: Control]
        RaviQ -->|basic_ack / basic_nack| DriverLogic[ACK / NACK Evaluation]
    end
```

---

### E2. Engineering Reflection: Debugging & Resolution

During implementation of the event streaming suite (Part D), an intermittent test failure occurred in `tests/test_stream.py`. When validating dispatcher publishing mocks via `unittest.mock.patch`, test cases threw:
`TypeError: tuple indices must be integers or slices, not str`

#### Root Cause Analysis:
In the earlier test scaffolding, the mock channel interception callback recorded publish calls as `(routing_key, message_body)` tuples. However, downstream test assertion logic attempted dictionary lookups directly on the tuple object (e.g. `call_args["routing_key"]`) rather than unpacking the tuple elements `(rk, body)`.

#### Engineering Resolution:
The callback was updated to explicitly unpack `(routing_key, json.loads(body))` into dedicated tuple variables. Assertions were revised to test `rk` and `body` independently:
```python
# Resolved unpacking logic
rk_10, msg_10 = published[9]
self.assertNotIn("zone", msg_10)
self.assertTrue(msg_10.get("malformed", False))
self.assertEqual(len(rk_10.split(".")), 2)
```
This fix ensured 100% deterministic test execution across local test environments and GitHub Actions CI pipelines.

---

## Bonus Challenge: Stream-to-Lakehouse Batch Ingestion (+10 Marks)

The bonus module (`bonus/stream_to_lakehouse.py`) bridges real-time streaming (Part D) with lakehouse storage (Part B). It models a scenario where streaming order payloads land in an append-only JSONL landing directory, followed by an idempotent batch curation job.

### Pipeline Workflow:
1. **Stream Buffering**: Simulates 60 order events representing a 1-minute streaming window.
2. **Landing Persistence**: Writes events sequentially to `bonus/raw_landing.jsonl`.
3. **Batch Curation Job**:
   - Reads raw JSONL records.
   - Enforces deduplication using `order_id` as primary key, preserving the latest state.
   - Idempotently curates records into lakehouse storage.
4. **Idempotency Proof**: Executes the curation job twice over the identical landing file, verifying that record counts and monetary totals remain invariant.

```
=================================================================
  FreshCart Stream-to-Lakehouse Pipeline Idempotency Verification  
=================================================================
[*] Simulating 60 real-time order stream events...
[+] Successfully generated 60 order events.
[*] Persisting raw event stream to landing path: .../raw_landing.jsonl
[+] Raw landing persistence complete (60 events written).

--- BATCH INGESTION: RUN 1 ---
    - Extracted 60 records from landing zone.
    - Successfully curated and stored 60 unique orders to curated layer.
Run 1 Metrics -> Total Rows: 60 | Total Order Value: INR 21,125.00

--- BATCH INGESTION: RUN 2 (Idempotency Retest) ---
    - Extracted 60 records from landing zone.
    - Successfully curated and stored 60 unique orders to curated layer.
Run 2 Metrics -> Total Rows: 60 | Total Order Value: INR 21,125.00

--- IDEMPOTENCY AUDIT VALIDATION ---
[PASSED] Verification confirmed:
         1. Row count remained exactly identical (60 unique orders).
         2. Total revenue remained exactly identical (INR 21,125.00).
         3. Re-running the pipeline over landing data is strictly idempotent.
=================================================================
```

![Bonus Execution](screenshots/Bonus.png)

---

## Conclusion

The FreshCart Data Engineering Platform demonstrates an enterprise-grade data pipeline:
- **Git & CI/CD** enforces robust versioning and test gates prior to deployment.
- **Snowflake ELT** ensures scalable multi-layer data cleaning, SCD-2 dimension tracking, and idempotent Gold fact publishing.
- **Mage AI MLOps** safeguards model training pipelines with pre-export quality gates and automated diagnostics.
- **RabbitMQ Streaming** guarantees reliable topic-based event routing with ACK/NACK fault tolerance.
- **Lakehouse Curation** confirms that batch ingestion over streaming landing zones remains strictly idempotent.
