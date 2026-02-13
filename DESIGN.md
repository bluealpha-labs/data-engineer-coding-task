# Design document

This document covers schema design, data quality strategy, trade-offs, and architecture decisions for the BlueAlpha data pipeline.

---

## 1. Schema design decisions and rationale

**Warehouse shape:** One dimension table and two fact tables, normalized so campaigns and dates can be joined consistently.

- **dim_campaign** – One row per (campaign_id, platform). Holds campaign_name, campaign_type, status. Rationale: campaigns are shared across Google and Facebook; keeping platform in the key allows the same campaign name in different platforms. Google has campaign_type (e.g. DISPLAY, SEARCH) and status (ENABLED); Facebook export does not, so those columns are nullable for Facebook rows.

- **fact_ad_performance** – One row per (date, campaign_id, platform) with metrics: impressions, clicks, spend, conversions, conversion_value, reach, frequency. Rationale: daily grain matches the source data and supports time-series and join to orders by date. Spend is in dollars (Google cost_micros converted). Reach/frequency are Facebook-only; null for Google.

- **fact_orders** – One row per order_id with order_date, revenue, channel_attributed, campaign_source, product_category, region. Rationale: order grain with attribution fields kept raw (channel_attributed, campaign_source) so the schema supports multiple attribution approaches later (e.g. last-click, multi-touch). Revenue is nullable so orders with missing or invalid revenue can still be loaded and flagged in validation.

**Attribution:** The brief asked for flexibility. We store channel_attributed and campaign_source as provided (normalized to lowercase for consistency) and keep order_date so downstream can apply different attribution rules without schema changes.

---

## 2. Data quality strategy

**Principle:** Do not drop rows. Detect issues, record them, and optionally normalize where safe.

- **Validation report** – Every issue (nulls, duplicates, bad dates, negative revenue, outliers, channel/campaign_source issues) is written to `output/validation_report.csv` with source, row id, column, issue type, message, and value. All rows continue through the pipeline.

- **Normalizations applied in validation:** Google cost_micros → spend (dollars); dates normalized to ISO where parseable; CRM channel_attributed normalized to lowercase. Unparseable dates are kept and flagged.

- **At load:** Duplicate order_id in CRM are deduplicated (keep first) for the warehouse PK; duplicates remain in the validation report. No other rows are dropped. Outliers are not capped or removed in the warehouse; they are only recorded in the report.

---

## 3. Trade-offs made given time constraints

- **Full refresh only** – Warehouse tables are dropped and recreated each run. Chosen for simplicity and idempotency. Trade-off: no incremental loads; not ideal for very large or streaming data.

- **Single load step** – One “load_warehouse” step builds dim_campaign, fact_ad_performance, and fact_orders. Trade-off: no separate BUILD_ATTRIBUTION or BUILD_MART layers; the schema is kept flexible so those can be added later (e.g. fact_order_attribution, unified_reporting).

- **SQLite by default** – No Docker required; reviewers run with SQLite. Trade-off: single-file DB and limited concurrency; acceptable for a take-home and for local runs. Postgres supported via WAREHOUSE_URL when needed.

- **Airflow staging via pickle** – Data between Airflow tasks is passed as pickle files under output/airflow_staging. Trade-off: simple and works for this data size; for larger data, a shared store (e.g. S3/GCS + Parquet) would be better.

---

## 4. What I’d do differently with more time

- **Incremental / CDC** – Add optional incremental loading (e.g. by last_updated or log-based CDC) for large fact tables while keeping full refresh as an option.

- **Explicit attribution and mart layers** – Add BUILD_ATTRIBUTION (e.g. fact_order_attribution) and BUILD_MART (e.g. unified_reporting) in the DAG and document join rules and attribution logic.

- **Config-driven validation** – Move validation rules (thresholds, column checks) into config (YAML/JSON) so non-engineers can adjust without code changes.

- **Tests** – Unit tests for loaders and validators; integration test that runs the pipeline on a small fixture and asserts row counts and report shape.

- **Outlier handling policy** – Document and optionally implement a warehouse rule for outliers (e.g. cap at p99 or exclude above a threshold) while keeping raw values in the validation report.

---

## 5. Architecture decision records

### ADR 1: CRM data handling (invalid / duplicate rows)

**Context:** CRM can have missing revenue, negative revenue, duplicate order_id, missing customer_id.

**Options considered:**
- (A) Drop invalid rows.
- (B) Keep first per order_id and drop the rest; flag in report.
- (C) Keep all rows; record every issue in a validation report; no drops.

**Chosen approach:** (C) – Keep all rows; record all issues in validation_report.csv; at warehouse load, deduplicate on order_id (keep first) only to satisfy the PK.

**Why:** Stakeholder (Stefan) asked not to drop invalid records and to surface issues via validation reporting. Downstream or reporting can then filter or flag as needed.

**Trade-offs:** Warehouse may contain rows with null/negative revenue or duplicate order_id (only one kept). Duplicates and invalid values are always visible in the report.

---

### ADR 2: Outlier treatment

**Context:** Extreme values (e.g. revenue spikes) can skew analytics.

**Options considered:**
- (A) Drop outlier rows.
- (B) Cap at a percentile (e.g. p99) in the warehouse.
- (C) Record outliers in validation only; leave values unchanged in the warehouse.
- (D) Record in validation and apply a documented rule in the warehouse (cap/exclude).

**Chosen approach:** (C) for the take-home: outliers are detected and written to the validation report (e.g. revenue > 10× p99 or > 1e6); warehouse loads all values unchanged.

**Why:** Stakeholder said to record outliers at ingestion/validation and let the unified layer make “reasonable” choices; documenting the choice is enough. With more time we’d adopt (D) and document the exact rule (e.g. “cap revenue at p99 in fact_orders”).

**Trade-offs:** Analytics on the warehouse can still be skewed by extreme values; consumers can filter using the validation report if needed.

---

### ADR 3: Orchestration tool choice

**Context:** Need a DAG with dependencies, retries, and idempotency.

**Options considered:**
- (A) Script-only (Python) with explicit sequence and retry logic.
- (B) Airflow (or similar) as the primary orchestrator.

**Chosen approach:** (B) – Airflow only. The pipeline runs as an Airflow DAG (`orchestration/airflow_dag.py`): **ingest → validate → load_warehouse**. Tasks pass data via staged pickle files under `output/airflow_staging/<run_id>/`. For ingest + validate only (no warehouse), a standalone script `run_ingestion.py` is available.

**Why:** Airflow provides a single, production-like entry point with retries, scheduling, and observability. SQLite is the default warehouse so reviewers can run without Docker; Postgres is supported via `WAREHOUSE_URL`.

**Trade-offs:** Running the full pipeline requires an Airflow environment (e.g. `airflow standalone`). Logic is shared (ingestion, validation, transformation), so behaviour is consistent.
