**Airflow-DBT based Data Engineering Project**
# 🏪 Enterprise Retail Data Warehouse (Walmart Dataset)

A scalable, production-grade data pipeline built using the **Medallion Lakehouse Architecture**. This project orchestrates raw retail data (stores, employees, transactions) through raw ingestion, cleaning, historization, and analytics-ready aggregation using **Databricks**, **dbt Core**, and **Apache Airflow**.

---

## ✨ Key Features

* **🥇 Medallion Lakehouse Architecture:** Multi-layer pipeline design transitioning data from raw (Bronze), cleaned/normalized (Silver), to business aggregates (Gold).
* **🛠 Metadata-Driven SQL Handling:** Dynamic dynamic SQL generation leveraging dbt Jinja macros. Schema updates (such as adding new table attributes) are handled via centralized configuration YAML files—eliminating redundant raw SQL code.
* **⚡ Incremental Processing:** Materialized dbt models configured with `materialized='incremental'` and timestamp watermarking to ensure nightly batch runs only compute net-new or modified records, keeping Databricks compute costs minimal.
* **📸 SCD Type 2 Historization:** Utilizes `dbt snapshots` to track slowly changing dimensions over time (e.g., store manager reassignments, product pricing updates) with automated `dbt_valid_from` and `dbt_valid_to` tracking.
* **🛡 Data Integrity Quality Gates:** Enforces automated `dbt tests` (`unique`, `not_null`, `relationships`) at the Silver layer. If data anomalies occur, downstream DAG execution halts automatically to prevent invalid metrics from entering Gold tables.
* **🔄 End-to-End Orchestration:** Managed via Apache Airflow to handle task scheduling, dependency management, and failure alerts across all pipeline stages.

---

## ⚙️ Tech Stack

* **Compute & Storage Engine:** Databricks, PySpark, Delta Lake
* **Transformation Engine:** dbt Core (dbt-databricks adapter)
* **Orchestration:** Apache Airflow
* **Dataset:** Walmart Retail Dataset (Store, Employee, Product, and Transaction entities)

---

## 🚀 Apache Airflow Pipeline Stages

| Task ID | Operator / Tool | Description |
| :--- | :--- | :--- |
| `01_ingest_raw_walmart_data` | Databricks Auto Loader | Ingests landing CSV/JSON files into raw Delta Lake Bronze tables. |
| `02_dbt_run_silver` | dbt Core | Applies schema constraints, standardizes types, and cleans data. |
| `03_dbt_test_silver` | dbt Quality Check | Validates unique keys and referential integrity; fails on corruption. |
| `04_dbt_snapshot` | dbt Snapshot | Executes SCD Type 2 tracking on store and employee dimensions. |
| `05_dbt_run_gold` | dbt Core | Aggregates facts into star-schema tables for BI reporting. |

---
# 🛠️ Getting Started

### Prerequisites

1. Access to a **Databricks Workspace** with an active cluster or SQL Warehouse.
2. **Apache Airflow 2.x+** installed locally or hosted via MWAA/Astronomer.
3. Python 3.9+ installed locally.

