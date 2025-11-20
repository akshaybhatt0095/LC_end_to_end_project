📌 Project Overview

This project implements a fully containerized local data pipeline that:
	1.	Ingests raw CSV files into DuckDB using Dagster assets
	2.	Transforms data using dbt (staging, intermediate, marts layers)
	3.	Runs data quality tests at multiple layers
	4.	Materializes final reporting tables
	5.	Outputs final CSV artifact locally
	6.	Runs end-to-end using Docker Compose, no local dependencies required.

The pipeline has three major components:
	•	Ingestion — Dagster assets (raw_accounts, raw_customers)
	•	Transformation — dbt models (staging, intermediate, marts)
	•	Orchestration — A Dagster job that runs all assets sequentially


------------------------------------------------------------------------------------------------------------------------------------------------------------------

🚀 Running the Pipeline 

Prerequisites
	•	Docker Desktop installed (Mac, Windows, Linux)
	•	No local Python/dbt/Dagster needed


1. Build and Start the Pipeline

docker compose down -v
docker compose up --build

Dagster UI is exposed at: http://localhost:3000


2. Run the Full Pipeline

In Dagster UI:
Jobs → pipeline_job → Launch Run


Note - All the pipeline run images and docker run images are stored in the images directory.

------------------------------------------------------------------------------------------------------------------------------------------------------------------


🧩 Pipeline Components

🔹 1. Ingestion (Dagster)

raw_accounts

Reads data/accounts.csv into DuckDB → raw.accounts

raw_customers

Reads data/customers.csv into DuckDB → raw.customers


🔹 2. dbt Transformations

- Staging Layer
	•	Type casting
	•	Normalization (trim, lowercase)
	•	Schema tests

   Key tests:
	•	not_null on PKs and required fields
	•	Relationships: stg_accounts.customer_id → stg_customers.customer_id
	•	Accepted values: account_type ∈ { checking, savings }

- Intermediate Layer

int_accounts_joined
	•	Joins accounts with customers

- Marts Layer

account_summary
	•	Final reporting table
	•	Materialized as table


📤 Output Files

When the pipeline completes we generate: output/account_summary.csv


------------------------------------------------------------------------------------------------------------------------------------------------------------------


-  Assumptions Made

Data Assumptions
	•	All AccountID and CustomerID represent unique identifiers.
	•	Balance may be null in raw files → replaced with 0 during staging.(Normalized)
	•	AccountType contains messy values → normalized to lowercase + trimmed.
	•	has_loan may be missing or None → accepted values include null.

Modeling Assumptions
	•	customer_id is always the join key.
	•	Staging layer should only clean and cast fields (no business logic).
	•	Intermediate layer handles enrichment.
	•	Marts layer produces final aggregates/tables.

Test Assumptions
	•	Null balances are treated as valid raw data but must be resolved in staging.
	•	All accounts must refer to a valid customer.
	•	Accepted account types limited to:
	•	checking
	•	savings


------------------------------------------------------------------------------------------------------------------------------------------------------------------


⚙️ Design Decisions & Trade-offs

1. DuckDB Chosen for Local Warehousing
	•	Lightweight, file-based, ideal for local running
	•	Avoids setup complexities of Postgres/Snowflake
	•	Limitation: no concurrent writes → forced sequential execution

2. dbt for Transformations
	•	Best practice ELT modeling
	•	Easy testing + documentation

3. Dagster as Orchestrator
	•	Asset-based workflow fits ingestion → dbt → output
	•	Clear lineage
	•	Easy Docker deployment

4. YAML Test Placement Separated by Layer
	•	staging.yml for stg_* models
	•	intermediate.yml for int_*
	•	marts.yml for final mart tables

Prevents duplicate definitions and manifest errors.


------------------------------------------------------------------------------------------------------------------------------------------------------------------

🚧 What I Would Improve Next

1. Switch to Postgres or other cloud based data warehouses.
	•	Fix concurrency problems
	•	Allow parallel asset execution

2. Add surrogate keys (Customerid and account_id)

3. Add more derived metrics in final datamart like total_interest_accrued, avg_balance, etc.

4. Add schedules to the pipeline
   @schedule(cron_schedule="0 * * * *", job=pipeline_job)
    def hourly_run(_):
    return {}

5. Add schema validation before loading

6. Add more logging and try catch methods

7. Save multiple output types - Eg: Parquet


------------------------------------------------------------------------------------------------------------------------------------------------------------------

📂 Project Structure

lendingclub-pipeline/
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_accounts.sql
│   │   │   ├── stg_customers.sql
│   │   │   └── staging.yml
│   │   ├── intermediate/
│   │   │   ├── int_accounts_joined.sql
│   │   │   └── intermediate.yml
│   │   └── marts/
│   │       ├── account_summary.sql
│   │       └── marts.yml
│   ├── profiles.yml
│   └── dbt_project.yml
│
├── pipeline/
│   ├── ingest_assets.py
│   ├── dbt_assets.py
│   ├── output_assets.py
│   └── __init__.py
│
├── data/
│   ├── accounts.csv
│   └── customers.csv
│
├── output/
│   └── account_summary.csv
│
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md

