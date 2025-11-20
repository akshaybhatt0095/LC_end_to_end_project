#!/usr/bin/env bash
set -euo pipefail
echo '1) Running dbt seed to load raw CSVs into duckdb...'
cd dbt
dbt seed --profiles-dir . || true
dbt run --profiles-dir .
dbt test --profiles-dir .
