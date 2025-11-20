from dagster import Definitions, define_asset_job, AssetSelection
from dagster_dbt import DbtCliResource

from .dbt_assets import dbt_models
from .ingest_assets import raw_accounts, raw_customers
from .output_assets import account_summary_csv

# Force sequential execution (DuckDB allows only 1 writer)
sequential_job = define_asset_job(
    name="pipeline_job",
    selection=AssetSelection.all(),
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 1   
                }
            }
        }
    }
)

defs = Definitions(
    assets=[
        raw_accounts,
        raw_customers,
        dbt_models,
        account_summary_csv,
    ],
    jobs=[sequential_job],   
)