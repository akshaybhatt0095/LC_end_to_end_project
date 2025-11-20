# import warnings
# from dagster import ExperimentalWarning
# warnings.filterwarnings("ignore", category=ExperimentalWarning)
from dagster import Definitions, define_asset_job, AssetSelection
from dagster import in_process_executor
from dagster_dbt import DbtCliResource
from .dbt_assets import dbt_models
from .ingest_assets import raw_accounts, raw_customers
from .output_assets import account_summary_csv

pipeline_job = define_asset_job(
    "pipeline_job",
    selection=AssetSelection.all(),
    executor_def=in_process_executor 
)

defs = Definitions(
    assets=[
        raw_accounts,
        raw_customers,
        dbt_models,
        account_summary_csv,
    ],
    jobs=[pipeline_job],
    resources={
        "dbt": DbtCliResource(
            project_dir="/app/dbt",
            profiles_dir="/app/dbt"
        )
    }
)