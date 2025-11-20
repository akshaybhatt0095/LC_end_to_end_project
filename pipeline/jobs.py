from dagster import job
from .ops import download_from_s3, dbt_run_op, dbt_test_op, export_summary

@job
def lendingclub_pipeline():
    downloaded = download_from_s3()
    dbt_run = dbt_run_op()
    dbt_tests = dbt_test_op()
    export_summary()