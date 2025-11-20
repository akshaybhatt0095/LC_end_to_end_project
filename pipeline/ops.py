import os
import shutil
from dagster import op
import subprocess

RAW_PATH = "data/raw"
OUTPUT_PATH = "data/outputs"

@op
def download_from_s3():
    """Mock S3 download by copying sample CSVs into data/raw (already present)."""
    os.makedirs(RAW_PATH, exist_ok=True)
    shutil.copyfile('data/raw/customers.csv', f'{RAW_PATH}/customers.csv')
    shutil.copyfile('data/raw/accounts.csv', f'{RAW_PATH}/accounts.csv')
    return {'raw_path': RAW_PATH}

@op
def dbt_run_op(_context):
    """Run dbt (expects dbt available in environment)"""
    res = subprocess.run(['bash', '-lc', 'cd dbt && dbt deps >/dev/null 2>&1 || true && dbt run'], shell=False)
    if res.returncode != 0:
        raise Exception('dbt run failed')
    return {'status': 'dbt_run_completed'}

@op
def dbt_test_op(_context):
    res = subprocess.run(['bash', '-lc', 'cd dbt && dbt test'], shell=False)
    if res.returncode != 0:
        raise Exception('dbt tests failed')
    return {'status': 'dbt_tests_completed'}

@op
def export_summary(_context):
    src = 'dbt/target/run/marts/account_summary/account_summary.csv'
    dst_dir = OUTPUT_PATH
    os.makedirs(dst_dir, exist_ok=True)
    if not os.path.exists(src):
        alt = 'dbt/target/run/marts/account_summary.csv'
        if os.path.exists(alt):
            src = alt
    if not os.path.exists(src):
        raise FileNotFoundError(f"Could not find dbt output at {src}")
    dst = os.path.join(dst_dir, 'account_summary.csv')
    shutil.copyfile(src, dst)
    return {'output_path': dst}
