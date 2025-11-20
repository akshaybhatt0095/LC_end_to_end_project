import duckdb
import pandas as pd
from dagster import asset

@asset(
    deps=["account_summary"],   # Wait for dbt model
    compute_kind="python",
)
def account_summary_csv():
    """
    Export the final dbt model `account_summary` to a local CSV file.
    """
    # Path to DuckDB used by dbt + raw assets
    db_path = "/app/dbt/warehouse.duckdb"

    # Query the final model 
    con = duckdb.connect(db_path)
    df = con.execute("SELECT * FROM analytics_analytics.account_summary").df()

    # Output path
    out_path = "/app/data/output/account_summary.csv"

    # Ensure directory exists
    import os
    os.makedirs("/app/data/output", exist_ok=True)

    # Save file
    df.to_csv(out_path, index=False)

    return out_path