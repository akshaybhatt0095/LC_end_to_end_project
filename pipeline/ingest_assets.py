import duckdb
import pandas as pd
from dagster import asset

RAW_PATH = "/app/data/raw"
DUCKDB_PATH = "/app/dbt/warehouse.duckdb"


@asset(group_name="raw")
def raw_accounts(context):
    path = f"{RAW_PATH}/accounts.csv"
    context.log.info(f"Loading {path}")

    df = pd.read_csv(path)

    con = duckdb.connect(DUCKDB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("DROP TABLE IF EXISTS raw.accounts")

    con.register("df", df)
    con.execute("CREATE TABLE raw.accounts AS SELECT * FROM df")
    con.close()

    context.log.info(f"Ingested {df.shape[0]} accounts")
    return df.shape[0]


@asset(group_name="raw")
def raw_customers(context):
    path = f"{RAW_PATH}/customers.csv"
    context.log.info(f"Loading {path}")

    df = pd.read_csv(path)

    con = duckdb.connect(DUCKDB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("DROP TABLE IF EXISTS raw.customers")

    con.register("df", df)
    con.execute("CREATE TABLE raw.customers AS SELECT * FROM df")
    con.close()

    context.log.info(f"Ingested {df.shape[0]} customers")
    return df.shape[0]