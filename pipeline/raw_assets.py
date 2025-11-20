from dagster import asset
import pandas as pd

@asset
def accounts():
    return pd.read_csv("/app/data/accounts.csv")

@asset
def customers():
    return pd.read_csv("/app/data/customers.csv")

raw_assets = [accounts, customers]