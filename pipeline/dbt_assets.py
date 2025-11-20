from dagster_dbt import dbt_assets, DbtCliResource

DBT_PROJECT_DIR = "/app/dbt"
MANIFEST_PATH = "/app/dbt/target/manifest.json"

dbt = DbtCliResource(project_dir=DBT_PROJECT_DIR)

@dbt_assets(manifest=MANIFEST_PATH)
def dbt_models(context):
    yield from dbt.cli(["build"], context=context).stream()