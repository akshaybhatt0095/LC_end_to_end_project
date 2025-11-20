FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------
# WORKDIR MUST BE /app/dbt
# ---------------------------------------
WORKDIR /app

# Copy dbt folder FIRST
COPY dbt /app/dbt

# Remove leftover DuckDB directory/file
RUN rm -rf /app/dbt/warehouse.duckdb

# Copy other components
COPY data /app/data
COPY pipeline /app/pipeline
COPY requirements.txt /app/requirements.txt

# Install dependencies
RUN pip install --upgrade pip && \
    pip install -r /app/requirements.txt

EXPOSE 3000
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]