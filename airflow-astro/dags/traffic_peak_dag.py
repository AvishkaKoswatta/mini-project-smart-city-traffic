# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# import sys
# import os

# # Add the batch folder to Python path
# PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
# sys.path.insert(0, PROJECT_ROOT)

# # Import the main function from batch_processing.py
# #from batch_processing import main as generate_report
# from batch.batch_processing import main as generate_report

# default_args = {
#     'owner': 'avishka',
#     'depends_on_past': False,
#     'start_date': datetime(2025, 12, 17),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# dag = DAG(
#     'daily_peak_traffic_report',
#     default_args=default_args,
#     description='Generate nightly peak traffic report',
#     schedule='0 23 * * *',  # every day at 11 PM
#     catchup=False,
# )

# run_report = PythonOperator(
#     task_id='generate_peak_report',
#     python_callable=generate_report,
#     dag=dag
# )


# from airflow import DAG
# from airflow.providers.standard.operators.python import PythonOperator
# from datetime import datetime, timedelta

# from batch.batch_processing import main as generate_report

# default_args = {
#     'owner': 'avishka',
#     'depends_on_past': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# with DAG(
#     dag_id='daily_peak_traffic_report',
#     default_args=default_args,
#     description='Generate nightly peak traffic report',
#     start_date=datetime(2025, 1, 1),
#     schedule='0 23 * * *',
#     catchup=False,
# ) as dag:

#     run_report = PythonOperator(
#         task_id='generate_peak_report',
#         python_callable=generate_report
#     )




from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import pyarrow.parquet as pq

PARQUET_PATH = "/usr/local/airflow/include/processed/traffic_aggregates"
REPORT_PATH = "/usr/local/airflow/include/reports/peak_traffic_report.csv"


def generate_peak_traffic_report():
    # 1. Validate input path
    if not os.path.isdir(PARQUET_PATH):
        raise FileNotFoundError(f"Parquet path not found: {PARQUET_PATH}")

    parquet_files = [
        f for f in os.listdir(PARQUET_PATH) if f.endswith(".parquet")
    ]
    if not parquet_files:
        raise ValueError("No parquet files found")

    # 2. Read all parquet files
    table = pq.ParquetDataset(PARQUET_PATH).read()
    df = table.to_pandas()

    # Expected columns check (safe)
    required_cols = {"sensor_id", "window_start", "total_vehicles"}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"Missing columns: {required_cols - set(df.columns)}")

    # 3. Extract hour
    df["hour"] = pd.to_datetime(df["window_start"]).dt.hour

    # 4. Aggregate per sensor per hour
    hourly = (
        df.groupby(["sensor_id", "hour"], as_index=False)["total_vehicles"]
        .sum()
    )

    # 5. Get peak hour per sensor
    peak = hourly.loc[
        hourly.groupby("sensor_id")["total_vehicles"].idxmax()
    ]

    # 6. Ensure output directory exists
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)

    # 7. Write CSV
    peak.to_csv(REPORT_PATH, index=False)

    print("✅ Peak traffic report generated")
    print(peak.head())


default_args = {
    "owner": "avishka",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_peak_traffic_report",
    default_args=default_args,
    description="Generate daily peak traffic report from parquet",
    start_date=datetime(2025, 1, 1),
    schedule="0 23 * * *",
    catchup=False,
    tags=["traffic", "batch"],
) as dag:

    generate_report = PythonOperator(
        task_id="generate_peak_traffic_report",
        python_callable=generate_peak_traffic_report,
    )
