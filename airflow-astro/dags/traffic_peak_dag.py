from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import pyarrow.parquet as pq

PARQUET_PATH = "/usr/local/airflow/include/processed/traffic_aggregates_new"
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



# type: ignore
# from airflow import DAG
# from airflow.providers.standard.operators.python import PythonOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from datetime import datetime, timedelta
# import pandas as pd
# import os

# default_args = {
#     "owner": "airflow",
#     "retries": 2,
#     "retry_delay": timedelta(minutes=5),
# }

# CSV_OUTPUT_PATH = "/usr/local/airflow/include/reports"

# def extract_process_load():
#     pg_hook = PostgresHook(postgres_conn_id="traffic_postgres")
#     engine = pg_hook.get_sqlalchemy_engine()

#     query = """
#         SELECT
#             window_start,
#             window_end,
#             sensor_id,
#             total_vehicles,
#             avg_speed
#         FROM traffic_aggregates
#         ORDER BY sensor_id;
#     """

#     # Extract
#     df = pg_hook.get_pandas_df(query)

#     if df.empty:
#         raise ValueError("No data found for report date")

#     # Add congestion label for critical counting
#     df["congestion_level"] = df["avg_speed"].apply(
#         lambda x: "CRITICAL" if x < 10 else "MODERATE" if x < 20 else "NORMAL"
#     )

#     # Group by sensor_id and aggregate
#     summary = df.groupby("sensor_id").agg(
#         total_vehicles=pd.NamedAgg(column="total_vehicles", aggfunc="sum"),
#         avg_speed=pd.NamedAgg(column="avg_speed", aggfunc="mean"),
#         critical_count=pd.NamedAgg(column="congestion_level", aggfunc=lambda x: (x == "CRITICAL").sum())
#     ).reset_index()

#     # Add report date
#     summary["report_date"] = pd.Timestamp.utcnow().date()


#     # Ensure directory exists
#     os.makedirs(os.path.dirname(CSV_OUTPUT_PATH), exist_ok=True)

#     # Save CSV
#     output_file = f"{CSV_OUTPUT_PATH}"
#     summary.to_csv(output_file, index=False)

#     print(f"CSV report written to {output_file}")


# with DAG(
#     dag_id="nightly_traffic_csv_report",
#     default_args=default_args,
#     start_date=datetime(2025, 1, 1),
#     schedule="0 1 * * *",  # 1 AM nightly
#     catchup=False,
#     tags=["traffic", "batch", "csv"],
# ) as dag:

#     generate_csv_report = PythonOperator(
#     task_id="extract_process_load_to_csv",
#     python_callable=extract_process_load,
# )


#     generate_csv_report
