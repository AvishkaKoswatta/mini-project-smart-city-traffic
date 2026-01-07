from airflow.providers.standard.operators.python import PythonOperator
from airflow import DAG
#from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import glob

def generate_daily_report():
    files = glob.glob("/home/avishka/data/projects/mini-project-traffic/traffic-v3/data/processed/traffic_aggregates/*.parquet")

    if not files:
        print("No parquet files found")
        return

    df = pd.concat([pd.read_parquet(f) for f in files])

    df["hour"] = pd.to_datetime(df["window_start"]).dt.hour

    report = (
        df.groupby(["sensor_id", "hour"])["total_vehicles"]
          .sum()
          .reset_index()
    )

    report.to_csv(
        "/home/avishka/data/projects/mini-project-traffic/traffic-v3/data/reports/daily_peak_traffic.csv",
        index=False
    )
    print("Daily report generated")

with DAG(
    dag_id="traffic_daily_report",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,          
    tags=["traffic", "report"]
) as dag:

    report_task = PythonOperator(
        task_id="generate_daily_report",
        python_callable=generate_daily_report
    )
