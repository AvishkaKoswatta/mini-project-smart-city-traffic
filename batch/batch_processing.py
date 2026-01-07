import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime
import os

# -------------------------------
# Paths
# -------------------------------
PARQUET_PATH = "/home/avishka/data/projects/mini-project-traffic/traffic-v1/data/processed/traffic_aggregates/"
REPORT_PATH = "/home/avishka/data/projects/mini-project-traffic/traffic-v1/data/reports/peak_traffic_report.csv"

def main():
    # Ensure report folder exists
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)

    # -------------------------------
    # Read all Parquet files
    # -------------------------------
    table = pq.ParquetDataset(PARQUET_PATH).read()
    df = table.to_pandas()

    # -------------------------------
    # Extract hour from window_start
    # -------------------------------
    df['hour'] = pd.to_datetime(df['window_start']).dt.hour

    # -------------------------------
    # Aggregate: Peak Traffic Hour per Junction
    # -------------------------------
    hourly_traffic = df.groupby(['sensor_id', 'hour'])['total_vehicles'].sum().reset_index()

    # For each sensor, find the hour with maximum vehicles
    peak_report = hourly_traffic.loc[hourly_traffic.groupby('sensor_id')['total_vehicles'].idxmax()]

    # -------------------------------
    # Save report
    # -------------------------------
    peak_report.to_csv(REPORT_PATH, index=False)

    print(f"Peak Traffic Report Generated: {REPORT_PATH}")
    print(peak_report)


if __name__ == "__main__":
    main()
