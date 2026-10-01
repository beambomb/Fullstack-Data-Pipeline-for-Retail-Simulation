import sys
import os
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)

from data_engineering.airflow.dags.retail_daily_etl_dag import (
    task_ingest_raw_to_oltp,
    task_cleanse_and_stage,
    task_load_dimensional_olap,
    task_data_quality_audit,
)


def run_full_pipeline():
    start_time = time.time()
    print("\n" + "#" * 70)
    print("### APACHE AIRFLOW ORCHESTRATOR: RETAIL DAILY ETL PIPELINE ###")
    print(f"### Scheduled Run: Daily Batch | Target: Docker PostgreSQL ###")
    print("#" * 70 + "\n")

    steps = [
        ("Task 1/4: Ingest Raw to OLTP (3NF)", task_ingest_raw_to_oltp),
        ("Task 2/4: Cleanse & Stage (Silver Layer)", task_cleanse_and_stage),
        ("Task 3/4: Load Dimensional OLAP (Star Schema)", task_load_dimensional_olap),
        ("Task 4/4: Automated Data Quality Audit", task_data_quality_audit),
    ]

    for title, task_fn in steps:
        print(f"\n>>> [EXECUTING] {title}...")
        task_start = time.time()
        try:
            task_fn()
            elapsed = time.time() - task_start
            print(f">>> [SUCCESS] {title} completed in {elapsed:.2f} seconds.")
        except Exception as e:
            print(f">>> [FAILED] {title} encountered an error: {e}")
            sys.exit(1)

    total_elapsed = time.time() - start_time
    print("\n" + "#" * 70)
    print(f"### ALL 4 PIPELINE TASKS SUCCEEDED! Total Runtime: {total_elapsed:.2f}s ###")
    print("#" * 70 + "\n")


if __name__ == "__main__":
    run_full_pipeline()
