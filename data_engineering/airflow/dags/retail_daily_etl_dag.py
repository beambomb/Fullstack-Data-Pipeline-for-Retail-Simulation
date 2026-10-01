import os
import sys
from datetime import datetime, timedelta

# Airflow standard imports (mockable for local test execution)
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.operators.bash import BashOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False


default_args = {
    "owner": "retail_data_engineer",
    "depends_on_past": False,
    "start_date": datetime(2026, 9, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def task_ingest_raw_to_oltp():
    import data_engineering.scripts.load_oltp as oltp_loader
    print("[AIRFLOW TASK] Starting Task 1: Ingesting Raw CSVs into PostgreSQL OLTP...")
    oltp_loader.main()
    print("[AIRFLOW TASK] Task 1 Completed Successfully.")


def task_cleanse_and_stage():
    import data_engineering.scripts.cleanse_silver as cleanser
    print("[AIRFLOW TASK] Starting Task 2: Cleansing & Preprocessing (Silver Layer)...")
    cleanser.run_cleansing()
    print("[AIRFLOW TASK] Task 2 Completed Successfully.")


def task_load_dimensional_olap():
    import data_engineering.scripts.load_olap as olap_loader
    print("[AIRFLOW TASK] Starting Task 3: Loading into OLAP Star Schema...")
    olap_loader.run_olap_loader()
    print("[AIRFLOW TASK] Task 3 Completed Successfully.")


def task_data_quality_audit():
    import psycopg2
    from psycopg2.extras import RealDictCursor

    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = int(os.getenv("POSTGRES_PORT", "5433"))
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_pass = os.getenv("POSTGRES_PASSWORD", "postgres")
    olap_db = os.getenv("OLAP_DB_NAME", "retail_olap")

    print("[AIRFLOW TASK] Starting Task 4: Automated Data Quality & Integrity Audit...")
    conn = psycopg2.connect(host=db_host, port=db_port, user=db_user, password=db_pass, dbname=olap_db)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Check 1: Fact sales must not be empty
            cur.execute("SELECT count(*) as total_facts, sum(net_revenue) as total_rev FROM fact_sales;")
            fact_summary = cur.fetchone()
            assert fact_summary["total_facts"] > 0, "DATA QUALITY FAIL: fact_sales is empty!"
            assert fact_summary["total_rev"] > 0, "DATA QUALITY FAIL: Total revenue is zero or negative!"

            # Check 2: No orphan foreign keys
            cur.execute("""
                SELECT count(*) as orphan_count 
                FROM fact_sales f 
                LEFT JOIN dim_product p ON f.product_key = p.product_key 
                WHERE p.product_key IS NULL;
            """)
            orphans = cur.fetchone()["orphan_count"]
            assert orphans == 0, f"DATA QUALITY FAIL: Found {orphans} orphan product keys in fact_sales!"

            # Check 3: Date dimension integrity
            cur.execute("SELECT count(*) as date_count FROM dim_date;")
            date_count = cur.fetchone()["date_count"]
            assert date_count > 0, "DATA QUALITY FAIL: dim_date is empty!"

            print(f"[DATA QUALITY AUDIT PASSED]")
            print(f" - Fact Sales Count : {fact_summary['total_facts']:,} valid rows")
            print(f" - Net Revenue Total: Rp {fact_summary['total_rev']:,.2f}")
            print(f" - Orphan Keys Check: 0 orphans found (100% Referential Integrity)")
            print(f" - Calendar Coverage: {date_count} active dates verified")
    finally:
        conn.close()


if AIRFLOW_AVAILABLE:
    dag = DAG(
        dag_id="retail_daily_etl_pipeline",
        default_args=default_args,
        description="End-to-End Retail Data Pipeline: OLTP Ingestion -> Cleansing -> OLAP Star Schema -> Data Quality Audit",
        schedule_interval="0 1 * * *",  # Runs daily at 01:00 AM midnight
        catchup=False,
        tags=["retail", "data_engineering", "olap", "etl"],
    )

    t1_ingest = PythonOperator(
        task_id="ingest_raw_to_oltp",
        python_callable=task_ingest_raw_to_oltp,
        dag=dag,
    )

    t2_cleanse = PythonOperator(
        task_id="cleanse_and_stage_silver",
        python_callable=task_cleanse_and_stage,
        dag=dag,
    )

    t3_load_olap = PythonOperator(
        task_id="load_dimensional_olap",
        python_callable=task_load_dimensional_olap,
        dag=dag,
    )

    t4_audit = PythonOperator(
        task_id="data_quality_audit",
        python_callable=task_data_quality_audit,
        dag=dag,
    )

    # DAG Dependency Pipeline
    t1_ingest >> t2_cleanse >> t3_load_olap >> t4_audit
