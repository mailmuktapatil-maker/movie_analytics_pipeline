"""
Movie Analytics Pipeline Orchestration Runner
=============================================
Runs the complete data processing pipeline:
  Step 1: Pig ETL - Raw watch history data cleaning
  Step 2: Hive SQL - Catalog exploration & genre querying
  Step 3: PySpark Batch - Full join, KPI aggregation & Parquet/CSV export

Can execute directly or via Airflow DAG test mode.
"""

import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Ensure output directory exists
(PROJECT_ROOT / "data" / "processed").mkdir(parents=True, exist_ok=True)


def run_via_airflow_dag():
    """Execute pipeline via Airflow DAG test runner."""
    print("\n" + "=" * 65)
    print("[ORCHESTRATION ENGINE] Apache Airflow DAG Runner")
    print("=" * 65)
    try:
        from airflow.movie_pipeline_dag import dag
        dag.test()
        print("\n" + "=" * 65)
        print("[SUCCESS] Airflow Orchestrated Execution Completed")
        print("=" * 65 + "\n")
        return True
    except Exception as e:
        print(f"\n[WARNING] Airflow DAG runner encountered an issue: {e}")
        print("[FALLBACK] Switching to direct sequential task execution...\n")
        return False


def run_direct_pipeline():
    """Direct execution of all pipeline stages with performance tracking."""
    import importlib.util
    def load_task(rel, name):
        spec = importlib.util.spec_from_file_location(name, PROJECT_ROOT / rel)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    clean_watch_data = load_task("pig/clean_data.py", "pig_clean").clean_watch_data
    run_hive_query = load_task("hive/warehouse_query.py", "hive_query").run_hive_query
    run_batch_analytics = load_task("pyspark/batch_analytics.py", "spark_batch").run_batch_analytics

    print("\n" + "=" * 65)
    print("[DIRECT ORCHESTRATION] Sequential Task Execution")
    print("=" * 65)

    start_total = time.time()

    # Step 1: Pig Cleaning
    print("\n[TASK 1/3] Simulating Apache Pig ETL (Data Cleaning)...")
    t0 = time.time()
    try:
        clean_watch_data()
        print(f"[SUCCESS] Task 1 completed in {time.time() - t0:.2f}s")
    except Exception as e:
        print(f"[ERROR] Task 1 (Pig Cleaning) failed: {e}")
        return False

    # Step 2: Hive Warehouse Query
    print("\n[TASK 2/3] Simulating Apache Hive Data Warehouse (SQL Query)...")
    t0 = time.time()
    try:
        run_hive_query()
        print(f"[SUCCESS] Task 2 completed in {time.time() - t0:.2f}s")
    except Exception as e:
        print(f"[ERROR] Task 2 (Hive Warehouse) failed: {e}")
        return False

    # Step 3: PySpark Batch Analytics
    print("\n[TASK 3/3] Executing PySpark Batch Analytics (Aggregation & Parquet Sink)...")
    t0 = time.time()
    try:
        run_batch_analytics()
        print(f"[SUCCESS] Task 3 completed in {time.time() - t0:.2f}s")
    except Exception as e:
        print(f"[ERROR] Task 3 (PySpark Analytics) failed: {e}")
        return False

    print("\n" + "=" * 65)
    print(f"[COMPLETE] All 3 pipeline stages finished in {time.time() - start_total:.2f}s!")
    print(f"[OUTPUT] Processed outputs ready in: {PROJECT_ROOT / 'data' / 'processed'}")
    print("=" * 65 + "\n")
    return True


if __name__ == "__main__":
    # Try running via Airflow DAG test runner first
    success = run_via_airflow_dag()
    if not success:
        run_direct_pipeline()