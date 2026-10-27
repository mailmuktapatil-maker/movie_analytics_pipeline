import os
import sys
import importlib.util
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# WSL project path
PROJECT_ROOT = "/mnt/c/Users/mukta patil/OneDrive/Desktop/movie_analytics_project"

def run_pig_cleaning():
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)
    from pig.clean_data import clean_watch_data
    clean_watch_data()

def run_hive_reporting():
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)
    from hive.warehouse_query import run_hive_query
    run_hive_query()

def run_pyspark_analytics():
    file_path = os.path.join(PROJECT_ROOT, "pyspark", "batch_analytics.py")
    spec = importlib.util.spec_from_file_location("batch_analytics_module", file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run_batch_analytics()

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'movie_analytics_pipeline',
    default_args=default_args,
    description='End-to-End Movie Analytics Pipeline (Pig -> Hive -> PySpark)',
    schedule='0 0 * * *',
    catchup=False,
)

task_pig = PythonOperator(
    task_id='pig_data_cleaning',
    python_callable=run_pig_cleaning,
    dag=dag,
)

task_hive = PythonOperator(
    task_id='hive_warehouse_query',
    python_callable=run_hive_reporting,
    dag=dag,
)

task_pyspark = PythonOperator(
    task_id='pyspark_batch_analytics',
    python_callable=run_pyspark_analytics,
    dag=dag,
)

# Execution flow
task_pig >> task_hive >> task_pyspark