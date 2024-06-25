import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from scripts.global_var import _LANDED, _PROCESSED, _CLEANED, _MYSQL_CSVS_DIR, _SQL_TABLES_INIT, _SQL_STAGING, _SQL_TRANSFORM_LOAD, _SQL_UPDATE_VIEW
from modules.airflow_file_handler import Read_landing
from scripts.csv_cleaning import CSV_source_cleaner
from scripts.mysql_dwh import dwh_init_tables, dwh_stage, dwh_transform_load, dwh_update_view
import shutil
import logging
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

mysql_credentials = {
    "host": "mysql-db",
    "user": "root",
    "password": "root"
}

def init_tables_task():
    dwh_init_tables(**mysql_credentials)
    
def dwh_stage_task(**kwargs):
    cleaned_csv_file_path = kwargs['ti'].xcom_pull(key='cleaned_file_name')
    logging.info(f"FILE PATH TO BE STAGED:{cleaned_csv_file_path}")
    dwh_stage(
        (cleaned_csv_file_path,), 
        **mysql_credentials
    )

def dwh_transform_load_task(**kwargs):
    logs = kwargs['ti'].xcom_pull(key='error_logs')
    logs = json.dumps(logs)
    source_name = kwargs['ti'].xcom_pull(key='src_file_name')
    params = {
        'ETL_errors': logs, 
        'source_name': source_name
    }
    dwh_transform_load(
        params=params, 
        **mysql_credentials
    )
    
def dwh_update_view_task():
    dwh_update_view(**mysql_credentials)
    

def check_files(**kwargs):
    reader = Read_landing(_LANDED)
    file_full_path = reader.get_newest_file('.csv')
    if not file_full_path:
        raise ValueError("No files found in the landing directory.")
    file_name = os.path.split(file_full_path)[-1]
    kwargs['ti'].xcom_push(key='src_file_name', value=file_name)
    kwargs['ti'].xcom_push(key='src_file_path', value=file_full_path)

def run_cleaner(**kwargs):
    file_name = kwargs['ti'].xcom_pull(key='src_file_name')
    source_path = os.path.join(_LANDED, file_name)
    destination_path = os.path.join(_CLEANED, f'cleaned_{file_name}')
    cleaned_file_in_mysql = os.path.split(destination_path)[-1]
    cleaned_file_in_mysql = os.path.join(_MYSQL_CSVS_DIR, cleaned_file_in_mysql)
    
    cleaner = CSV_source_cleaner(source_path)
    cleaner.save_cleaned_data(destination_path)
    kwargs['ti'].xcom_push(key='cleaned_file', value=destination_path)
    kwargs['ti'].xcom_push(key='cleaned_file_name', value=cleaned_file_in_mysql)
    kwargs['ti'].xcom_push(key='error_logs', value=cleaner.json_logs)

def move_processed_file(**kwargs):
    src_file_name = kwargs['ti'].xcom_pull(key='src_file_name')
    source_path = kwargs['ti'].xcom_pull(key='src_file_path')
    destination_path = os.path.join(_PROCESSED, src_file_name)
    shutil.move(source_path, destination_path)


with DAG(
    dag_id="pipeline",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["pipeline", "mysql", "pandas"],
    default_args=default_args
) as dag:
    
    task_check_files = PythonOperator(
        task_id = "task_check_files",
        python_callable = check_files
    )
    
    task_clean = PythonOperator(
        task_id = "task_clean",
        python_callable= run_cleaner
    )
    
    task_stage = PythonOperator(
        task_id = "task_stage",
        python_callable=dwh_stage_task
    )
    
    task_transform_load = PythonOperator(
        task_id = "task_transform_load",
        python_callable=dwh_transform_load_task
    )
    
    task_move_to_processed = PythonOperator(
        task_id="task_move_to_processed",
        python_callable=move_processed_file
    )
    
    task_update_vw = PythonOperator(
        task_id="task_update_vw",
        python_callable=dwh_update_view_task
    )
    
    # Define task dependencies
    # task_init_tables >> task_clean
    task_check_files >> task_clean
    task_clean >> task_stage >> task_transform_load
    task_transform_load >> task_move_to_processed
    task_transform_load >> task_update_vw