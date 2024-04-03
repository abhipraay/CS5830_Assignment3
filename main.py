from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime

from fetch import get_data,unzip
from parse import parseCSV
from extract import ExtractAndFilterFields, ExtractFieldsWithMonth
from process import process_csv
from compute import compute_avg, compute_monthly_avg
from aggregate import Aggregated
from plot import plot_geomaps,create_heatmap_visualization 
from delete import delete_csv

## Task 1
dag_1 = DAG(
    dag_id= "fetch_data",
    schedule_interval="@daily",
    default_args={
            "owner": "first_task",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 1, 1),
        },
    catchup=False
)

get_data = PythonOperator(
        task_id="get_data",
        python_callable=get_data,
        provide_context=True,
        op_kwargs={"name":"first_task"},
        dag=dag_1
    )


get_data

            
dag_2 = DAG(
    dag_id= "data_analysis",
    schedule_interval="@daily",
    default_args={
            "owner": "first_task",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 1, 1),
        },
    catchup=False
)


wait = FileSensor(
    task_id = 'wait',
    mode="poke",
    poke_interval = 5,                              # Check every 5 seconds
    timeout = 5,                                    # Timeout after 5 seconds
    filepath = "/root/airflow/DAGS/weather.zip",
    dag=dag_2,
    fs_conn_id = "fs_default",                      # File path system must be defined
)

unzip_task = PythonOperator(
        task_id="unzip_task",
        python_callable=unzip,
        provide_context=True,
        dag=dag_2
    )

process_csv_files_task = PythonOperator(
    task_id='process_csv_files',
    python_callable=process_csv,
    dag=dag_2,
)

compute_monthly_avg_task = PythonOperator(
    task_id='compute_monthly_averages',
    python_callable=compute_monthly_avg,
    dag=dag_2,
)

create_heatmap_task = PythonOperator(
    task_id='create_heatmap_visualization',
    python_callable=create_heatmap_visualization,
    dag=dag_2,
)

delete_csv_task = PythonOperator(
    task_id='delete_csv_file',
    python_callable=delete_csv,     
    dag=dag_2,
)

wait >> unzip_task >> process_csv_files_task >> compute_monthly_avg_task >> create_heatmap_task >> delete_csv_task