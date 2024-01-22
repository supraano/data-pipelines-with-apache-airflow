import datetime as dt 
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id='scheduled_daily',
    start_date=dt.datetime.now(),
    end_date=dt.datetime(2024, 2, 1),
    schedule_interval='@daily'
)

fetch_events = BashOperator(
    task_id='fetch_events',
    dag=dag,
    bash_command=(
        'mkdir -p /data && '
        'curl -o /data/events.json "http://3_scheduling-events_api-1:5000/events"'
    )
)

def _calculate_stats(input_path, output_path):
    events = pd.read_json(input_path)
    stats = events.groupby(['date', 'user']).size().reset_index()
    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)

calculate_stats = PythonOperator(
    task_id='calculate_stats',
    dag=dag,
    python_callable=_calculate_stats,
    op_kwargs={
        'input_path': '/data/events.json',
        'output_path': '/data/stats.csv'
    }
)

fetch_events >> calculate_stats