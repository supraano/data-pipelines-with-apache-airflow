import datetime as dt 
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id='scheduled_timedelta_partitioned',
    start_date=dt.datetime(2019, 1, 1),
    end_date=dt.datetime(2019, 1, 5),
    schedule_interval=dt.timedelta(days=3) # execute every three days
)

fetch_events = BashOperator(
    task_id='fetch_events',
    dag=dag,
    bash_command=(
        'mkdir -p /data/events && '
        'curl -o /data/events/{{ds}}.json ' # create json for each execution
        'http://3_scheduling-events_api-1:5000/events?'
        'start_date={{ds}}&'
        'end_date={{next_ds}}'
    )
)

def _calculate_stats(**context):
    input_path = context["templates_dict"]["input_path"]
    output_path = context["templates_dict"]["output_path"]

    print("Saving stats within following path:")
    print(output_path)

    Path(output_path).parent.mkdir(exist_ok=True)

    events = pd.read_json(input_path)
    print(events)
    stats = events.groupby(['date', 'user']).size().reset_index()
    print(stats)
    stats.to_csv(output_path, index=False)

calculate_stats = PythonOperator(
    task_id='calculate_stats',
    dag=dag,
    python_callable=_calculate_stats,
    templates_dict={
        'input_path': '/data/events/{{ds}}.json',
        'output_path': '/data/stats/{{ds}}.csv'
    }
)

fetch_events >> calculate_stats