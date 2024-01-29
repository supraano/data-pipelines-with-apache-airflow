import airflow.utils.dates
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy import DummyOperator

dag = DAG(
    dag_id="file_sensor",
    start_date = airflow.utils.dates.days_ago(1),
    schedule_interval = "@daily",
    concurrency=16 # number of tasks allowed to run simultanously
)

create_metrics = DummyOperator(
    task_id="create_metrics",
    dag=dag
)

for supermarket_id in range(1, 4):

    # returns true if file exists
    wait_for_file = FileSensor(
        task_id=f"wait_for_file_{supermarket_id}",
        filepath=f"/data/supermarket{supermarket_id}/data.csv",
        mode="reschedule", # release slot after finishing poking -> default: poke 
        dag=dag
    )

    copy = DummyOperator(
        task_id=f"copy_supermarket{supermarket_id}",
        dag=dag
    )

    process = DummyOperator(
        task_id=f"process_supermarket{supermarket_id}",
        dag=dag
    )

    wait_for_file >> copy >> process >> create_metrics