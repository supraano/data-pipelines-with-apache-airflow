import airflow.utils.dates
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

dag1 = DAG(
    dag_id="file_sensor_trigger_dag",
    start_date = airflow.utils.dates.days_ago(1),
    schedule_interval = "@daily",
    concurrency=16 # number of tasks allowed to run simultanously
)

for supermarket_id in range(1, 4):

    # returns true if file exists
    wait_for_file = FileSensor(
        task_id=f"wait_for_file_{supermarket_id}",
        filepath=f"/data/supermarket{supermarket_id}/data.csv",
        mode="reschedule", # release slot after finishing poking -> default: poke 
        dag=dag1
    )

    copy = DummyOperator(
        task_id=f"copy_supermarket{supermarket_id}",
        dag=dag1
    )

    process = DummyOperator(
        task_id=f"process_supermarket{supermarket_id}",
        dag=dag1
    )

    # no dependency to other dags allowed
    trigger_create_metrics = TriggerDagRunOperator(
        task_id=f"trigger_create_metrics_supermarket{supermarket_id}",
        trigger_dag_id="create_metrics_instantly",
        dag=dag1
    )

    wait_for_file >> copy >> process >> trigger_create_metrics

dag2 = DAG(
    dag_id = "create_metrics_instantly",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval=None # gets triggered via other dag
)

create_metrics = DummyOperator(
    task_id="create_metrics",
    dag=dag2
)

notify = DummyOperator(
    task_id="notify",
    dag=dag2
)

create_metrics >> notify