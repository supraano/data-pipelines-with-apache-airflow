from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
import datetime
import airflow.utils.dates

dag1 = DAG(
    dag_id="preceding_dag_for_polling_task_states",
    schedule_interval="0 16 * * *",
    start_date = airflow.utils.dates.days_ago(1)
    )

dag2 = DAG(
    dag_id="trailing_dag_for_polling_task_states",
      schedule_interval="0 20 * * *", # executed 4 hours later
      start_date = airflow.utils.dates.days_ago(1)
      )

DummyOperator(
    task_id ="etl",
    dag=dag1
)

ExternalTaskSensor(
    task_id="wait_for_etl",
    external_dag_id="preceding_dag_for_polling_task_states",
    external_task_id="etl",
    execution_delta=datetime.timedelta(hours=4), # class polls metastore for same execution date -> timedelta needed
    dag=dag2
)