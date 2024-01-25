from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.latest_only import LatestOnlyOperator
import airflow.utils.dates as dates

dag = DAG(
    dag_id="conditional_tasks",
    start_date = dates.days_ago(5),
    schedule_interval="@daily"
)

fetch_data_api_1 = BashOperator(
    task_id="fetch_data_api_1",
    bash_command='echo "Fetching Data from API1!"',
    dag=dag
)

fetch_data_api_2 = BashOperator(
    task_id="fetch_data_api_2",
    bash_command='echo "Fetching Data from API2!"',
    dag=dag
)


latest_only = LatestOnlyOperator(
    task_id="latest_only",
    dag=dag
)

train_model = BashOperator(
    task_id="train_model",
    bash_command='echo "Training model!"',
    dag=dag
)

[fetch_data_api_1, fetch_data_api_2] >> latest_only >> train_model # fan-in and train model for latest run only

