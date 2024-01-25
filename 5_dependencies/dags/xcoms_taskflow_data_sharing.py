import uuid
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.latest_only import LatestOnlyOperator
import airflow.utils.dates as dates
from airflow.decorators import task

with DAG(
    dag_id="xcoms_data_sharing",
    start_date = dates.days_ago(5),
    schedule_interval="@daily",
) as dag:
    

    @task
    def train_model():
        model_id=str(uuid.uuid4())
        return model_id # return value as input for next task using xcoms

    @task
    def deploy_model(model_id):
        print(f"Deploying model with id {model_id}")

    latest_only = LatestOnlyOperator(
        task_id="latest_only",
        dag=dag
    )

    clean_up = BashOperator(
        task_id="clean_up",
        bash_command='echo "Cleaning up pipeline!"',
        dag=dag
    )

    model_id = train_model()
    deploy_model(model_id) >> latest_only >> clean_up
