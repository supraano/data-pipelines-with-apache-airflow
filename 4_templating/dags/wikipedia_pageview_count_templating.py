from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import airflow.utils.dates

dag = DAG(
    dag_id="wikipedia_pageview_templated",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly"
)

get_data = BashOperator(
    task_id="get_data",
    bash_command = (
        "curl -o /tmp/wikipageviews.gz "
        "https://dumps.wikimedia.org/other/pageviews/"
        "{{ execution_date.year }}/"
        "{{ execution_date.year }}-"
        "{{ '{:02}'.format(execution_date.month) }}/"
        "pageviews-{{ execution_date.year }}"
        "{{ '{:02}'.format(execution_date.month) }}"
        "{{ '{:02}'.format(execution_date.day) }}-"
        "{{ '{:02}'.format(execution_date.hour) }}0000.gz"
        ),
dag=dag,
)



print_data_head = BashOperator(
    task_id="print_data",
    bash_command = (
        "ls /tmp"
    ),
    dag=dag
)

get_data >> print_data_head