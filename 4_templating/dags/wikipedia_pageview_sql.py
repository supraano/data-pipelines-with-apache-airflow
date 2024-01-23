from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import airflow.utils.dates
from urllib import request
import os

dag = DAG(
    dag_id="wikipedia_pageview_sql",
    start_date=airflow.utils.dates.days_ago(0, hour=1),
    schedule_interval="@hourly",
    template_searchpath="/tmp" # searchpath for sql queries
)

def _get_data(year, month, day, hour, output_path, **context):
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    request.urlretrieve(url, output_path)

get_data = PythonOperator(
    task_id = "get_data",
    python_callable=_get_data,
    op_kwargs={
        "year": "{{ execution_date.year }}",
        "month": "{{ execution_date.month }}",
        "day": "{{ execution_date.day }}",
        "hour": "{{ execution_date.hour }}",
        "output_path": "/tmp/wikipageviews.gz",
    },
    dag=dag
)

extract_gz = BashOperator(
    task_id="extract_gz",
    bash_command="gunzip --force /tmp/wikipageviews.gz",
    dag=dag
)



def _fetch_pageviews(pagenames, execution_date, **context):
    result = dict.fromkeys(pagenames, 0) # set counts to 0 for selected pages
    with open("/tmp/wikipageviews", "r") as file:
        for line in file:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames: # check conditions for fetching
                result[page_title] = view_counts

    with open("/tmp/postgres_query.sql", "w+") as f:
        for pagename, pageviewcount in result.items():
            f.write(
                "INSERT INTO pageview_counts VALUES("
                f"'{pagename}', {pageviewcount}, '{execution_date}'"
                ");\n"
            )

fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={
        "pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook"} # kwargs for templating
    },
    dag=dag
)

write_to_postgres = PostgresOperator(
    task_id = "write_to_postgres",
    postgres_conn_id="my_postgres", # conn id saved in admin -> connections (UI)
    sql="postgres_query.sql",
    dag=dag
)


get_data >> extract_gz >> fetch_pageviews >> write_to_postgres