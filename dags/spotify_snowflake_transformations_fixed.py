from airflow import DAG
from airflow.providers.snowflake.operators.snowflake_sql import SnowflakeSqlOperator
from datetime import datetime
import os

default_args = {
    'owner': 'vaishnavi',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

dag = DAG(
    dag_id='spotify_snowflake_transformations',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Automates all Spotify Snowflake transformations'
)

def load_sql(filename):
    path = os.path.join(os.path.dirname(__file__), 'sql', filename)
    with open(path, 'r') as file:
        return file.read()

task_1 = SnowflakeSqlOperator(
    task_id='run_silver_transformations',
    sql=load_sql('01_transform_to_silver.sql'),
    snowflake_conn_id='snowflake_defaults',
    dag=dag
)

task_2 = SnowflakeSqlOperator(
    task_id='run_user_transformations',
    sql=load_sql('02_transform_user_data_to_silver.sql'),
    snowflake_conn_id='snowflake_defaults',
    dag=dag
)

task_3 = SnowflakeSqlOperator(
    task_id='create_gold_tables',
    sql=load_sql('03_create_gold_tables.sql'),
    snowflake_conn_id='snowflake_defaults',
    dag=dag
)

task_4 = SnowflakeSqlOperator(
    task_id='create_analytics_views',
    sql=load_sql('04_create_views.sql'),
    snowflake_conn_id='snowflake_defaults',
    dag=dag
)

task_1 >> task_2 >> task_3 >> task_4