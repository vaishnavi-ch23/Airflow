from datetime import datetime, timedelta
from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
import snowflake.connector
from airflow.models import Variable


def load_data_to_snowflake(ti, **kwargs):
    # tracks_json = ti.xcom_pull(task_ids="download_blob", key="tracks_data")
    
    # if not tracks_json:
    #     raise ValueError("No tracks data found in XCom")

    # tracks_df = pd.DataFrame(json.loads(tracks_json))

    # Correct connection parameters
    conn_params = {
    "user": "VAISHNAVI23",
    "password": "vaishnavi_CH23",
    "account" : "SPSRJWA-SH71950" , # If you're in us-west-2 region
    "warehouse" : "COMPUTE_WH",
    "database": "SPOTIFY",
    "schema": "SPDATA",
    "role": "ACCOUNTADMIN"  # If you have a specific role
    }

    try:
        # Create connection
        conn = snowflake.connector.connect(**conn_params)
        cs = conn.cursor()

        # Create table if not exists
        create_table_query = """
            CREATE OR REPLACE TABLE SPDATA.TRACKS (
                track_id VARCHAR,
                track_name VARCHAR,
                album VARCHAR,
                release_date DATE,
                duration_ms INTEGER,
                explicit BOOLEAN,
                artist_names VARCHAR,
                artist_count INTEGER,
                popularity INTEGER,
                bpm FLOAT,
                key INTEGER,
                mode INTEGER,
                danceability FLOAT,
                energy FLOAT,
                valence FLOAT,
                acousticness FLOAT,
                instrumentalness FLOAT,
                liveness FLOAT,
                speechiness FLOAT,
                language VARCHAR,
                streams INTEGER
            )
        """
        cs.execute(create_table_query)

        # Write pandas dataframe
        # success, nchunks, nrows, _ = write_pandas(
        #     conn=conn,
        #     df=tracks_df,
        #     table_name="TRACKS",
        #     schema="SPDATA"
        # )

        # if success:
        #     print(f" Loaded {nrows} rows into SPDATA.TRACKS successfully.")
        # else:
        #     raise Exception(" Failed to load data into Snowflake.")
    except Exception as e:
        print(f" Error: {str(e)}")
        raise
    finally:
        if 'cs' in locals():
            cs.close()
        if 'conn' in locals():
            conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'snowflake_table_creation',
    default_args=default_args,
    description='Create tables in Snowflake for Spotify data',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['snowflake', 'spotify'],
) as dag:

    # Create the Snowflake connection if it doesn't exist
    create_connection = PythonOperator(
        task_id='create_snowflake_connection',
        python_callable=load_data_to_snowflake,
    )


    # Task flow
    create_connection