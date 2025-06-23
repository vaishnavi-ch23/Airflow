import snowflake.connector

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
        CREATE OR REPLACE TABLE SPDATA.TRACKS_2 (
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