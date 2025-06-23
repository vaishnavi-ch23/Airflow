import snowflake.connector

def create_snowflake_connection():
    """
    Establishes and returns a connection to Snowflake using predefined parameters.
    """
    conn_params = {
        "user": "VAISHNAVI23",
        "password": "vaishnavi_CH23",
        "account": "SPSRJWA-SH71950",
        "warehouse": "COMPUTE_WH",
        "database": "SPOTIFY",
        "schema": "SPDATA",
        "role": "ACCOUNTADMIN"
    }

    try:
        conn = snowflake.connector.connect(**conn_params)
        print("Successfully connected to Snowflake!")
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        raise

if __name__ == "__main__":
    # Example usage:
    connection = None
    try:
        connection = create_snowflake_connection()
        # You can now use the 'connection' object to execute queries
        # For example:
        # cursor = connection.cursor()
        # cursor.execute("SELECT current_version()")
        # print(cursor.fetchone())
    except Exception:
        # The error is already printed in the function, just pass here
        pass
    finally:
        if connection:
            connection.close()
            print("Snowflake connection closed.")