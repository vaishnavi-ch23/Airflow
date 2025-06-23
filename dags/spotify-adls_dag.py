from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import requests
import base64
import json
import time
from langdetect import detect
import langcodes
import random
import pandas as pd
import io
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from airflow.exceptions import AirflowException
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
 
 
# Spotify credentials from Airflow Variables
from airflow.models import Variable

SPOTIFY_CLIENT_ID = Variable.get("spotify_client_id")
SPOTIFY_CLIENT_SECRET = Variable.get("spotify_client_secret")



def get_spotify_token():
    auth_str = f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}"
    b64_auth_str = base64.b64encode(auth_str.encode()).decode()

    url = "https://accounts.spotify.com/api/token"
    headers = {"Authorization": f"Basic {b64_auth_str}"}
    data = {"grant_type": "client_credentials"}

    resp = requests.post(url, headers=headers, data=data)
    resp.raise_for_status()
    return resp.json()['access_token']

def detect_language(text):
    try:
        short_code = detect(text)
        full_name = langcodes.Language.get(short_code).display_name("en")
        return full_name
    except:
        return "Unknown"

def search_tracks(token, query="a", limit=50, max_tracks=1000):
    url = "https://api.spotify.com/v1/search"
    headers = {"Authorization": f"Bearer {token}"}
    all_tracks = []
    offset = 0

    while offset < max_tracks:
        params = {
            "q": query,
            "type": "track",
            "limit": limit,
            "offset": offset
        }
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        tracks = data['tracks']['items']
        if not tracks:
            break
        all_tracks.extend(tracks)
        offset += limit
        time.sleep(0.1)

    return all_tracks[:max_tracks]

def get_audio_features_batch(token, track_ids):
    url = "https://api.spotify.com/v1/audio-features"
    headers = {"Authorization": f"Bearer {token}"}
    features_map = {}

    for i in range(0, len(track_ids), 100):
        ids = ",".join(track_ids[i:i+100])
        response = requests.get(url, headers=headers, params={"ids": ids})
        if response.status_code == 200:
            features = response.json().get('audio_features', [])
            for feature in features:
                if feature:
                    features_map[feature['id']] = feature
        else:
            print(f"❌ Failed batch {i//100 + 1}: {response.text}")
        time.sleep(0.2)

    return features_map

def get_artist(token, artist_id, cache):
    if artist_id in cache:
        return cache[artist_id]

    url = f"https://api.spotify.com/v1/artists/{artist_id}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    artist_info = resp.json()
    cache[artist_id] = artist_info
    return artist_info

def fetch_spotify_data(ti,**context):
    token = get_spotify_token()
    query = "a"  # You can parametrize this if needed
    max_tracks = 1000  # You can parametrize this too

    tracks = search_tracks(token, query=query, max_tracks=max_tracks)

    artist_cache = {}
    all_data = []

    track_ids = [track['id'] for track in tracks]
    audio_features_map = get_audio_features_batch(token, track_ids)

    for track in tracks:
        track_id = track['id']
        track_name = track['name']
        album = track['album']['name']
        release_date = track['album']['release_date']
        duration_ms = track['duration_ms']
        explicit = track['explicit']
        popularity = track['popularity']
        artists = track['artists']
        artist_names = []
        for artist in artists:
            artist_info = get_artist(token, artist['id'], artist_cache)
            artist_names.append(artist_info['name'])

        artist_count = len(artist_names)
        audio = audio_features_map.get(track_id, {})

        streams = random.randint(10_000, 1_000_000)  # Simulated streams
        language = detect_language(track_name)

        all_data.append({
            'track_id': track_id,
            'track_name': track_name,
            'album': album,
            'release_date': release_date,
            'duration_ms': duration_ms,
            'explicit': explicit,
            'artist_names': ", ".join(artist_names),
            'artist_count': artist_count,
            'popularity': popularity,
            'bpm': audio.get('tempo'),
            'key': audio.get('key'),
            'mode': audio.get('mode'),
            'danceability': audio.get('danceability'),
            'energy': audio.get('energy'),
            'valence': audio.get('valence'),
            'acousticness': audio.get('acousticness'),
            'instrumentalness': audio.get('instrumentalness'),
            'liveness': audio.get('liveness'),
            'speechiness': audio.get('speechiness'),
            'language': language,
            'streams': streams
        })
        
    with open ("include/extracted.json","w",encoding = "utf-8") as f:
        json.dump(all_data, f, indent = 4)

    # Push the entire dataset as JSON string to XCom
    ti.xcom_push(key='spotify_data', value=json.dumps(all_data))
    
def json_to_csv(ti, **context):
    # Pull data from XCom
    data = ti.xcom_pull(task_ids='fetch_spotify_data', key='spotify_data')
    
    # Check if data is None
    if data is None:
        raise AirflowException("No data received from XCom for task 'fetch_spotify_data' with key 'spotify_data'")
    
    # Check if data is a string and try to parse it as JSON
    if isinstance(data, str):
        try:
            data = json.loads(data)
            print("Parsed string data from XCom into Python object")
        except json.JSONDecodeError as e:
            raise AirflowException(f"Failed to parse XCom data as JSON: {str(e)}")
    
    # Check if data is a list and not empty
    if not isinstance(data, list):
        raise AirflowException(f"Expected a list from XCom, but got type: {type(data)}")
    if not data:
        raise AirflowException("Empty data received from XCom for task 'fetch_spotify_data'")
    
    # Log data for debugging
    print(f"Data type: {type(data)}")
    print(f"Number of records: {len(data)}")
    print(f"Sample data (first 2 records): {data[:2]}")
    
    try:
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Verify expected columns are present
        expected_columns = [
            'track_id', 'track_name', 'album', 'release_date', 'duration_ms',
            'explicit', 'artist_names', 'artist_count', 'popularity', 'bpm',
            'key', 'mode', 'danceability', 'energy', 'valence', 'acousticness',
            'instrumentalness', 'liveness', 'speechiness', 'language', 'streams'
        ]
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            raise AirflowException(f"Missing expected columns in data: {missing_columns}")
        
        # Save to CSV
        df.to_csv('include/stage/tracks.csv', index=False, encoding='utf-8')
        print("Successfully saved data to stage/tracks.csv")
    except Exception as e:
        raise AirflowException(f"Failed to convert data to DataFrame or save to CSV: {str(e)}")
    
    
    

def upload_to_adls(ti,**context):

    
    with open('include/stage/tracks.csv', 'rb') as f:  # Note 'rb' for binary mode
        data = f.read()
    
    # Get Azure connection details
    conn = BaseHook.get_connection("azure_blob_conn")
    conn_str = json.loads(conn.extra)['connection_string']
    
    # Create Blob Service Client
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    container_name = "spotify-data"
    
    # Create blob path with current date
    blob_path = f"spotify/tracks.csv"  # Changed to .csv to match input
    
    # Upload the file
    blob_client = blob_service_client.get_blob_client(
        container=container_name, 
        blob=blob_path
    )
    
    # Upload the data (using overwrite=True to replace if exists)
    blob_client.upload_blob(data, overwrite=True)
    
    # Return confirmation
    return f"Successfully uploaded to {blob_path}"

            
            
with DAG(
    "spotify-adls_dag",
    start_date=datetime(2024, 1, 1),
    catchup=False
    ) as dag:

    fetch = PythonOperator(
        task_id="fetch_spotify_data",
        python_callable=fetch_spotify_data
    )
    
    convert = PythonOperator(
        task_id = "convert_jsontocsv",
        python_callable=json_to_csv
    )

    upload = PythonOperator(
        task_id="upload_to_adls",
        python_callable=upload_to_adls
    )
    
    copy_to_snowflake_task_1 = CopyFromExternalStageToSnowflakeOperator(
        task_id='copy_from_BLOB_to_snowflake',
        # snowflake_conn_id='snowflake_venky',
        # stage='STUDENT_DB.STUDENT_SCHM.BRONZE_STAGE',  
        # table='STUDENT_DB.STUDENT_SCHM.STUDENT_TABLE',  
        snowflake_conn_id='snowflake_defaults',
        stage='SPOTIFY.SPDATA.MY_AZURE_STAGE_TRACKS',  
        table='SPOTIFY.SPDATA.TRACKS',  
        file_format='(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY= \'\"\' SKIP_HEADER = 1)',
        pattern='.*',  
    )
    
    copy_to_snowflake_task_2 = CopyFromExternalStageToSnowflakeOperator(
        task_id='copy_users_from_blob',
        # snowflake_conn_id='snowflake_venky',
        # stage='STUDENT_DB.STUDENT_SCHM.BRONZE_STAGE',  
        # table='STUDENT_DB.STUDENT_SCHM.STUDENT_TABLE',  
        snowflake_conn_id='snowflake_defaults',
        stage='SPOTIFY.SPDATA.MY_AZURE_STAGE_USER',  
        table='SPOTIFY.SPDATA.SPOTIFY_USER_DATA',  
        file_format='(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY= \'\"\' SKIP_HEADER = 1)',
        pattern='.*',  
    )
    
    
    # download_blob_task = PythonOperator(
    #     task_id="download_blob",
    #     python_callable=download_blob
    # )

    # load_snowflake = PythonOperator(
    # task_id='load_snowflake',
    # python_callable=load_data_to_snowflake
    # )
    
    fetch >> convert>> upload >> [ copy_to_snowflake_task_1 , copy_to_snowflake_task_2]
    
    
