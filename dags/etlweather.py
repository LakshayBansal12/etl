from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json


#Latitude and Longitude for the desired location (london in this case)
LATITUDE = '51.5074'
LONGITUTDE = '-0.1278'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner' : 'airflow',
    'start_date':days_ago(1)
}

#DAG

with DAG(dag_id='weather_etl_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dags:
    
    #this task is for getting data
    @task()
    def extract_weather_data():
        """Extract weather data from Open_Meteo API using Airflow connection."""

        #use HTTP hook to get the weather data

        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')

        # Build the API enpint
        # https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        endpoint=f'v1/forecast?latitude={LATITUDE}&longitude={LONGITUTDE}&current_weather=true'
        ## Make a request via HTTP hook
        response = http_hook.run(endpoint)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather details : {respnse.status_code}")
        
    # this task is to transform data
    @task()
    def transform_Weather_data(weather_data):
        """Transfomr the extracted weather data."""
        current_weather = weather_data['current_weather']
        transform_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUTDE,
            'temperature': current_weather['temperature'],
            'windspeed' : current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }

        return transform_data
    
    #task for pushing transform data into data base
    @task()
    def load_weather_data(transformed_data):
        """Load transformed data into PostgresSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        #create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Insert transformed data into the table
        cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))

        conn.commit()
        cursor.close()
    
    ## DAG workflow ETL pipeline
    weather_data = extract_weather_data()
    transformed_data = transform_Weather_data(weather_data)
    load_weather_data(transformed_data)

