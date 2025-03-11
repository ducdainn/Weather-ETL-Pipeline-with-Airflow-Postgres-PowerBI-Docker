from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import pandas as pd

POSTGRES_CONN_ID = 'postgres_weather'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

df = pd.read_csv("D:/Learning/DE/Weather/Sub_Data/vietnam_provinces.csv")  # Ensure correct path
provinces = df.to_dict(orient="records")  # Convert DataFrame to a list of dicts

with DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    description='Weather ETL pipeline visualization',
    schedule_interval='@daily',
    catchup=False
) as dag:

    @task()
    def extract_task(latitude, longitude):
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        endpoint = f'/v1/forecast?latitude={latitude}&longitude={longitude}&hourly=temperature_2m,relative_humidity_2m,rain,wind_speed_10m,wind_gusts_10m,visibility,dew_point_2m,wind_direction_10m,cloud_cover&forecast_days=1'

        response = http_hook.run(endpoint)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f'Failed to fetch data: {response.status_code}')

    @task()
    def transform_task(weather_data, province_name, latitude, longitude):
        current_weather = weather_data['hourly']
        transformed_data = []
        
        for hour in range(len(current_weather['time'])):
            transformed_data.append({
                'province': province_name,
                'latitude': latitude,
                'longitude': longitude,
                'time': current_weather['time'][hour],  # Add time column
                'temperature_2m': current_weather['temperature_2m'][hour],
                'relative_humidity_2m': current_weather['relative_humidity_2m'][hour],
                'rain': current_weather['rain'][hour],
                'wind_speed_10m': current_weather['wind_speed_10m'][hour],
                'wind_gusts_10m': current_weather['wind_gusts_10m'][hour],
                'visibility': current_weather['visibility'][hour],
                'dew_point_2m': current_weather['dew_point_2m'][hour],
                'wind_direction_10m': current_weather['wind_direction_10m'][hour],
                'cloud_cover': current_weather['cloud_cover'][hour],
            })
        return transformed_data  # Returns a list of dictionaries

    @task()
    def load_task(transformed_data):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            province TEXT,
            latitude FLOAT,
            longitude FLOAT,
            time TIMESTAMP,
            temperature_2m FLOAT,
            relative_humidity_2m FLOAT,
            rain FLOAT,
            wind_speed_10m FLOAT,
            wind_gusts_10m FLOAT,
            visibility FLOAT,
            dew_point_2m FLOAT,
            wind_direction_10m FLOAT,
            cloud_cover FLOAT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        for row in transformed_data:
            cursor.execute("""
            INSERT INTO weather_data (province, latitude, longitude, time, temperature_2m, relative_humidity_2m, rain, wind_speed_10m, wind_gusts_10m, visibility, dew_point_2m, wind_direction_10m, cloud_cover)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                row['province'],
                row['latitude'],
                row['longitude'],
                row['time'],  # Insert time column
                row['temperature_2m'],
                row['relative_humidity_2m'],
                row['rain'],
                row['wind_speed_10m'],
                row['wind_gusts_10m'],
                row['visibility'],
                row['dew_point_2m'],
                row['wind_direction_10m'],
                row['cloud_cover']
            ))

        conn.commit()
        cursor.close()

    # Dynamically create tasks for each province
    for province in provinces:
        extracted_data = extract_task(province['Latitude'], province['Longitude'])
        transformed_data = transform_task(extracted_data, province['Province'], province['Latitude'], province['Longitude'])
        load_task(transformed_data)
