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

df = pd.read_csv("/opt/airflow/dags/Sub_Data/vietnam_provinces.csv")  
provinces = df.to_dict(orient="records")  # Convert CSV data to list of dictionaries

with DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    description='Weather ETL pipeline with reduced tasks',
    schedule_interval='@daily',
    catchup=False
) as dag:

    @task()
    def extract_task(provinces):
        """ Fetch weather data for all provinces and return as a dictionary. """
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        raw_data = {}

        for province in provinces:
            latitude = province['Latitude']
            longitude = province['Longitude']
            
            endpoint = f'/v1/forecast?latitude={latitude}&longitude={longitude}&hourly=temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_gusts_10m,visibility,dew_point_2m,wind_direction_10m,cloud_cover&forecast_days=1'
            response = http_hook.run(endpoint)

            if response.status_code == 200:
                raw_data[province['Province']] = {
                    'latitude': latitude,
                    'longitude': longitude,
                    'data': response.json()['hourly']
                }
            else:
                raise Exception(f'Failed to fetch data for {province["Province"]}: {response.status_code}')

        return raw_data  # Dictionary containing data for all provinces

    @task()
    def transform_task(raw_data):
        """ Transform all extracted data into structured format. """
        transformed_data = []

        for province, weather_info in raw_data.items():
            latitude = weather_info['latitude']
            longitude = weather_info['longitude']
            weather_data = weather_info['data']

            for hour in range(len(weather_data['time'])):
                transformed_data.append({
                    'province': province,
                    'latitude': latitude,
                    'longitude': longitude,
                    'time': weather_data['time'][hour],
                    'temperature_2m': weather_data['temperature_2m'][hour],
                    'relative_humidity_2m': weather_data['relative_humidity_2m'][hour],
                    'precipitation': weather_data['precipitation'][hour],
                    'wind_speed_10m': weather_data['wind_speed_10m'][hour],
                    'wind_gusts_10m': weather_data['wind_gusts_10m'][hour],
                    'visibility': weather_data['visibility'][hour],
                    'dew_point_2m': weather_data['dew_point_2m'][hour],
                    'wind_direction_10m': weather_data['wind_direction_10m'][hour],
                    'cloud_cover': weather_data['cloud_cover'][hour],
                })

        return transformed_data  # List of transformed weather records

    @task()
    def load_task(transformed_data):
        """ Load transformed data into PostgreSQL. """
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
            precipitation FLOAT,
            wind_speed_10m FLOAT,
            wind_gusts_10m FLOAT,
            visibility FLOAT,
            dew_point_2m FLOAT,
            wind_direction_10m FLOAT,
            cloud_cover FLOAT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        insert_query = """
        INSERT INTO weather_data 
        (province, latitude, longitude, time, temperature_2m, relative_humidity_2m, precipitation, wind_speed_10m, wind_gusts_10m, visibility, dew_point_2m, wind_direction_10m, cloud_cover)
        VALUES %s;
        """

        # Convert list of dicts to list of tuples
        data_tuples = [
            (
                row['province'], row['latitude'], row['longitude'], row['time'],
                row['temperature_2m'], row['relative_humidity_2m'], row['precipitation'],
                row['wind_speed_10m'], row['wind_gusts_10m'], row['visibility'],
                row['dew_point_2m'], row['wind_direction_10m'], row['cloud_cover']
            )
            for row in transformed_data
        ]

        from psycopg2.extras import execute_values
        execute_values(cursor, insert_query, data_tuples)  # Bulk insert

        conn.commit()
        cursor.close()

    # Define task dependencies
    extracted_data = extract_task(provinces)
    transformed_data = transform_task(extracted_data)
    load_task(transformed_data)
