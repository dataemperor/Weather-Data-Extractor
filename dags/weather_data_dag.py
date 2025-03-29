from datetime import timedelta, datetime 
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Jayathu Fernando',
    'start_date': datetime(2025, 3, 29),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

weather_extraction_dag = DAG('weather_data_dag',
                             default_args = default_args,
                             description = 'OpenWeather Data Extractor DAG',
                             # runs every hour
                             schedule = '0 * * * *',
                             # doesn't make up for any intervals between start_date and deployment
                             catchup = False,
                             tags = ['weather', 'OpenWeatherMap', 'OpenWeather', 'Open Weather Map', 'Open Weather', 'open weather map', 'open weather'])
