"""The DAG for the Weather Data Extractor"""
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
import boto3
import os

# File paths to the API key and request count
API_FILE_PATH = "/home/dataemperor/projects/Weather-Data-Extractor/OpenWeatherMap Api Key.txt"
REQUEST_COUNT_FILE_PATH = "Request Count.json"

# For OpenWeatherMap
ROOT_URL = "http://api.openweathermap.org/data/2.5/weather?"
LOCATION = "Colombo"

# Config for AWS/Localstack
BUCKET_NAME = "open-weather-map-jayathu"
AWS_REGION = "ap-southeast-1"
LOCALSTACK_ENDPOINT = "http://localhost:4566"
s3 = boto3.client("s3", region_name=AWS_REGION,
                  endpoint_url=LOCALSTACK_ENDPOINT,
                  aws_access_key_id='test', aws_secret_access_key='test')

"""
Variables used to test how long each task takes
Used to record the starting and ending times of a task
NOTE:
    A value of -1 indicates that the variable was either
    Not Reached
    or modifying the value was a failure
"""
fetching_start, fetching_end = -1, -1
transform_start, transform_end = -1, -1
upload_start, upload_end = -1, -1

# used to indicate whether a task has succeeded or not
fetch_success, transform_success, upload_success = False, False, False


def load_request_data():
    """
    Loading request count data
    WARNING: If cloned from remote, create your own file with an API key
    """
    try:
        with open(REQUEST_COUNT_FILE_PATH, "r") as openFile:
            return json.load(openFile)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {"Request Count": 0,
                "Last Reset": datetime.utcnow().isoformat()}
        save_request_data(data)
        return data


def save_request_data(data):
    """
    Saving request data into the specified file
    """
    with open(REQUEST_COUNT_FILE_PATH, "w") as openFile:
        json.dump(data, openFile, indent=4)


def increment_request_count():
    """
    Incrementing the request count by one
    If it's been 24 hours or more since last reset, reset count
    """
    data = load_request_data()
    last_reset_time = datetime.fromisoformat(data["Last Reset"])
    now = datetime.utcnow()
    hours_passed = now - last_reset_time

    if hours_passed > timedelta(hours=24):
        data["Request Count"] = 0
        data["Last Reset"] = now.isoformat()

    data["Request Count"] += 1
    save_request_data(data)


def fetch_weather_data(**kwargs):
    """
    Fetching weather data from OpenWeatherMap
    WARNING: API key and file must be configured correctly
    """
    # removing the /n character that is read from
    # the text file containing the api key
    API_KEY = open(API_FILE_PATH, "r").read().strip()
    url = f"{ROOT_URL}appid={API_KEY}&q={LOCATION}"

    # sending a request to the API
    response = requests.get(url)
    increment_request_count()
    data = response.json()

    if data["cod"] == 200:
        kwargs['ti'].xcom_push(key="weather_data", value=data)
    else:
        raise Exception("The request to OpenWeatherMap API has failed," +
                        "most likely an issue with the API Key or what" +
                        "is being sent as the API key")


def transform_data(**kwargs):
    """
    Transforming the data that's been fetched into a dictionary
    """
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(key="weather_data", task_ids="fetch_weather_data")

    transformed_data = {
        "location": raw_data["name"],
        "temperature": raw_data["main"]["temp"],
        "humidity": raw_data["main"]["humidity"],
        "timestamp": datetime.utcnow().isoformat(),
    }

    ti.xcom_push(key="transformed_data", value=transformed_data)


def upload_to_s3(**kwargs):
    """
    Uploading dictionary with transformed data into an s3 bucket as a json
    """
    ti = kwargs['ti']
    transformed_data_upload = ti.xcom_pull(key="transformed_data",
                                           task_ids="transform_data")

    OBJECT_KEY = (
        f"transformed-weather-data-{datetime.utcnow().isoformat()}.json"
    )
    json_string = json.dumps(transformed_data_upload, indent=4)
    try:
        s3.put_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY,
                      Body=json_string, ContentType="application/json")
        print(f"Uploaded file to S3: {OBJECT_KEY}")
    except Exception as e:
        print(f"Upload failed: {e}")


default_args = {
    'owner': 'Jayathu Fernando',
    'start_date': datetime(2025, 3, 29),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG('weather_data_dag',
         default_args=default_args,
         description='OpenWeather Data Extractor DAG',
         # runs every hour
         schedule='0 * * * *',
         # doesn't make up for any intervals between start_date and deployment
         catchup=False,
         tags=['weather', 'OpenWeatherMap', 'OpenWeather',
               'Open Weather Map', 'Open Weather', 'open weather map',
               'open weather']
         ) as weather_data_dag:
    fetch_weather_data_task = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=fetch_weather_data)

    transform_data_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data)

    upload_to_s3_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3)

# dependencies
fetch_weather_data_task >> transform_data_task >> upload_to_s3_task
