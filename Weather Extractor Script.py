import requests, json, boto3
from datetime import datetime, timedelta

API_FILE_PATH = "OpenWeatherMap Api Key.txt"
REQUEST_COUNT_FILE_PATH = "Request Count.json"

BUCKET_NAME = "open-weather-map-jayathu"
OBJECT_KEY = f"transformed-weather-data-{datetime.utcnow().isoformat()}.json"
AWS_REGION = "ap-southeast-1" 

# Initializing the S3 client 
s3 = boto3.client("s3", region_name=AWS_REGION)

# Loading the request data and initializing the "Request Count" file if it isn't present
def load_request_data():
    try:
        with open(REQUEST_COUNT_FILE_PATH, "r") as openFile:
            data = json.load(openFile)

    # In case the "Request Count.json" file got corrupted or doesn't exist in the first place
    except (FileNotFoundError, json.JSONDecodeError):
        data = {"Request Count": 0, "Last Reset": datetime.utcnow().isoformat()}
        save_request_data(data)
    return data


# Updating the Request Count file with the new data
def save_request_data(data):
    with open(REQUEST_COUNT_FILE_PATH, "w") as openFile:
        json.dump(data, openFile, indent=4)


# Incrementing the Request Count
def increment_request_count():
    data = load_request_data()
    last_reset_time = datetime.fromisoformat(data["Last Reset"])
    now = datetime.utcnow()
    hours_passed = now - last_reset_time

    if hours_passed > timedelta(hours=24):
        data["Request Count"] = 0
        data["Last Reset"] = now.isoformat()

    data["Request Count"] += 1
    save_request_data(data)

# Transforming the data received by the API
def transform_data(raw_data):
    transformed_data = {
            "location": raw_data["name"],
            "temperature": raw_data["main"]["temp"],
            "humidity": raw_data["main"]["humidity"],
            "timestamp": datetime.utcnow().isoformat(),
            }

    return transformed_data

# Uploads the JSON data into the specified S3 bucket
def upload_to_bucket(data, bucket=BUCKET_NAME, key=OBJECT_KEY):
    json_string = json.dumps(data, indent=4)
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=json_string, ContentType="application/json")
    except:
        print("The file was not found")


API_key = open(API_FILE_PATH, "r").read().strip()
root_url = "http://api.openweathermap.org/data/2.5/weather?"

location = "Colombo"
url = f"{root_url}appid={API_key}&q={location}"

request = requests.get(url)
increment_request_count()
data = request.json()
# the if statement checks if the response from OpenWeatherMap API is successful
if data["cod"] == 200:
    # uploading the transformed data into a bucket
    upload_to_bucket(transform_data(data)) 
else:
    print("There must be something wrong with the API key")
