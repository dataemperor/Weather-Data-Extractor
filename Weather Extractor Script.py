import requests, json
from datetime import datetime, timedelta

API_FILE_PATH = "OpenWeatherMap Api Key.txt"
REQUEST_COUNT_FILE_PATH = "Request Count.json"


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


API_key = open(API_FILE_PATH, "r").read()
root_url = "http://api.openweathermap.org/data/2.5/weather?"

location = "Colombo"
url = f"{root_url}appid={API_key}&q={location}"

request = requests.get(url)
increment_request_count()
data = request.json()

if data["cod"] == 200:
    # Getting weather variables from the json data
    temperature = data["main"]["temp"] 
    humidity = data["main"]["humidity"]
    pressure = data["main"]["pressure"]
    wind_speed = data["wind"]["speed"]
    weather_description = data["weather"][0]["description"]


