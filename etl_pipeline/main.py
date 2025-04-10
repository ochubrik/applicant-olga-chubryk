import argparse
import csv
import datetime
import logging
import os
import time
from collections import defaultdict
from typing import List, Tuple, Optional, Dict, Any

import requests
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)

API_KEY = os.environ.get("OPENWEATHER_API_KEY")
API_SLEEP_TIME = 1

GEOCODE_URL = "https://api.openweathermap.org/geo/1.0/direct"
HISTORICAL_URL = "https://api.openweathermap.org/data/3.0/onecall/timemachine"

CITIES_FILE = "cities.csv"
HISTORICAL_OUTPUT_FILE = "jan1_7_2024.csv"
YESTERDAY_OUTPUT_FILE = "yesterday_forecast.csv"
TABLE_ID = "open-weather-project.weather_data.oc_weather_data_forecast"

HISTORICAL_START_DATE = datetime.date(2024, 1, 1)
HISTORICAL_END_DATE = datetime.date(2024, 1, 7)


def read_cities(file_path: str) -> List[Tuple[str, str, str]]:
    cities = []
    with open(file_path, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            cities.append((row["city"], row["state"], row["country"]))
    logger.info(f"Loaded cities: {cities}")
    return cities


def get_coordinates(city: str, state: str, country: str) -> Tuple[Optional[float], Optional[float]]:
    location = f"{city},{state},{country}" if state else f"{city},{country}"
    params = {
        "q": location,
        "limit": 1,
        "appid": API_KEY
    }
    resp = requests.get(GEOCODE_URL, params=params)
    if resp.status_code != 200 or not resp.json():
        logger.warning(f"Failed to get coordinates for {city}")
        return None, None
    data = resp.json()[0]
    logger.info(f"Coordinates for {city}: ({data['lat']}, {data['lon']})")
    return data["lat"], data["lon"]


def get_unix_timestamp(date_obj: datetime.date) -> int:
    """as OpenWeather API requires Unix timestamp in UTC"""
    return int(datetime.datetime.combine(date_obj, datetime.time.min, tzinfo=datetime.timezone.utc).timestamp())


def get_historical_weather(lat: float, lon: float, unix_timestamp: int) -> List[Dict[str, Any]]:
    params = {
        "lat": lat,
        "lon": lon,
        "dt": unix_timestamp,
        "units": "metric",
        "appid": API_KEY
    }
    resp = requests.get(HISTORICAL_URL, params=params)
    if resp.status_code != 200:
        logger.error(f"Error fetching weather data: {resp.status_code}, {resp.text}")
        return []

    try:
        data = resp.json()
    except Exception as e:
        logger.exception("Failed to parse API response")
        return []

    return data.get("data", [])


def process_weather_for_date(city: str, state: str, country: str, date: datetime.date) -> Optional[Dict[str, Any]]:
    lat, lon = get_coordinates(city, state, country)
    if lat is None or lon is None:
        return None

    unix_ts = get_unix_timestamp(date)
    weather_data = get_historical_weather(lat, lon, unix_ts)
    if not weather_data:
        logger.info(f"No weather data for {city} on {date}")
        return None

    temps, humidities, weather_counts = [], [], defaultdict(int)

    for entry in weather_data:
        temps.append(entry.get("temp"))
        humidities.append(entry.get("humidity"))
        if "weather" in entry and entry["weather"]:
            weather_main = entry["weather"][0]["main"]
            weather_counts[weather_main] += 1

    if not temps or not humidities:
        return None

    return {
        "city": city,
        "state": state,
        "country": country,
        "date": date.isoformat(),
        "avg_temp": round(sum(temps) / len(temps), 2),
        "min_temp": round(min(temps), 2),
        "max_temp": round(max(temps), 2),
        "avg_humidity": round(sum(humidities) / len(humidities), 2),
        "dominant_weather": next(iter(weather_counts), "")
    }


def run_yesterday_weather_to_bigquery(request) -> str:
    """
    cloud function - entry point
    """

    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    client = bigquery.Client()

    delete_query = f"""
        DELETE FROM `{TABLE_ID}`
        WHERE date = '{yesterday.isoformat()}'
    """

    logger.info(f"Deleting existing rows for {yesterday} from {TABLE_ID}")
    client.query(delete_query).result()

    cities = read_cities(CITIES_FILE)
    rows_to_insert = []
    for city, state, country in cities:
        result = process_weather_for_date(city, state, country, yesterday)
        if result:
            rows_to_insert.append(result)

    if len(rows_to_insert) > 0:
        errors = client.insert_rows_json(TABLE_ID, rows_to_insert)
        if errors:
            logger.error(f"BigQuery insert errors: {errors}")
            return "BigQuery insert failed"

        logger.info(f"Inserted {len(rows_to_insert)} rows into {TABLE_ID}")
        return f"Inserted {len(rows_to_insert)} rows into {TABLE_ID}"
    else:
        logger.info("No data to insert")
        return "No data to insert"


def local_main() -> None:
    """
    local script for testing - daily/historical
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["daily", "historical"], required=True)
    args = parser.parse_args()

    if not API_KEY:
        logger.error("Env OPENWEATHER_API_KEY is missing.")
        return

    if args.mode == "historical":
        start_date = HISTORICAL_START_DATE
        end_date = HISTORICAL_END_DATE
        output_file = HISTORICAL_OUTPUT_FILE
    else:
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        start_date = end_date = yesterday
        output_file = YESTERDAY_OUTPUT_FILE

    with open(output_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=[
            "city", "state", "country", "date", "avg_temp", "min_temp", "max_temp",
            "avg_humidity", "dominant_weather"
        ])
        writer.writeheader()

        cities = read_cities(CITIES_FILE)

        for city, state, country in cities:
            for delta in range((end_date - start_date).days + 1):
                current_date = start_date + datetime.timedelta(days=delta)
                result = process_weather_for_date(city, state, country, current_date)
                if result:
                    writer.writerow(result)
                    logger.info(f"{city} - {current_date}")

                # to prevent rate limit by time
                time.sleep(API_SLEEP_TIME)

    logger.info(f"Saved to {output_file}")


if __name__ == "__main__":
    local_main()
