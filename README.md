## Important notes

Some cities in the original dataset lacked associated states. Since many US cities share the same name but are located in different states,\
I manually added states (selecting the most well-known city with that name via Google search) to eliminate ambiguity during data collection and future analysis.\
This step was also necessary because the `bigquery-public-data.geo_us_boundaries` dataset associates identical geolocations with cities of the same name across different states,\
which makes geolocation-based analysis potentially unreliable.

## Overview

This solution is designed to be simple, clear, and maintainable while efficiently handling the task.\
To keep the script lightweight and easy to understand, a straightforward procedural approach is used instead of a complex class-based (OOP) structure, which would be overkill for this task.

Appropriate GCP services used:
* Cloud Functions - Triggers the OpenWeather API and loads results into BigQuery.
* Cloud Scheduler - Executes the Cloud Function on a daily schedule.
* BigQuery - Stores weather data for analysis and querying.

## For local testing
### Create and use Virtual Env

```shell
python3 -m venv .venv
source .venv/bin/activate
```

### Install packages

```shell
python3 -m pip install -r etl_pipeline/requirements.txt
```

### Export API key

```shell
OPENWEATHER_API_KEY='YOUR_API_KEY'
```

1 - for historical data, run 
```shell
cd etl_pipeline
python3 main.py --mode historical
```

2 - for yesterday data, run 
```shell
cd etl_pipeline
python3 main.py --mode daily
```

## PyTests

To execute tests, run:

```shell
pytest test/
```

## GCP Deployment
For installing google cloud sdk
https://cloud.google.com/sdk/docs/install

1. Set Up BigQuery Dataset
```shell
gcloud config set project open-weather-project
bq mk --dataset open-weather-project:weather_data
```

Upload Historical Data (One-time Step)

```shell
bq load \
  --autodetect \
  --source_format=CSV \
  open-weather-project:weather_data.oc_weather_data_historical \
  _evidence/jan1_7_2024.csv
```

2. Enable Required Services
```shell
gcloud services enable artifactregistry.googleapis.com
gcloud services enable cloudbuild.googleapis.com
```

3. Configure IAM for Cloud Build
```shell
gcloud projects describe open-weather-project --format="value(projectNumber)"
```
Then:
```shell
gcloud projects add-iam-policy-binding open-weather-project \
  --member="serviceAccount:PROJECT_NUMBER@cloudbuild.gserviceaccount.com" \
  --role="roles/cloudbuild.builds.editor"

gcloud projects add-iam-policy-binding open-weather-project \
  --member="serviceAccount:PROJECT_NUMBER@cloudbuild.gserviceaccount.com" \
  --role="roles/iam.serviceAccountUser"
 ```
Replace PROJECT_NUMBER with the actual number.

4. Deploy the Cloud Function
```shell
cd etl_pipeline
gcloud functions deploy oc_weather_loader \
  --runtime python310 \
  --trigger-http \
  --entry-point run_yesterday_weather_to_bigquery \
  --allow-unauthenticated \
  --set-env-vars OPENWEATHER_API_KEY='YOUR_API_KEY'
 ```

5. Schedule Daily Execution with Cloud Scheduler
```shell
gcloud scheduler jobs create http weather-daily-job \
  --schedule "0 6 * * *" \
  --time-zone "UTC" \
  --http-method GET \
  --uri https://us-central1-open-weather-project.cloudfunctions.net/oc_weather_loader \
  --location us-central1
 ```
Optional: Trigger the Job Immediately
```shell
gcloud scheduler jobs run weather-daily-job --location us-central1
```


## Future improvements:
* Secret Manager - Secure storage for the OpenWeather API key.
* Apache Airflow - For orchestration and better visibility into data workflows.
* Docker - To standardise environments, support CI/CD, and simplify deployment to Cloud Run or Kubernetes.
* BigQuery Table Partitioning - Improve query performance and cost-efficiency by partitioning by date.
* Monitoring and Alerting - Add logging, error tracking, and automated alerts for better reliability.
