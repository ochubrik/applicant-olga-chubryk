### All the queries have 2 variants: one that uses bigquery-public-data.geo_us_boundaries for enhanced geographic matching, and one that doesn't — to account for cities with the same name across different states.

Please find the answers in .csv files. 

### Provide the average daily temperature for each city in each state.
Analysis_1.csv
```
SELECT
  oc.city,
  oc.state,
  ROUND(AVG(oc.avg_temp), 2) AS avg_daily_temperature
FROM
  `open-weather-project.weather_data.oc_weather_data_historical` oc
GROUP BY
  oc.city, oc.state
ORDER BY
  oc.state, avg_daily_temperature DESC;
```
Analysis_1_1.csv
```
SELECT
  bq.state_name,
  TRIM(REGEXP_REPLACE(bq.city, r'(?i)\s*city$', '')) AS city,
  ROUND(AVG(oc.avg_temp), 2) AS avg_daily_temperature
FROM
  `open-weather-project.weather_data.oc_weather_data_historical` oc
JOIN
  `bigquery-public-data.geo_us_boundaries.zip_codes` bq
ON
  LOWER(oc.city) = LOWER(TRIM(REGEXP_REPLACE(bq.city, r'(?i)\s*city$', '')))
GROUP BY
  bq.state_name, city
ORDER BY
  bq.state_name, avg_daily_temperature DESC;

```

### Find the top 3 cities with the highest average humidity in each state.
Analysis_2.csv
```
SELECT 
    state,
    city,
    avg_humidity
FROM (
  SELECT
    oc.state,
    oc.city,
    ROUND(AVG(oc.avg_humidity), 2) AS avg_humidity,
    RANK() OVER (PARTITION BY oc.state ORDER BY AVG(oc.avg_humidity) DESC) AS rank
  FROM
    `open-weather-project.weather_data.oc_weather_data_historical` oc
  GROUP BY
    oc.state, oc.city
)
WHERE rank <= 3
ORDER BY state, rank;
```
Analysis_2_1.csv
```
WITH cleaned_geo AS (
  SELECT
    state_name,
    TRIM(REGEXP_REPLACE(city, r'(?i)\s*city$', '')) AS city_cleaned
  FROM
    `bigquery-public-data.geo_us_boundaries.zip_codes`
  GROUP BY
    state_name, city_cleaned
)
SELECT 
    state,
    city,
    avg_humidity
FROM (
  SELECT
    oc.state,
    oc.city,
    ROUND(AVG(oc.avg_humidity), 2) AS avg_humidity,
    RANK() OVER (PARTITION BY oc.state ORDER BY AVG(oc.avg_humidity) DESC) AS rank
  FROM
    `open-weather-project.weather_data.oc_weather_data_historical` oc
  JOIN
    cleaned_geo bq
  ON
    LOWER(oc.city) = LOWER(bq.city_cleaned)
    AND LOWER(oc.state) = LOWER(bq.state_name)
  GROUP BY
    oc.state, oc.city
)
WHERE rank <= 3
ORDER BY state, rank;
```

### Find the percentage of cities in each state experiencing "rain" as the weather condition.
Analysis_3.csv
```
WITH total_cities_per_state AS (
  SELECT
    state,
    COUNT(DISTINCT city) AS total_cities
  FROM
    `open-weather-project.weather_data.oc_weather_data_historical`
  GROUP BY
    state
),
rain_cities_per_state AS (
  SELECT
    state,
    COUNT(DISTINCT city) AS rain_cities
  FROM
    `open-weather-project.weather_data.oc_weather_data_historical`
  WHERE
    LOWER(dominant_weather) = 'rain'
  GROUP BY
    state
)

SELECT
  t.state,
  t.total_cities,
  IFNULL(r.rain_cities, 0) AS rain_cities,
  ROUND(IFNULL(r.rain_cities, 0) * 100.0 / t.total_cities, 2) AS rain_city_percentage
FROM
  total_cities_per_state t
LEFT JOIN
  rain_cities_per_state r
ON
  t.state = r.state
ORDER BY
  rain_city_percentage DESC;
```
Analysis_3_1.csv
```
WITH cleaned_geo AS (
  SELECT
    state_name,
    TRIM(REGEXP_REPLACE(city, r'(?i)\s*city$', '')) AS city_cleaned
  FROM
    `bigquery-public-data.geo_us_boundaries.zip_codes`
  GROUP BY
    state_name, city_cleaned
),

weather_with_geo AS (
  SELECT DISTINCT
    oc.city,
    oc.state,
    oc.dominant_weather
  FROM
    `open-weather-project.weather_data.oc_weather_data_historical` oc
  JOIN
    cleaned_geo bq
  ON
    LOWER(oc.city) = LOWER(bq.city_cleaned)
    AND LOWER(oc.state) = LOWER(bq.state_name)
),

total_cities AS (
  SELECT
    state,
    COUNT(DISTINCT city) AS total_cities
  FROM
    weather_with_geo
  GROUP BY
    state
),

rain_cities AS (
  SELECT
    state,
    COUNT(DISTINCT city) AS rain_cities
  FROM
    weather_with_geo
  WHERE
    LOWER(dominant_weather) = 'rain'
  GROUP BY
    state
)

SELECT
  t.state,
  t.total_cities,
  IFNULL(r.rain_cities, 0) AS rain_cities,
  ROUND(IFNULL(r.rain_cities, 0) * 100.0 / t.total_cities, 2) AS rain_city_percentage
FROM
  total_cities t
LEFT JOIN
  rain_cities r
ON
  t.state = r.state
ORDER BY
  rain_city_percentage DESC;
```
