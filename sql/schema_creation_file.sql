CREATE TABLE `open-weather-project.weather_data.oc_weather_data_forecast` (
  city STRING,
  state STRING,
  country STRING,
  date DATE,
  avg_temp FLOAT64,
  min_temp FLOAT64,
  max_temp FLOAT64,
  avg_humidity FLOAT64,
  dominant_weather STRING
);

CREATE TABLE `open-weather-project.weather_data.oc_weather_data_historical` (
  city STRING,
  state STRING,
  country STRING,
  date DATE,
  avg_temp FLOAT64,
  min_temp FLOAT64,
  max_temp FLOAT64,
  avg_humidity FLOAT64,
  dominant_weather STRING
);
