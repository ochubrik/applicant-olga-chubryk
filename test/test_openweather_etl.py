import os
import unittest
from unittest.mock import patch, mock_open, MagicMock
import datetime
from etl_pipeline import main


class TestOpenWeatherETL(unittest.TestCase):
    def setUp(self):
        os.environ['OPENWEATHER_API_KEY'] = 'test_key'

    @patch("builtins.open", new_callable=mock_open, read_data="""city,state,country
Glasgow,Montana,US
Bismarck,North Dakota,US""")
    def test_read_cities(self, mock):
        result = main.read_cities("dummy.csv")

        self.assertEqual(result, [("Glasgow", "Montana", "US"), ("Bismarck", "North Dakota", "US")])

    @patch("etl_pipeline.main.requests.get")
    def test_get_coordinates_success(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [{"lat": 48.2, "lon": -106.6}]

        lat, lon = main.get_coordinates("Glasgow", "Montana", "US")

        self.assertEqual((lat, lon), (48.2, -106.6))

    @patch("etl_pipeline.main.requests.get")
    def test_get_coordinates_failure(self, mock_get):
        mock_get.return_value.status_code = 404
        mock_get.return_value.json.return_value = []

        lat, lon = main.get_coordinates("Nowhere", "ZZ", "US")

        self.assertIsNone(lat)
        self.assertIsNone(lon)

    def test_get_unix_timestamp(self):
        date = datetime.date(2024, 1, 1)
        expected_ts = int(datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc).timestamp())

        self.assertEqual(main.get_unix_timestamp(date), expected_ts)

    @patch("etl_pipeline.main.requests.get")
    def test_get_historical_weather_success(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "data": [
                {"temp": 123, "humidity": 50, "weather": [{"main": "Rain"}]}
            ]
        }

        result = main.get_historical_weather(48.2, -106.6, 1704067200)

        self.assertEqual(result[0]["temp"], 123)

    @patch("etl_pipeline.main.requests.get")
    def test_get_historical_weather_failure(self, mock_get):
        mock_get.return_value.status_code = 500
        mock_get.return_value.text = "Internal Server Error"

        result = main.get_historical_weather(0, 0, 0)

        self.assertEqual(result, [])

    @patch("etl_pipeline.main.get_coordinates", return_value=(48.2, -106.6))
    @patch("etl_pipeline.main.get_historical_weather")
    def test_process_weather_for_date_valid(self, mock_weather, mock_get_coordinates):
        mock_weather.return_value = [
            {"temp": 2, "humidity": 60, "weather": [{"main": "Rain"}]},
            {"temp": 4, "humidity": 65, "weather": [{"main": "Rain"}]},
            {"temp": 6, "humidity": 55, "weather": [{"main": "Cloudy"}]},
        ]

        result = main.process_weather_for_date("Glasgow", "Montana", "US", datetime.date(2024, 1, 1))

        self.assertEqual(result["avg_temp"], 4.0)
        self.assertEqual(result["avg_humidity"], 60.0)
        self.assertEqual(result["dominant_weather"], "Rain")

    @patch("etl_pipeline.main.get_coordinates", return_value=(None, None))
    def test_process_weather_for_date_no_coords(self, mock_get_coordinates):
        result = main.process_weather_for_date("Unknown", "ZZ", "US", datetime.date(2024, 1, 1))

        self.assertIsNone(result)

    @patch("etl_pipeline.main.bigquery.Client")
    @patch("etl_pipeline.main.read_cities", return_value=[("Glasgow", "Montana", "US")])
    @patch("etl_pipeline.main.process_weather_for_date")
    def test_run_yesterday_weather_to_bigquery(self, mock_process, mock_read, mock_bq_client):
        mock_process.return_value = {"city": "Glasgow", "state": "Montana", "country": "US", "date": "2024-01-01",
                                     "avg_temp": 3.0, "min_temp": 2.0, "max_temp": 4.0, "avg_humidity": 60.0,
                                     "dominant_weather": "Rain"}
        mock_client = MagicMock()
        mock_client.insert_rows_json.return_value = []

        mock_bq_client.return_value = mock_client

        result = main.run_yesterday_weather_to_bigquery(None)

        self.assertIn("Inserted", result)
