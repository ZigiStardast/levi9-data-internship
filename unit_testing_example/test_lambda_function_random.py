import json
import unittest
from unittest.mock import Mock, patch

from unit_testing_example.lambda_function_random import WeatherService, lambda_handler


class TestLambdaFunction(unittest.TestCase):
    def test_lambda_handler_success(self):
        mock_service = Mock(spec=WeatherService)
        mock_service.fetch_temperature.return_value = 24.5

        event = {"city": "Belgrade"}
        response = lambda_handler(event, None, weather_service=mock_service)

        self.assertEqual(response["statusCode"], 200)
        payload = json.loads(response["body"])
        self.assertEqual(payload, {"city": "Belgrade", "temperature": 24.5})
        mock_service.fetch_temperature.assert_called_once_with("Belgrade")

    def test_lambda_handler_missing_city(self):
        response = lambda_handler({}, None)

        self.assertEqual(response["statusCode"], 400)
        payload = json.loads(response["body"])
        self.assertEqual(payload["message"], "City is required")

    def test_lambda_handler_handles_runtime_error(self):
        mock_service = Mock(spec=WeatherService)
        mock_service.fetch_temperature.side_effect = RuntimeError("Boom")

        response = lambda_handler({"city": "Novi Sad"}, None, weather_service=mock_service)

        self.assertEqual(response["statusCode"], 502)
        payload = json.loads(response["body"])
        self.assertEqual(payload["message"], "Boom")


class TestWeatherService(unittest.TestCase):
    def test_fetch_temperature_parses_response(self):
        fake_body = json.dumps({"temperature": 18}).encode("utf-8")

        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                return False

            def read(self):
                return fake_body

        with patch(
            "unit_testing_example.lambda_function_random.request.urlopen",
            return_value=FakeResponse(),
        ) as mock_urlopen:
            service = WeatherService()
            temperature = service.fetch_temperature("Novi Sad")

        self.assertEqual(temperature, 18.0)
        mock_urlopen.assert_called_once()


if __name__ == "__main__":
    unittest.main()
