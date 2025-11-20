"""
Example AWS Lambda function that fetches weather information for a city.

The function delegates the actual HTTP request to ``WeatherService`` so that
the service can be mocked in unit tests.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional
from urllib import error, parse, request


class WeatherService:
    """Simple wrapper around an imaginary weather API."""

    base_url = "https://api.example.com/weather"

    def fetch_temperature(self, city: str) -> float:
        """Fetch temperature for the provided city."""
        encoded_city = parse.quote_plus(city.strip())
        url = f"{self.base_url}?city={encoded_city}"
        try:
            with request.urlopen(url, timeout=5) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except error.URLError as exc:
            raise RuntimeError("Could not reach weather service") from exc

        if "temperature" not in payload:
            raise RuntimeError("Weather service response missing temperature field")

        return float(payload["temperature"])


def lambda_handler(
    event: Optional[Dict[str, Any]],
    context: Any,
    weather_service: Optional[WeatherService] = None,
) -> Dict[str, Any]:
    """
    AWS Lambda handler that returns the temperature for the requested city.
    """
    weather_service = weather_service or WeatherService()

    if not isinstance(event, dict) or not event.get("city"):
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "City is required"}),
        }

    city = str(event["city"]).strip()
    if not city:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "City is required"}),
        }

    try:
        temperature = weather_service.fetch_temperature(city)
    except RuntimeError as exc:
        return {
            "statusCode": 502,
            "body": json.dumps({"message": str(exc)}),
        }

    return {
        "statusCode": 200,
        "body": json.dumps({"city": city, "temperature": temperature}),
    }


__all__ = ["WeatherService", "lambda_handler"]
