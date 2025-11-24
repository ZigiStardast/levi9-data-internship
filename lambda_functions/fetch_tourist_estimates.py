import argparse
import json
import os
from datetime import datetime
from typing import Any, Dict, Optional

import boto3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_URL = "https://rq5fbome43vbdgq7xoe7d6wbwa0ngkgr.lambda-url.eu-west-1.on.aws/"
DEFAULT_SECRET_REGION = "eu-west-1"
DEFAULT_SECRET_NAME = "tourist_estimate_token"
SECRET_REGION = os.getenv("SECRET_REGION", DEFAULT_SECRET_REGION)
SECRET_NAME = os.getenv("SECRET_NAME", DEFAULT_SECRET_NAME)
API_TIMEOUT_SECONDS = 10
_token_cache: Optional[str] = None
_session: Optional[requests.Session] = None


def get_session() -> requests.Session:
    """Return a reusable requests session with basic retries configured."""
    global _session
    if _session is None:
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            backoff_factor=0.5,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        s = requests.Session()
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _session = s
    return _session


def get_token() -> str:
    """Fetch and cache the bearer token from Secrets Manager."""
    global _token_cache
    if _token_cache:
        return _token_cache

    try:
        client = boto3.client("secretsmanager", region_name=SECRET_REGION)
        response = client.get_secret_value(SecretId=SECRET_NAME)
        secret_value = response["SecretString"]
    except Exception as exc:
        raise RuntimeError(f"Failed to read secret '{SECRET_NAME}' in region '{SECRET_REGION}'") from exc

    _token_cache = secret_value
    return secret_value


def validate_date(date_str: str) -> str:
    """Validate date format YYYY-MM-DD and return the normalized string."""
    try:
        parsed = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("date must be in YYYY-MM-DD format") from exc
    return parsed.strftime("%Y-%m-%d")


def fetch_estimates(date_str: str) -> Dict[str, Any]:
    date = validate_date(date_str)
    token = get_token()

    headers = {"Authorization": f"Bearer {token}"}
    params = {"date": date}

    response = get_session().get(API_URL, headers=headers, params=params, timeout=API_TIMEOUT_SECONDS)

    if response.status_code != 200:
        raise RuntimeError(f"API error {response.status_code}: {response.text}")

    return response.json()


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch tourist estimates for a given date.")
    parser.add_argument("date", help="Date in YYYY-MM-DD format")
    args = parser.parse_args()

    data = fetch_estimates(args.date)
    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
