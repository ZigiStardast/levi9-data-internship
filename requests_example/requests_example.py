import requests

def example_basic_get():
    url = "https://httpbin.org/get"
    response = requests.get(url)

    print("Status:", response.status_code)
    print("Body:", response.json())

def example_get_with_params():
    url = "https://httpbin.org/get"
    params = {"city": "Belgrade", "units": "metric"}

    response = requests.get(url, params=params)

    print("Final URL:", response.url)
    print("Response JSON:", response.json())

def example_post_json():
    url = "https://httpbin.org/post"
    payload = {"username": "tara", "role": "data_engineer"}

    response = requests.post(url, json=payload)

    print("Status:", response.status_code)
    print("Sent JSON:", response.json()["json"])

def example_with_error_handling():
    url = "https://this-domain-does-not-exist-12345.com"

    try:
        response = requests.get(url, timeout=3)
        print(response.text)
    except requests.exceptions.RequestException as exc:
        print("API request failed:", exc)

if __name__ == "__main__":
    print("=== Basic GET ===")
    example_basic_get()

    print("\n=== GET with params ===")
    example_get_with_params()

    print("\n=== POST JSON ===")
    example_post_json()

    print("\n=== Error handling ===")
    example_with_error_handling()
