import time
import requests


def fetch_with_retry(url, params, auth=None, headers=None, retries=3, delay=2):
    for i in range(retries):
        response = requests.get(url, params=params, auth=auth, headers=headers)
        if response.status_code == 200:
            return response.json()
        elif response.status_code in [429, 500, 502, 503]:
            time.sleep(delay**i)  # Exponential backoff
        else:
            raise Exception(
                f"Failed to fetch data after {retries} retries: {response.status_code}"
            )
    return None
