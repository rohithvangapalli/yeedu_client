import requests

class YeeduClient:
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

    def _request(self, method, endpoint, **kwargs):
        url = f"{self.base_url}{endpoint}"
        resp = requests.request(method, url, headers=self.headers, **kwargs)
        if not resp.ok:
            raise Exception(f"API Error {resp.status_code}: {resp.text}")
        return resp.json() if resp.text else {}