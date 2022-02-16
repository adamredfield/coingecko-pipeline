import requests

class CoinGeckoAPI:

    def __init__(self, cg_base_url, cg_endpoint) -> None:
        self.cg_base_url = cg_base_url
        self.cg_endpoint = cg_endpoint

    def call_cg_api(self,params=''):
        response = requests.get(f'{self.cg_base_url}{self.cg_endpoint}{params}')
        if response.status_code != 200:
            raise Exception(response.raise_for_status)
        return response.json()
