
from cg_api import CoinGeckoAPI
import requests 

base_url = 'https://api.coingecko.com/api/v3/'
coin_list_url = 'coins/list'
coin_price_url = 'simple/price'

def get_coin_ids(data: list[dict]) -> list:   
    coin_symbols = [coin['id'] for coin in data]
    return coin_symbols

def coin_price_params(coin_id: str) -> str:
    return f'?ids={coin_id}&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true'