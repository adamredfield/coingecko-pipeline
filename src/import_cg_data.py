from cg_api import CoinGeckoAPI
from cg_helpers import get_coin_ids, coin_price_params

base_url = 'https://api.coingecko.com/api/v3/'
coin_list_url = 'coins/list'
coin_price_url = 'simple/price'

coin_data = CoinGeckoAPI(base_url,coin_list_url).call_cg_api()
coin_ids = get_coin_ids(coin_data)
for id in coin_ids[:20]:
    coin_price = CoinGeckoAPI(base_url,coin_price_url).call_cg_api(coin_price_params(id))
    print(coin_price)