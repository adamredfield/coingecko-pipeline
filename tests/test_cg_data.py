from src.cg_helpers import get_coin_market_data

base_url = 'https://api.coingecko.com/api/v3/'
coins_markets_url = '/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=1&sparkline=false'

market_data = get_coin_market_data(base_url, coins_markets_url)
print(type(market_data))

def test_market_data_schema():
    valid_schema = {'id','symbol','name','current_price','high_24h','low_24h','market_cap','total_volume','last_updated'}
    assert valid_schema.issubset(market_data[0].keys())
