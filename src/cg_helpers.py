def get_coin_ids(data: list[dict]) -> list:   
    coin_symbols = [coin['id'] for coin in data]
    return coin_symbols

def coin_price_params(coin_id: str) -> str:
    return f'?ids={coin_id}&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true'