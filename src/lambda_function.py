from cg_helpers import get_coin_market_data, coin_market_data_to_df, lower_df_column, deduper
from rds_config import rds_engine
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

base_url = 'https://api.coingecko.com/api/v3/'
coins_markets_url = '/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=1&sparkline=false'

if __name__ == "__main__":
    coin_market_data = get_coin_market_data(base_url, coins_markets_url)
    coin_market_data_df = coin_market_data_to_df(
        coin_market_data,
        [
            'id',
            'symbol',
            'name',
            'current_price',
            'high_24h',
            'low_24h',
            'market_cap',
            'total_volume',
            'last_updated'])
    coin_market_data_df = lower_df_column(
        coin_market_data_df, [
            'id', 'symbol']).sort_values('id')
    deduped_coin_market_data_df = deduper(
        coin_market_data_df, subset=[
            'id', 'high_24h'], sorted_column='id')
    deduped_coin_market_data_df.to_sql(
        name='market_data',
        con=rds_engine("coingecko_data"),
        if_exists='append',
        index=False)
