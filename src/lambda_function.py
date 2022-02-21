from cg_helpers import get_coin_market_data, response_to_df, lower_df_column, deduper, market_data_schema_check, add_date_fields
from rds_config import rds_engine
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BASE_URL = 'https://api.coingecko.com/api/v3/'
COIN_MARKET_URL = '/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=1&sparkline=false'
COLUMNS_TO_INCLUDE = [
    'id',
    'symbol',
    'name',
    'current_price',
    'high_24h',
    'low_24h',
    'market_cap',
    'total_volume',
    'last_updated']

DF_COLUMN_MAPPER = {
    # cg  # db
    'id': 'coin_id',
    'symbol': 'coin_symbol',
    'name': 'coin_name',
    'current_price': 'price_at_ingestion',
    'high_24h': 'high_24h_price',
    'low_24h': 'low_24h_price',
    'last_updated': 'cg_last_updated'
}

if __name__ == "__main__":
    # Retrieve json response
    coin_market_data = get_coin_market_data(BASE_URL, COIN_MARKET_URL)
    # Check that response has required fields
    market_data_schema_check(coin_market_data, COLUMNS_TO_INCLUDE)
    coin_market_data_df = response_to_df(coin_market_data, COLUMNS_TO_INCLUDE)
    coin_market_data_df = add_date_fields(coin_market_data_df).rename(columns=DF_COLUMN_MAPPER)
    coin_market_data_df_ = lower_df_column(coin_market_data_df, ['coin_id', 'coin_symbol']).sort_values('coin_id')
    # dedupe on coin_id and cg_date so that we ensure only one coin insert
    # into db / day
    deduped_coin_market_data_df = deduper(coin_market_data_df, subset=['coin_id', 'cg_date'], grouped_column='coin_id')
    deduped_coin_market_data_df.to_sql(
        name='market_data',
        con=rds_engine("coingecko_data"),
        if_exists='append',
        index=False)
    logger.info(
        f'Successfully inserted {deduped_coin_market_data_df.shape[0]} records into the db.')
