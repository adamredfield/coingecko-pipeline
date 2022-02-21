from ratelimit import limits, sleep_and_retry
import logging
import pandas as pd
import requests
from datetime import datetime


logger = logging.getLogger()
logging.basicConfig(filename=f'data{datetime.now()}.log', encoding='utf-8')
logger.setLevel(logging.INFO)

base_url = 'https://api.coingecko.com/api/v3/'
coins_markets_url = '/coins/markets?vs_currency=usd&order=id_asc&per_page=250&page=1&sparkline=false'

ONE_MINUTE = 60  # seconds


class CoinGeckoAPI:

    def __init__(self, cg_base_url, cg_endpoint):
        self.cg_base_url = cg_base_url
        self.cg_endpoint = cg_endpoint

    def call_cg_api(self, params=''):
        response = requests.get(
            f'{self.cg_base_url}{self.cg_endpoint}{params}')
        if response.status_code != 200:
            raise Exception(response.raise_for_status)
        return response.json()


@sleep_and_retry
@limits(calls=40, period=ONE_MINUTE)
def get_coin_market_data(base_url, endpoint):
    """
    Retrieves paginated json response from the CoinGecko API. https://www.coingecko.com/en/api/documentation
    The API allows for 50 calls/minute but can vary. The limit decorator will limit API calls to 40 calls/minute.
    """

    coin_market_data = []
    coin_data = True
    page = 1
    while coin_data:
        coin_data = CoinGeckoAPI(base_url, endpoint).call_cg_api(
            params=f'?vs_currency=usd&order=market_cap_desc&per_page=250&page={page}&sparkline=false')
        coin_market_data.extend(coin_data)
        page += 1
    logger.info(
        f'Retrieved {len(coin_market_data)} records from {page} pages of data')
    return coin_market_data


def coin_market_data_to_df(response, list_of_columns):
    """
    Converts json api response to a dataframe object.
    Pass in a list of keys from the response to be converted to dataframe columns.
    """

    market_data_df = pd.json_normalize(response)
    market_data_df = market_data_df[list_of_columns]
    return market_data_df


def lower_df_column(df, column_names):
    """
    Sets specified dataframe columns to lowercase.
    This may be useful for analyzing data from different sources.
    Pass in a list of column names for the 'column_names' arg to be converted to lowercase.
    """

    for column in column_names:
        df[column] = df[column].str.lower()
    return df


def deduper(df, subset, sorted_column):
    """
    Identifies duplicates from the api response and returns a de-duped dataframe.
    The removed duplicates are logged in descending order by how many duplicates were removed.
    Args:
    sorted_column (str): a string specifying which column you would like to display in the logs.
    subset (list): a list of columns specifying which columns you want to dedupe on
    """

    dupes = df.duplicated(subset=subset)
    logger.info(f'duplicates found: {dupes.sum()}')
    duped_df = df.loc[dupes]
    dupe_counts_df = duped_df.groupby(sorted_column).size().reset_index(
        name='counts').sort_values('counts', ascending=False)
    logger.info(
        f'dupes removed, grouped by the {sorted_column} column: {dupe_counts_df}')
    df.drop_duplicates(subset=subset, keep='first', inplace=True)
    logger.info(f'{df.shape[0]} records remaing after de-duping.')
    return df
