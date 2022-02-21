from ratelimit import limits, sleep_and_retry
import logging
import pandas as pd
import requests
import smtplib
from datetime import datetime
from typing import Dict, List
from rds_config import get_secret
from email.message import EmailMessage
from datetime import datetime


logger = logging.getLogger()
logging.basicConfig(filename=f'data{datetime.now()}.log', encoding='utf-8')
logger.setLevel(logging.INFO)

base_url = 'https://api.coingecko.com/api/v3/'
coins_markets_url = '/coins/markets?vs_currency=usd&order=id_asc&per_page=250&page=1&sparkline=false'

ONE_MINUTE = 60  # seconds


class CoinGeckoAPI:

    def __init__(self, cg_base_url: str, cg_endpoint: str) -> None:
        self.cg_base_url = cg_base_url
        self.cg_endpoint = cg_endpoint

    def call_cg_api(self, params='') -> Dict:
        response = requests.get(
            f'{self.cg_base_url}{self.cg_endpoint}{params}')
        if response.status_code != 200:
            raise Exception(response.raise_for_status)
        return response.json()


@sleep_and_retry
@limits(calls=40, period=ONE_MINUTE)
def get_coin_market_data(base_url: str, endpoint: str) -> List[Dict]:
    """
    Retrieves paginated json response from the CoinGecko API. https://www.coingecko.com/en/api/documentation
    The API allows for 50 calls/minute but can vary. The limit decorator will limit API calls to 40 calls/minute.
    """

    coin_market_data = []  # type: List[Dict]
    page = 1
    while True:
        coin_data = CoinGeckoAPI(base_url, endpoint).call_cg_api(
            params=f'?vs_currency=usd&order=market_cap_desc&per_page=250&page={page}&sparkline=false')
        if len(coin_data) == 0:
            break
        coin_market_data.extend(coin_data)
        page += 1
    logger.info(
        f'Retrieved {len(coin_market_data)} records from {page} pages of data')
    return coin_market_data


def response_to_df(response: list[dict], column_list: list):
    """
    Converts json api response to a dataframe object.
    Pass in a list of keys from the response to retain in dataframe conversion.

    Returns
    -------
    pd.DataFrame
        Data columns are as follows:

        ========================================================================
        id              (as `Object(str)`)
        symbol          (as `Object(str)`)
        name            (as `Object(str)`)
        current_price   (as `float64`)
        high_24h        (as `float64`)
        low_24h         (as `float64`)
        market_cap      (as `float64`)
        total_volume    (as `float64`)
        last_updated    (as `object(str)`)
        ========================================================================
    """

    df = pd.json_normalize(response)
    df = df[column_list]
    return df


def add_date_fields(df):
    df['insert_time'] = datetime.utcnow()
    df['cg_date'] = datetime.utcnow().date()
    return df


def lower_df_column(df, column_names):
    """
    Sets specified dataframe columns to lowercase.
    This may be useful for analyzing data from different sources.
    Pass in a list of column names for the 'column_names' arg to be converted to lowercase.
    """

    for column in column_names:
        df[column] = df[column].str.lower()
    return df


def deduper(df, subset, grouped_column):
    """
    Identifies duplicates from the api response and returns a de-duped dataframe.
    The removed duplicates are logged in descending order by how many duplicates were removed.
    Args:
    grouped_column (str): a string specifying which column you would like to group by and display in the logs.
    subset (list): a list of columns specifying which columns you want to dedupe on
    """

    pd.set_option('display.max_rows', None)
    dupes = df.duplicated(subset=subset)
    logger.info(f'Duplicates found: {dupes.sum()}')
    duped_df = df.loc[dupes]
    dupe_counts_df = duped_df.groupby(grouped_column).size().reset_index(
        name='counts').sort_values('counts', ascending=False)
    df.drop_duplicates(subset=subset, keep='first', inplace=True)
    logger.info(f'{df.shape[0]} records remaining after de-duping.')
    logger.info(f'# duplicates removed by: {grouped_column}')
    logger.info(dupe_counts_df)
    return df


def market_data_schema_check(market_data, columns):
    # Sends out an email alert if schema changes from what is expected.
    gmail_user = get_secret()["gmail_un"]
    gmail_pw = get_secret()["gmail_pw"]
    valid_schema = set(columns)
    if not valid_schema.issubset(market_data[0].keys()):
        missing_keys = [key for key in valid_schema if key not in market_data[0].keys()]
        msg = EmailMessage()
        msg.set_content(
            f'The CoinGecko API is not giving the expected response. The following keys are missing: {missing_keys}. Look into this here: https://www.coingecko.com/en/api/documentation')

        msg['Subject'] = 'CoinGecko API Schema Alert!'
        msg['From'] = gmail_user
        msg['To'] = gmail_user
        try:
            smtp_server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
            smtp_server.login(gmail_user, gmail_pw)
            smtp_server.send_message(msg)
            smtp_server.close()
            logger.info("Email sent successfully!")
        except Exception as ex:
            logger.error("Something went wrong….", ex)
