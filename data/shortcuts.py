import time
import datetime
import functools
import logging

import pandas as pd

import config
from data.database import Database
from data.api_manager import APIManager


db = Database(config.database)
api = APIManager(config.api_key)


@functools.lru_cache(maxsize=None)
def exchange_for_ticker(ticker):
    ticker_details = db.get_ticker_details(ticker)
    if ticker_details is None:
        ticker_details = db.store_ticker_details(api.get_ticker_details(ticker))
    return ticker_details['exchange']


@functools.lru_cache(maxsize=None)
def get_open_dates(exchange, date_from, date_to):
    """
    Get list of dates within range where exchange is open. Weekends and
    holidays are excluded.

    Args:
        exchange (str): exchange symbol
        date_from (date|str): first date in range (inclusive)
        date_to (date|str): last date in range (inclusive)
    Returns:
        [date, ..]
    """

    open_dates = []

    # Get holidays.
    holidays = db.get_holidays(exchange, date_from, date_to)
    holidays = [date for date, hours in holidays if hours == 'closed']

    for date in pd.date_range(date_from, date_to):
        # Skip Saturdays and Sundays.
        if date.weekday() >= 5:
            continue
        # Skip holidays where the exchange is closed.
        if date in holidays:
            continue
        # Skip today and future days.
        if (datetime.datetime.now() - date).days <= 0:
            continue
        open_dates.append(date.date())

    return open_dates


def fetch_and_store_trades(ticker, date_from, date_to):
    exchange = exchange_for_ticker(ticker)
    dates_with_trades = get_open_dates(exchange, date_from, date_to)
    dates_stored = db.get_stored_dates('trades', ticker)

    dates_to_fetch = [d for d in dates_with_trades if d not in dates_stored]

    logging.info(f'Fetching {len(dates_to_fetch)} day(s) of {ticker} trades.')
    for date in dates_to_fetch:
        # Download trades.
        time_before_fetch = time.time()
        trades = api.get_daily_trades(ticker, date)

        # Store trades in database.
        time_before_store = time.time()
        db.store_trades(ticker, date, trades)

        logging.info(
            f'{ticker} {date} - '
            f'fetch time: {int(round(time_before_store - time_before_fetch))}s, '
            f'store time: {int(round(time.time() - time_before_store))}s'
        )

