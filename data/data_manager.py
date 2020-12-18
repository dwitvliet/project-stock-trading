import time
import datetime
import functools
import logging

import pandas as pd

import config
from data.database import Database
from data.api_manager import APIManager
from utils import descriptive_stats


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


def get_open_hours(exchange, date):
    """ Determine open operating hours of exchange for date

    """
    holidays = dict(db.get_holidays(exchange))
    half_day = (date in holidays and holidays[date] == 'half')
    start_time = datetime.time(9, 30)
    close_time = datetime.time(13 if half_day else 16, 0)

    return start_time, close_time


def _dates_missing_from_database(ticker, date_from, date_to, data_type):
    exchange = exchange_for_ticker(ticker)
    dates_with_trades = get_open_dates(exchange, date_from, date_to)

    if data_type in ('trades', 'quotes'):
        dates_stored = db.get_stored_dates(data_type, ticker)
    else:
        dates_stored = db.get_stored_dates_for_feature(ticker, data_type)

    return [d for d in dates_with_trades if d not in dates_stored]


def download_trades(ticker, date_from, date_to, data_type='trades',
                    verbose=False):

    dates_to_fetch = _dates_missing_from_database(
        ticker, date_from, date_to, data_type
    )

    if len(dates_to_fetch) == 0:
        if verbose:
            logging.info(
                f'All day(s) of {data_type} from {date_from} to {date_to} are'
                'already stored.'
            )
        return

    logging.info(
        f'Fetching {len(dates_to_fetch)} day(s) of {ticker} {data_type}.'
    )
    for date in dates_to_fetch:
        # Download trades.
        time_before_fetch = time.time()
        trades = api.get_daily_trades(ticker, date, data_type)

        # Store trades in database.
        time_before_store = time.time()
        db.store_trades(ticker, date, trades, data_type)

        logging.info(
            f'{ticker} {date} - '
            f'fetch: {int(round(time_before_store - time_before_fetch))}s, '
            f'store: {int(round(time.time() - time_before_store))}s'
        )


def generate_and_store_feature(ticker, feature_name, date_from, date_to,
                               generate_feature_func, *args, **kwargs):

    dates_to_generate = _dates_missing_from_database(
        ticker, date_from, date_to, feature_name
    )

    logging.info(
        f'Generating {len(dates_to_generate)} day(s) of feature {feature_name} '
        f'from {date_from} to {date_to} for ticker {ticker}'
    )

    for date in dates_to_generate:
        series = generate_feature_func(ticker, date, *args, **kwargs)

        # Ensure no accidentally left in NaNs.
        nan_counts = series.isna().sum()
        assert nan_counts == 0, (
            f'Feature {feature_name} for {ticker} has {nan_counts} NaN values.'
        )

        db.store_feature(ticker, feature_name, series)


@functools.lru_cache(maxsize=10)
def get_trades(ticker, date_from, date_to=None, data_type='trades'):
    """ Get all trades for a range of dates.

    Fetches the dates from the database. If they do not exist, they are first
    fetched from the API and stored.

    Args:
        ticker (str): Ticker symbol.
        date_from (Date): First date in range to get.
        date_to (Date, optional): Last date in range to get.
        data_type (str, 'trades' or 'quotes'): The type or data to fetch.

    Returns:
        pd.DataFrame
    """

    if date_to is None:
        date_to = date_from

    download_trades(ticker, date_from, date_to, data_type)

    exchange = exchange_for_ticker(ticker)
    dates_with_trades = get_open_dates(exchange, date_from, date_to)
    if len(dates_with_trades) == 0:
        logging.info(f'There are no {data_type} for the selected date(s).')
        return

    trades = []
    for date in dates_with_trades:
        trades.append(db.get_trades(ticker, date, data_type))

    return pd.concat(trades)


@functools.lru_cache(maxsize=10)
def get_quotes(ticker, date_from, date_to=None):
    quotes = get_trades(ticker, date_from, date_to, data_type='quotes')
    quotes['spread'] = quotes['ask_price'] - quotes['bid_price']
    return quotes


@functools.lru_cache(maxsize=10)
def get_bars(ticker, date, agg='mean', data_type='trades', smooth_periods=1):
    if data_type == 'trades':
        trades = get_trades(ticker, date)
    else:
        trades = get_quotes(ticker, date)

    grouper = pd.Grouper(key='time', freq='1S')
    if agg == 'weighted_mean':
        bars = descriptive_stats.weighted_mean(
            trades, values='price', weights='volume', groupby=grouper
        )
    else:
        bars = trades.groupby(grouper).agg(agg)

    if agg in ('weighted_mean', 'mean', 'median'):
        bars = bars.fillna(method='ffill')
    elif agg in ('sum', 'min', 'max', 'std'):
        bars = bars.fillna(0)

    if smooth_periods > 1:
        bars = bars.rolling(smooth_periods).mean()

    open_time, close_time = get_open_hours(exchange_for_ticker(ticker), date)
    bars = bars.reindex(pd.date_range(
        datetime.datetime.combine(date, open_time),
        datetime.datetime.combine(date, close_time),
        freq='1S',
        closed='left'
    ))

    return bars
