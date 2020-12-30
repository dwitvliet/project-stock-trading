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


@functools.lru_cache(maxsize=5)
def get_open_dates(ticker, date_from, date_to, exclude_future=True):
    """
    Get list of dates within range where exchange is open. Weekends and
    holidays are excluded.

    Args:
        ticker (str): ticker symbol
        date_from (date|str): first date in range (inclusive)
        date_to (date|str): last date in range (inclusive)
        exclude_future (bool, optional): whether to exclude today and future
            dates.
    Returns:
        [date, ..]
    """

    exchange = exchange_for_ticker(ticker)
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
        if exclude_future and (datetime.datetime.now() - date).days <= 0:
            continue
        open_dates.append(date.date())

    return open_dates


@functools.lru_cache(maxsize=5)
def get_trading_hours(ticker, date, extended_hours=False):
    """ Determine open operating hours of exchange for date

    """
    holidays = dict(db.get_holidays(exchange_for_ticker(ticker)))
    half_day = (date in holidays and holidays[date] == 'half')
    if not extended_hours:
        open_time = datetime.time(9, 30)
        close_time = datetime.time(13 if half_day else 16, 0)
    else:
        open_time = datetime.time(4, 0)
        close_time = datetime.time(17 if half_day else 20, 0)

    return open_time, close_time


@functools.lru_cache(maxsize=5)
def get_trading_hours_index(ticker, date, extended_hours=False):

    open_time, close_time = get_trading_hours(
        ticker, date, extended_hours
    )
    return pd.date_range(
        datetime.datetime.combine(date, open_time),
        datetime.datetime.combine(date, close_time),
        freq='1S'
    )


def dates_missing_from_database(ticker, date_from, date_to, data_type):
    dates_with_trades = get_open_dates(ticker, date_from, date_to)

    if data_type in ('trades', 'quotes'):
        dates_stored = db.get_stored_dates(data_type, ticker)
    else:  # features
        dates_stored = db.get_stored_dates_for_feature(ticker, data_type)

    return [d for d in dates_with_trades if d not in dates_stored]


def download_trades(ticker, date_from, date_to, data_type='trades',
                    verbose=False):

    dates_to_fetch = dates_missing_from_database(
        ticker, date_from, date_to, data_type
    )

    if len(dates_to_fetch) == 0:
        if verbose:
            logging.info(
                f'All days of {data_type} from {date_from} to {date_to} are'
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


@functools.lru_cache(maxsize=5)
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

    dates_with_trades = get_open_dates(ticker, date_from, date_to)
    if len(dates_with_trades) == 0:
        logging.info(f'There are no {data_type} for the selected date(s).')
        return

    trades = []
    for date in dates_with_trades:
        trades.append(db.get_trades(ticker, date, data_type))

    return pd.concat(trades)


@functools.lru_cache(maxsize=5)
def get_quotes(ticker, date_from, date_to=None):
    quotes = get_trades(ticker, date_from, date_to, data_type='quotes')
    quotes['spread'] = quotes['ask_price'] - quotes['bid_price']
    return quotes


@functools.lru_cache(maxsize=20)
def get_bars(ticker, date, agg='mean', data_type='trades', smooth_periods=1,
             extended_hours=False, fillna=False):

    if type(date) == str:
        date = datetime.datetime.strptime(date, '%Y-%m-%d').date()

    if data_type == 'trades':
        trades = get_trades(ticker, date)
        trades['dollar_volume'] = trades['volume'] * trades['price']
    else:
        trades = get_quotes(ticker, date)

    # Aggregate by the second. Shift after aggregation for each bar to represent
    # what happened leading up to the time point (rather than after it).
    grouper = pd.Grouper(key='time', freq='1S')
    if agg == 'weighted_mean':
        bars = descriptive_stats.weighted_mean(
            trades[['price', 'volume', 'time']],
            values='price', weights='volume', groupby=grouper
        )
    else:
        bars = trades.groupby(grouper).agg(agg)
    bars = bars.shift(1)

    if fillna:
        bars = bars.fillna(method='ffill')

    if smooth_periods > 1:
        bars = bars.rolling(smooth_periods).mean()

    # Restrict time to tradings hours. Includes the opening time, which
    # represents trading data during one second of pre-market trading (will be
    # NaN if extended hours are requested).
    bars = bars.reindex(get_trading_hours_index(ticker, date, extended_hours))

    return bars
