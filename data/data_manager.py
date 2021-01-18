import time
import datetime
import functools
import logging

import pandas as pd

import config
from data.database import Database
from data.api_manager import APIManager
from utils import descriptive_stats


db = Database(config.database, store_features_as_pickle=True)
api = APIManager(config.api_key)


@functools.lru_cache(maxsize=None)
def exchange_for_ticker(ticker):
    """ Get the exchange where a ticker is traded.

    Args:
        ticker (str): Ticker symbol, e.g. 'AAPL'.

    Returns:
        str: Exchange symbol, e.g. 'NGS' for Nasdaq.

    """
    ticker_details = db.get_ticker_details(ticker)
    # Populate the ticker details in the database if they do not already exist.
    if ticker_details is None:
        ticker_details = db.store_ticker_details(api.get_ticker_details(ticker))
    return ticker_details['exchange']


@functools.lru_cache(maxsize=5)
def get_open_dates(ticker, date_from, date_to, exclude_future=True):
    """ Get list of dates within range where exchange is open.

    Args:
        ticker (str): Ticker symbol.
        date_from (date|str): First date in range (inclusive).
        date_to (date|str): Last date in range (inclusive).
        exclude_future (bool, optional): Whether to exclude today and future
            dates.
    Returns:
        [datetime.date, ..]
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
    """ Determine trading hours for a ticker on a date.

    Args:
        ticker (str): Ticker symbol.
        date (date|str): Date to get hours for.
        extended_hours (bool): Whether to included pre- and post-market hours.
            If `False`, only regular trading hours are considered (9:30 AM -
            4:00 PM on most days).
    Returns:
        (datetime.time, datetime.time): (open_time, close_time)

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
def get_trading_hours_index(ticker, date, extended_hours=False, freq='1S'):
    """ Get index spanning trading hours for a ticker.

    Is useful for building dataframes where each row is an aggregate statistic
    over an interval spanning the entire day (e.g. the mean volume of trades
    every second).

    Args:
        ticker (str): Ticker symbol.
        date (date|str): Date to get hours for.
        extended_hours (bool): Whether to included pre- and post-market hours.
        freq (str, optional): The interval frequency to use to generate the
            time interval.

    Returns:
        DatetimeIndex

    """

    open_time, close_time = get_trading_hours(
        ticker, date, extended_hours
    )
    return pd.date_range(
        datetime.datetime.combine(date, open_time),
        datetime.datetime.combine(date, close_time),
        freq=freq
    )


def dates_missing_from_database(ticker, date_from, date_to, data_type):
    """ Get dates where trades/quotes/feature has not been stored in the database.

    Args:
        ticker (str): Ticker symbol.
        date_from (date|str): First date in range (inclusive).
        date_to (date|str): Last date in range (inclusive).
        data_type (str): What database table to check. May be 'trades', 'quotes',
            or the name of a feature.

    Returns:
        [datetime.date, ..]

    """

    dates_with_trades = get_open_dates(ticker, date_from, date_to)

    if data_type in ('trades', 'quotes'):
        dates_stored = db.get_stored_dates(data_type, ticker)
    else:  # features
        dates_stored = db.get_stored_dates_for_feature(ticker, data_type)

    return [d for d in dates_with_trades if d not in dates_stored]


def download_trades(ticker, date_from, date_to, data_type='trades',
                    verbose=False):
    """ Download missing trades from the API and store in database.

    Args:
        ticker (str): Ticker symbol.
        date_from (date|str): First date in range (inclusive).
        date_to (date|str): Last date in range (inclusive).
        data_type (str, optional): Whether to download 'trades' or 'quotes'.
        verbose (bool, optional): If `True`, logs the time and duration of each
            days worth of trades downloaded.

    """

    # Check which dates are not already downloaded and stored to the database.
    dates_to_fetch = dates_missing_from_database(
        ticker, date_from, date_to, data_type
    )

    # Return if everything is already fetched.
    if len(dates_to_fetch) == 0:
        if verbose:
            logging.info(
                f'All days of {data_type} from {date_from} to {date_to} are '
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

    # Download any missing trades to the database.
    download_trades(ticker, date_from, date_to, data_type)

    # Determine which dates have trades.
    dates_with_trades = get_open_dates(ticker, date_from, date_to)
    if len(dates_with_trades) == 0:
        logging.info(f'There are no {data_type} for the selected date(s).')
        return

    # Fetch all trades for the date range.
    trades = []
    for date in dates_with_trades:
        trades.append(db.get_trades(ticker, date, data_type))

    return pd.concat(trades)


@functools.lru_cache(maxsize=5)
def get_quotes(ticker, date_from, date_to=None):
    quotes = get_trades(ticker, date_from, date_to, data_type='quotes')
    quotes['spread'] = quotes['ask_price'] - quotes['bid_price']
    quotes.loc[quotes['spread'] < 0, 'spread'] = 0
    return quotes


@functools.lru_cache(maxsize=20)
def get_bars(ticker, date, data_type='trades', agg='mean', smooth_periods=1,
             freq='1S', extended_hours=False, fillna=False):
    """ Get aggregate bars for trades/quotes on a specific date.

    Group trades or qoutes by a time interval to generate aggregate bars of
    their price and volume.

    Args:
        ticker (str): Ticker symbol to generate bars for.
        date (datetime.date|str): Date to generate bars for.
        data_type (str, optional): The data type, 'trades' or 'quotes'.
        agg (str|func, optional): The aggregate function to call over the time
            period.
        smooth_periods (int, optional): How many periods to smooth the time
            periods after aggregation. Defaults to no smoothing.
        freq (str, optional): The frequency to group trades/quotes by.
        extended_hours (bool, optional): Whether to include trades/quotes that
            happened outside regular trading hours.
        fillna (bool, optional): Whether to fill empty values. If `True`, empty
            values are filled by the most recent values.

    Returns:
        pd.DataFrame: DataFrame with aggregate bars.

    """

    if type(date) == str:
        date = datetime.datetime.strptime(date, '%Y-%m-%d').date()

    if data_type == 'trades':
        trades = get_trades(ticker, date)
        trades['dollar_volume'] = trades['volume'] * trades['price']
    else:
        trades = get_quotes(ticker, date)

    # Group trades/quotes by the time frequency. Shift after aggregation for
    # each bar to represent what happened leading up to the time point (rather
    # than after it).
    grouper = pd.Grouper(key='time', freq=freq)
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
    bars = bars.reindex(get_trading_hours_index(ticker, date, extended_hours, freq))

    return bars


def get_prices(ticker, date):
    """ Get prices, defined as the weighted mean smoothed over 3 seconds """
    return get_bars(
        ticker, date, agg='weighted_mean', smooth_periods=3, fillna=True
    )


def get_features(ticker, date_from, date_to=None, feature_ids=None):
    """ Get all stored features over a range of dates.

    Args:
        ticker (str): Ticker symbol.
        date_from (Date): First date in range to get.
        date_to (Date, optional): Last date in range to get.
        feature_ids (list): List of features to filter the result down to.

    Returns:
        (pd.DataFrame, pd.Series): (Requested features, target value).

    """

    if date_to is None:
        date_to = date_from

    # Check that features have been generated for all dates in the range.
    open_dates = get_open_dates(ticker, date_from, date_to)
    dates_with_features = db.get_stored_dates_for_feature(ticker, '')
    assert len(set(open_dates) - set(dates_with_features)) == 0, (
        f'Features for {ticker} are not stored on all dates from {date_from} '
        f'to {date_to}.'
    )

    # Fetch all features and concat them into one large dataframe.
    dfs = []
    for date in open_dates:
        features = db.get_features(ticker, date)
        # Filter features, ensuring that the target variable is never removed.
        if feature_ids is not None:
            target_idx = features.columns[0]
            if target_idx not in feature_ids:
                feature_ids = feature_ids.insert(0, target_idx)
            features = features[feature_ids]
        dfs.append(features)
    df_final = pd.concat(dfs, axis=0, sort=False, copy=False)

    # Extract the target value from the features.
    X = df_final.iloc[:, 1:]
    y = df_final.iloc[:, 0]
    return X, y
