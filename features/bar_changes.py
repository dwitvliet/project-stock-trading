import datetime

import numpy as np
import pandas as pd

import data.data_manager as data
from features import bar_properties


def current_bar_compared_to_rolling(ticker, date, _):
    """ Price and volume compared to a rolling average of previous periods.

    The relative changes in the mean, min, max, and std of the price and volume
    compared to a rolling average. The rolling average stretches from 3 seconds
    to the beginning of the day (but currently not any previous days).

    """

    bars = bar_properties.current_bar(ticker, date)
    trading_hours = data.get_trading_hours_index(ticker, date)
    df = pd.DataFrame(index=bars.index)

    # Calculate relative to rolling averages. For all measures except the price,
    # the absolute difference is calculated instead of the relative difference
    # to prevent infinite values.
    measures = (
        'price', 'price_min_relative', 'price_max_relative', 'price_std_relative',
        'volume', 'volume_mean', 'volume_min', 'volume_max', 'volume_std',
        'count',
    )
    windows = (
        '1S', '3S', '5S', '10S', '30S',
        '1min', '3min', '5min', '10min', '30min', '1H', '1D'
    )
    for i in windows:
        rolling = bars.shift().rolling(i, min_periods=1)
        if i == '1D':
            rolling = bars.shift().reindex(trading_hours).rolling(i, min_periods=1)
        for measure in measures:
            if measure == 'price':
                df[f'{i}_{measure}'] = bars[measure] / rolling[measure].mean() - 1
            else:
                df[f'{i}_{measure}'] = bars[measure] - rolling[measure].mean()

    return df.reindex(trading_hours)


def current_bar_compared_to_high_and_low(ticker, date, _):
    """ Price compared to the previous high and low.

    The relative change in the mean, min, and max price compared to the high and
    low of a previous time window, stretching from a minute to the beginning of
    the day (currently not any previous days).

    """

    bars = bar_properties.current_bar(ticker, date)
    trading_hours = data.get_trading_hours_index(ticker, date)
    df = pd.DataFrame(index=bars.index)

    # Calculate relative to time high and low.
    measures = ('price', 'price_min', 'price_max')
    windows = ('1min', '3min', '5min', '10min', '30min', '1H', '1D')
    for i in windows:
        rolling = bars.shift().rolling(i, min_periods=1)
        if i == '1D':
            rolling = bars.shift().reindex(trading_hours).rolling(i, min_periods=1)
        for measure in measures:
            df[f'{i}_low_{measure}'] = (
                bars[measure] / rolling['price'].min() - 1
            )
            df[f'{i}_high_{measure}'] = (
                bars[measure] / rolling['price'].max() - 1
            )

    return df.reindex(trading_hours)


def current_bar_compared_to_open(ticker, date, _):
    """ Price compared to the beginning of the previous bars.

    The relative change in the mean, min, and max price compared to the
    beginning of the minute, hour, day, and a few refrequencies in between.

    """

    bars = bar_properties.current_bar(ticker, date)
    trading_hours = data.get_trading_hours_index(ticker, date)
    df = pd.DataFrame(index=bars.index)

    # Calculate relative to opening of minutes/hour/day.
    measures = ('price', 'price_min', 'price_max')
    last_opens = ('1min', '5min', '10min', '30min', '1H', '1D')
    for i in last_opens:
        price = bars['price'].copy()
        price[~price.index.isin(pd.date_range(
            datetime.datetime.combine(date, datetime.time(9, 30)),
            datetime.datetime.combine(date, datetime.time(16, 0)),
            freq=i
        ))] = np.nan
        price = price.fillna(method='ffill')
        for measure in measures:
            df[f'open_{i}_{measure}'] = bars[measure] / price - 1

    return df.reindex(trading_hours)


def recent_bars_compared_to_current(ticker, date, params):
    """ Price and volume of recent aggregate bars.

    The price (including min, max, and std) and volume (including mean, min,
    max, and std) of a number of recent bars normalized to now.

    Params:
        "periods_to_go_back" (int): The number of periods into the past to use
            as features.

    """

    periods_to_go_back = params.get('periods_to_go_back', 60)

    bars = bar_properties.current_bar(ticker, date)
    trading_hours = data.get_trading_hours_index(ticker, date)

    dfs = []
    measures = [
        'price_min_relative', 'price_max_relative', 'price_std_relative',
        'volume', 'volume_min', 'volume_max', 'volume_mean', 'volume_std'
    ]
    for i in range(1, periods_to_go_back+1):
        df = bars[measures] - bars[measures].shift(i)
        df['price'] = bars['price'].pct_change(i)
        dfs.append(df.add_suffix(f'_{i}S_ago_vs_now'))

    return pd.concat(dfs, axis=1, sort=False, copy=False).reindex(trading_hours)


def recent_bars_compared_to_preceding(ticker, date, params):
    """ Price and volume of recent aggregate bars compared to the one before it.

    The price (including min, max, and std) and volume (including mean, min,
    max, and std) of a number of recent bars compared to the bar preceding it.

    Params:
        "periods_to_go_back" (int): The number of periods into the past to use
            as features.
    """

    periods_to_go_back = params.get('periods_to_go_back', 60)

    bars = bar_properties.current_bar(ticker, date)
    trading_hours = data.get_trading_hours_index(ticker, date)

    dfs = []

    bar_changes = pd.DataFrame(index=bars.index)
    bar_changes['price'] = bars['price'].pct_change()
    measures = [
        'price_min_relative', 'price_max_relative', 'price_std_relative',
        'volume', 'volume_min', 'volume_max', 'volume_mean', 'volume_std'
    ]
    for measure in measures:
        bar_changes[measure] = bars[measure].diff()
    for i in range(1, periods_to_go_back):
        dfs.append(bar_changes.shift(i).add_suffix(f'_{i}S_ago_vs_{i-1}S ago'))

    return pd.concat(dfs, axis=1, sort=False, copy=False).reindex(trading_hours)


def proportion_of_increasing_bars(ticker, date, _):
    """ Proportion of recent bars that increased.

    The proportion of aggregate bars in a time window that increased in price,
    count, or volume (each a separate features). The time window spans from 1
    second to the beginning of the day.

    """

    bars = bar_properties.current_bar(ticker, date)
    trading_hours = data.get_trading_hours_index(ticker, date)
    df = pd.DataFrame(index=bars.index)

    # Increase or decrease.
    measures = ('price', 'count', 'volume')
    for measure in measures:
        df[f'{measure}_inc_sign'] = np.sign(bars[measure].diff())

    # Proportion of increases in the last seconds/minutes.
    measures = ('price', 'count', 'volume')
    windows = (
        '3S', '5S', '10S', '30S',
        '1min', '3min', '5min', '10min', '30min', '1H', '1D'
    )
    for i in windows:
        rolling = df.eq(1).rolling(i, min_periods=1)
        if i == '1D':
            rolling = df.eq(1).reindex(trading_hours).rolling(i, min_periods=1)
        for measure in measures:
            column = f'{measure}_inc_sign'
            df[f'{i}_{column}'] = (
                rolling[column].sum() / rolling[column].count()
            )

    return df.reindex(trading_hours)


def consecutive_of_increasing_bars(ticker, date, _):
    """ The number of consequtive bars that increased.

    The number of consecutive bars into the past than increased in price, count,
    or volume after applying a moving average smoothing. The time window spans
    from 1 second to 30 minutes.

    """

    bars = bar_properties.current_bar(ticker, date)
    trading_hours = data.get_trading_hours_index(ticker, date)
    df = pd.DataFrame(index=bars.index)

    # How much time since last decreased.
    measures = ('price', 'count', 'volume')
    windows = (
        '1S', '3S', '5S', '10S', '30S',
        '1min', '3min', '5min', '10min', '30min'
    )
    for i in windows:
        rolling = bars.rolling(i, min_periods=1).mean()
        for measure in measures:
            signs = np.sign(rolling[measure].diff())
            df[f'{i}_{measure}_since_down'] = signs.eq(1).groupby(
                (signs != signs.shift()).cumsum()
            ).transform('cumsum')

    return df.reindex(trading_hours)
