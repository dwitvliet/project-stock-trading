import datetime

import numpy as np
import pandas as pd

import data.data_manager as data
from feature import bar_properties


def current_bar_compared_to_rolling(ticker, date, _):
    bars = bar_properties.current_bar(ticker, date)
    trading_hours = data.get_trading_hours_index(ticker, date)
    df = pd.DataFrame(index=bars.index)

    # Calculate relative to rolling averages.
    measures = (
        'price', 'price_min', 'price_max', 'price_std', 'count',
        'volume', 'volume_mean', 'volume_min', 'volume_max', 'volume_std',
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
            df[f'{i}_{measure}'] = bars[measure] / rolling[measure].mean() - 1

    # Center standard deviation at 0.
    df[[c for c in df.columns if c.endswith('_std')]] += 1

    return df.reindex(trading_hours)


def current_bar_compared_high_and_low(ticker, date, _):
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
                bars[measure] / rolling['price_min'].min() - 1
            )
            df[f'{i}_high_{measure}'] = (
                bars[measure] / rolling['price_max'].max() - 1
            )

    return df.reindex(trading_hours)


def current_bar_compared_open(ticker, date, _):
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
    # For the most recent bars, determine price and volume changes compared to
    # now.
    periods_to_go_back = params.get('periods_to_go_back', 60)

    bars = bar_properties.current_bar(ticker, date)
    trading_hours = data.get_trading_hours_index(ticker, date)

    dfs = []
    measures = [
        'price_min_relative', 'price_max_relative', 'price_std_relative',
        'count', 'volume'
    ]
    for i in range(1, periods_to_go_back+1):
        df = bars[measures].shift(-i) - bars[measures]
        df['price'] = bars['price'].pct_change(-i)
        dfs.append(df.add_suffix(f'_{i}S_ago_vs_now'))

    return pd.concat(dfs, axis=1, sort=False, copy=False).reindex(trading_hours)


def recent_bars_compared_to_preceding(ticker, date, params):
    # For the most recent bars, determine price and volume changes compared to
    # the previous bar.
    periods_to_go_back = params.get('periods_to_go_back', 60)

    bars = bar_properties.current_bar(ticker, date)
    trading_hours = data.get_trading_hours_index(ticker, date)

    dfs = []

    bar_changes = pd.DataFrame(index=bars.index)
    bar_changes['price'] = bars['price'].pct_change()
    measures = [
        'price_min_relative', 'price_max_relative', 'price_std_relative',
        'count', 'volume'
    ]
    for measure in measures:
        bar_changes[measure] = bars[measure].diff()
    for i in range(1, periods_to_go_back):
        dfs.append(bar_changes.shift(i).add_suffix(f'_{i}S_ago_vs_{i-1}S ago'))

    return pd.concat(dfs, axis=1, sort=False, copy=False).reindex(trading_hours)


def recent_bars_up_or_down(ticker, date, _):
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
