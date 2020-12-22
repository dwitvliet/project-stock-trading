import datetime

import numpy as np
import pandas as pd

import data.data_manager as data


def recent_trades(ticker, date, params):
    """ Get details of recent trades for each second during the selected date.

    For each timepoint, a number of the most recent trades are selected and
    the three properties of each trade are listed:
    - The price-weighted volume (volume * price).
    - The relative price difference compared to the most recent established
        price.
    - The time in nanoseconds since the trade happened.

    The top trades by price and volume and the bottom trades by price are
    selected and their properties are returned.

    Args:
        ticker (str): Ticker symbol.
        date (datetime.date): Date to label.
        params (dict):
            "num_of_trades" (int): The number of recent trades to summarize.
            "num_of_top_trades" (int): The number of top trades to use as
                features.

    Returns:
        pd.DataFrame

    """

    num_of_trades = params.get('num_of_trades', 100)
    num_of_top_trades = params.get('num_of_top_trades', 10)

    # Get all trades and price aggregate per second.
    trades = data.get_trades(ticker, date)
    bars = data.get_bars(
        ticker, date, agg='weighted_mean', smooth_periods=3, extended_hours=True
    )
    trade_hours_index = data.get_trading_hours_index(ticker, date)

    # Convert all data to numpy ndarrays outside loop for better performance.
    previous_price = bars.shift(1).reindex(trade_hours_index)
    trades = trades.sort_values('time', ascending=False)  # latest first
    trade_price_arr = trades['price'].to_numpy()
    trade_volume_arr = trades['volume'].to_numpy()
    trade_timestamp_arr = trades['time'].to_numpy(int)

    # Iterate all time points, selecting the attributes of the most recent
    # trades and summarizing them into a dataframe.
    recent_prices = np.full((len(trade_hours_index), num_of_trades), np.nan)
    recent_volumes = np.full((len(trade_hours_index), num_of_trades), np.nan)
    recent_times = np.full((len(trade_hours_index), num_of_trades), np.nan)
    for i, time in enumerate(trade_hours_index.astype(int)):
        first_idx = np.argmax(trade_timestamp_arr < time)
        last_idx = first_idx + num_of_trades

        price = trade_price_arr[first_idx:last_idx]
        recent_prices[i] = (price - previous_price[i]) / previous_price[i]
        recent_volumes[i] = trade_volume_arr[first_idx:last_idx] * price
        recent_times[i] = time - trade_timestamp_arr[first_idx:last_idx]

    # Sort recent trades by price and volume, selecting the top and bottom
    # trades.
    idx_high_price = np.fliplr(
        np.argsort(recent_prices, axis=1)
    )[:, :num_of_top_trades]
    idx_low_price = np.argsort(recent_prices, axis=1)[:, :num_of_top_trades]
    idx_volume = np.fliplr(
        np.argsort(recent_volumes, axis=1)
    )[:, :num_of_top_trades]

    features = [
        ('price_of_trade_with_{}_highest_price', recent_prices, idx_high_price),
        ('volume_of_trade_with_{}_highest_price', recent_volumes, idx_high_price),
        ('time_of_trade_with_{}_highest_price', recent_times, idx_high_price),
        ('price_of_trade_with_{}_lowest_price', recent_prices, idx_low_price),
        ('volume_of_trade_with_{}_lowest_price', recent_volumes, idx_low_price),
        ('time_of_trade_with_{}_lowest_price', recent_times, idx_low_price),
        ('price_of_trade_with_{}_highest_volume', recent_prices, idx_volume),
        ('volume_of_trade_with_{}_highest_volume', recent_volumes, idx_volume),
        ('time_of_trade_with_{}_highest_volume', recent_times, idx_volume),
    ]

    df = pd.DataFrame(index=trade_hours_index)
    for feature_names, recent_property, idx in features:
        df[[
            feature_names.replace('{}', str(j)) for j in range(num_of_top_trades)
        ]] = \
            np.take_along_axis(recent_property, idx, axis=1)

    return df


def _current_bar(ticker, date):

    """

    Args:
        ticker (str): Ticker symbol.
        date (datetime.date): Date to make aggregate bars for.

    Returns:
        pd.DataFrame

    """

    bars = pd.DataFrame(index=data.get_trading_hours_index(
        ticker, date, extended_hours=True
    ))

    # Weighted mean.
    bars = bars.join(
        data.get_bars(ticker, date, 'weighted_mean', extended_hours=True)
            .rename('price')
    )
    # Count.
    bars = bars.join(
        data.get_bars(ticker, date, 'count', extended_hours=True)['price']
            .rename('count')
    )
    # Price, volume, and price-adjusted volume: mean, median, min, max, std.
    for agg in ['mean', 'median', 'min', 'max', 'std']:
        bars = bars.join(
            data.get_bars(
                ticker, date, agg, extended_hours=True
            ).add_suffix('_' + agg)
        )

    return bars


def current_bar_stats(ticker, date, _):
    bars = _current_bar(ticker, date)
    return bars.reindex(data.get_trading_hours_index(ticker, date))


def bar_changes_relative(ticker, date, _):
    bars = _current_bar(ticker, date)

    df = pd.DataFrame(index=bars.index)

    # Calculate relative to now.
    measures = ('median', 'min', 'max', 'std')
    prefixes = ('price', 'volume')
    for prefix in prefixes:
        for measure in measures:
            relative_to = 'price' if prefixes == 'price' else 'volume_mean'
            df[f'0S_{prefix}_{measure}'] = (
                bars[f'{prefix}_{measure}'] / bars[relative_to] - 1
            )

    # Calculate relative to rolling averages.
    measures = (
        'price', 'price_min', 'price_max', 'price_std',
        'volume_mean', 'volume_min', 'volume_max', 'volume_std'
    )
    windows = (
        '1S', '3S', '5S', '10S', '30S',
        '1min', '3min', '5min', '10min', '30min', '1H', '1D'
    )
    for i in windows:
        rolling = bars.shift().rolling(i, min_periods=1)
        for measure in measures:
            df[f'{i}_{measure}'] = bars[measure] / rolling[measure].mean() - 1

    # Calculate relative to time high and low.
    measures = ('price', 'price_min', 'price_max')
    windows = ('1min', '3min', '5min', '10min', '30min', '1H', '1D')
    for i in windows:
        rolling = bars.shift().rolling(i, min_periods=1)
        for measure in measures:
            df[f'{i}_low_{measure}'] = (
                bars[measure] / rolling['price_min'].min() - 1
            )
            df[f'{i}_high_{measure}'] = (
                bars[measure] / rolling['price_max'].max() - 1
            )

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

    # Center standard deviation at 0.
    df[[c for c in df.columns if c.endswith('_std')]] += 1

    return df.reindex(data.get_trading_hours_index(ticker, date))


def bar_trends(ticker, date, _):
    bars = _current_bar(ticker, date)

    df = pd.DataFrame(index=bars.index)

    # TODO:
    #  number of times gone up in the last minute
    #  and time since last up
    #  up or down, np.sign

