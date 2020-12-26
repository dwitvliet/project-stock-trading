import datetime

import numpy as np
import pandas as pd

import data.data_manager as data


def recent_top_trades(ticker, date, params):
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
