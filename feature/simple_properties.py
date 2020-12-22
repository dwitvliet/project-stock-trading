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


def current_bar_stats(ticker, date, params):
    bars = _current_bar(ticker, date)
    return bars.reindex(data.get_trading_hours_index(ticker, date))


def bar_changes_relative(ticker, date, params):
    bars = _current_bar(ticker, date)

    df = pd.DataFrame(index=bars.index)

    # Relative to now.
    df['0_price_min'] = bars['price_min'] / bars['price'] - 1
    df['0_price_max'] = bars['price_max'] / bars['price'] - 1
    df['0_price_median'] = bars['price_median'] / bars['price'] - 1
    df['0_price_std'] = bars['price_std'] / bars['price']
    df['0_volume_min'] = bars['volume_min'] / bars['volume_mean'] - 1
    df['0_volume_max'] = bars['volume_max'] / bars['volume_mean'] - 1
    df['0_volume_median'] = bars['volume_median'] / bars['volume_mean'] - 1
    df['0_volume_std'] = bars['volume_std'] / bars['volume_mean']


    return df.reindex(data.get_trading_hours_index(ticker, date))
