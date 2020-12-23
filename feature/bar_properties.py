import datetime

import pandas as pd

import data.data_manager as data


def current_bar(ticker, date):

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

    # Price (weighted mean).
    bars = bars.join(
        data.get_bars(ticker, date, 'weighted_mean', extended_hours=True)
            .rename('price')
    )
    # Count.
    bars = bars.join(
        data.get_bars(ticker, date, 'count', extended_hours=True)['price']
            .rename('count')
    )
    # Total volume.
    bars = bars.join(
        data.get_bars(ticker, date, 'sum', extended_hours=True)['volume']
            .rename('volume')
    )
    # Price, volume, and price*volume: mean, median, min, max, std, and sum.
    for agg in ['mean', 'median', 'min', 'max', 'std']:
        bars = bars.join(
            data.get_bars(
                ticker, date, agg, extended_hours=True
            ).add_suffix('_' + agg)
        )

    # Stats relative to mean.
    measures = ('median', 'min', 'max', 'std')
    prefixes = ('price', 'volume')
    for prefix in prefixes:
        for measure in measures:
            relative_to = 'price' if prefix == 'price' else 'volume_mean'
            bars[f'{prefix}_{measure}_relative'] = (
                bars[f'{prefix}_{measure}'] / bars[relative_to] - 1
            )

    return bars


def current_bar_stats(ticker, date, _):
    bars = current_bar(ticker, date)
    return bars.reindex(data.get_trading_hours_index(ticker, date))
