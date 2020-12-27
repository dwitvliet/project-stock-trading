import pandas as pd

import data.data_manager as data


def current_bar(ticker, date):

    bars = pd.DataFrame(index=data.get_trading_hours_index(
        ticker, date, extended_hours=True
    ))

    # Price (weighted mean), count, and volume.
    bars = bars.join([
        data.get_bars(ticker, date, 'weighted_mean', extended_hours=True)
            .rename('price')
            .fillna(method='ffill'),
        data.get_bars(ticker, date, 'count', extended_hours=True)['price']
            .rename('count')
            .fillna(0),
        data.get_bars(ticker, date, 'sum', extended_hours=True)['volume']
            .rename('volume')
            .fillna(0),
        data.get_bars(ticker, date, 'sum', extended_hours=True)['dollar_volume']
            .rename('dollar_volume')
            .fillna(0),
    ])

    # Price, volume, and price*volume: mean, median, min, max, std, and sum.
    for agg in ['mean', 'median', 'min', 'max', 'std']:
        df = data.get_bars(
            ticker, date, agg, extended_hours=True
        ).add_suffix('_' + agg)

        if agg in ('mean', 'median'):
            df = df.fillna(method='ffill')
        elif agg in ('min', 'max'):
            for prefix in ('price', 'volume', 'dollar_volume'):
                fill_with = bars[prefix + ('' if prefix == 'price' else '_mean')]
                df[f'{prefix}_{agg}'] = df[f'{prefix}_{agg}'].fillna(fill_with)
        elif agg in ('std',):
            df = df.fillna(0)

        bars = bars.join(df)

    # Stats relative to mean.
    measures = ('median', 'min', 'max', 'std')
    prefixes = ('price', 'volume')
    for prefix in prefixes:
        for measure in measures:
            relative_to = 'price' if prefix == 'price' else 'volume_mean'
            bars[f'{prefix}_{measure}_relative'] = (
                bars[f'{prefix}_{measure}'] / bars[relative_to] - 1
            )

    # Center standard deviation at 0.
    bars[[c for c in bars.columns if c.endswith('_std')]] += 1

    return bars


def current_bar_stats(ticker, date, _):
    """ Stats of the price and volume of the current time period.

    Mean, median, min, max, and standard deviation of price, volume, and price-
    adjusted volume (price * volume).

    """
    bars = current_bar(ticker, date)
    return bars.reindex(data.get_trading_hours_index(ticker, date))
