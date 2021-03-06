import numpy as np
import pandas as pd
import scipy as sc
import scipy.signal
import matplotlib as mpl
import matplotlib.patches
import matplotlib.pyplot as plt

import data.data_manager as data


def label_buy_or_sell(ticker, date, params):
    """ Label price increases as 'buy' and decreases as 'sell'.

    Any increases larger than the `gain_threshold` is labeled as 'buy' whereas
    the rest is labeled as 'sell'.

    Args:
        ticker (str): Ticker symbol.
        date (datetime.date): Date to label.
        params (dict):
            "periods_to_smooth_by" (int): The number of periods into the past
                (inclusive) to average the price over, to reduce low-level
                noise.
            "gain_threshold" (float): The threshold that an increase have to be
                above to be considered profitable.
            "classify_keep" (bool): If true, distinguishes when during an
                increase that a buy is always profitable, and when it is only
                profitable to keep if already bought.

    Returns:
        pd.Series

    """
    smooth_periods = params.get('periods_to_smooth_by', 3)
    gain_threshold = params.get('gain_threshold', 0.05)
    classify_keep = params.get('classify_keep', False)

    # Get price aggregates per second.
    bars = data.get_bars(
        ticker, date, agg='weighted_mean', smooth_periods=smooth_periods,
        fillna=True
    )

    # Get local minima and maxima (extrema) for the time series.
    minima_and_maxima = np.sort(np.concatenate([
        [0, bars.size - 1],
        sc.signal.argrelextrema(bars.values, np.less_equal, order=5)[0],
        sc.signal.argrelextrema(bars.values, np.greater_equal, order=5)[0],
    ]))

    # For each extrema, calculate the price difference to the next series of
    # extrema until the price has decreased below the maximum price.
    label = pd.Series(index=bars.index, name='prediction')
    extrema_labeled = []
    for i, start_extremum in enumerate(minima_and_maxima):
        if i in extrema_labeled:
            continue
        start_price = bars[start_extremum]

        # Initiate variable for loop in case the extremum is the of the day.
        future_extremum = max_extremum = start_extremum
        max_price = start_price
        action = None
        j = i

        # Iterate all future extrema.
        for j, future_extremum in enumerate(minima_and_maxima[i + 1:], i + 1):
            future_price = bars[future_extremum]
            # Still gaining in price.
            if future_price > max_price:
                max_extremum = future_extremum
                max_price = future_price
            # Not gaining anymore or end of time series.
            if (
                max_price - future_price > gain_threshold or
                j + 1 == len(minima_and_maxima)
            ):
                if max_price - start_price > gain_threshold:
                    # Gained enough to be profitable.
                    action = 'buy'
                else:
                    # Not profitable.
                    action = 'sell'
                break

        # If the maximum price is above the threshold, buy from the start to the
        # maximum, and sell during the following decrease.
        label.iloc[start_extremum:max_extremum] = action
        label.iloc[max_extremum:future_extremum] = 'sell'

        # Separate 'buy' epochs into 'buy' and 'keep', to prevent buying shortly
        # before the price decreases.
        if classify_keep and action == 'buy':
            last_idx_to_buy = max_extremum
            for idx in reversed(range(start_extremum, max_extremum)):
                if max_price - bars[idx] < gain_threshold:
                    last_idx_to_buy = idx
            label.iloc[last_idx_to_buy:max_extremum] = 'keep'

        extrema_labeled.extend(range(i, j))

    # Always sell at the end of the day.
    label.iloc[-1] = 'sell'

    return label.replace({'buy': 1, 'keep': 0, 'sell': -1})


def profits(prices, labels, buy_cost=0):
    """ Test how profitable predictions are.

    Given a dataframe of prices and a prediction on whether to buy or sell at a
    certain time, calculate the profits gained during the time period.

    Args:
        prices (pd.Series): Time series with prices.
        labels (pd.Series|np.ndarray): Time series containing predictions on
            whether to buy or not. Values should be either 1 (buy) or -1 (sell).
        buy_cost (float, optional): The cost of buying and selling a stock, to
            be substracted from the profits.

    Returns:
        dict

    """

    if len(prices) != len(labels):
        return {}

    if type(labels) == np.ndarray:
        labels = pd.Series(labels, index=prices.index)

    # Determine which time periods the stock is owned.
    own = pd.Series(index=prices.index)
    own[labels.isin((1, 'buy'))] = True
    own[labels.isin((-1, 'sell'))] = False

    # Determine relative gain during owned epochs.
    bars = prices.copy().rename('price').to_frame()
    bars['increase'] = bars.shift(-1) - bars
    bars['increase'] = bars['increase'].fillna(0)
    bars_by_epoch = bars[own].groupby((own.shift() != own).cumsum()[own])
    price_at_buy = bars_by_epoch['price'].head(1).values
    gain_after_sell = bars_by_epoch['increase'].sum()
    gains_per_epoch = (gain_after_sell - buy_cost) / price_at_buy

    # Calculate and return result.
    active_gain = np.prod(1 + gains_per_epoch) - 1
    passive_gain = bars['price'].dropna()[-1] / bars['price'].dropna()[0] - 1

    return {
        'active_gain': active_gain,
        'total_buys': len(gains_per_epoch),
        'buys_with_loss': sum(gains_per_epoch < 0),
        'passive_gain': passive_gain
    }


def profits_metric(y_true, y_pred, ticker=None):
    """ Calculate profit during model optimization.

    Calculates the difference between the active and passive profits with
    predicted buy/sell classifications. Can be used as a scikit-learn metric if
    the ticker argument is preset with functools.partials, e.g.:
    functools.partial(profits_metric, ticker='AAPL')

    Args:
        y_true (pd.Series): The true buy/sell classification with datetime index.
        y_pred (pd.Series|np.ndarray): The predicted buy/sell classifications.
        ticker (str): The ticker that buy/selss are predicted for.

    """
    assert ticker is not None, (
        'Set `profits_metric` ticker with `functools.partial` before use'
    )
    prices = data.get_prices(ticker, y_true.index[0].date())
    pred_profits = profits(prices, y_pred, buy_cost=0.05)
    return pred_profits.get('active_gain', 0) - pred_profits.get('passive_gain', 0)


def plot_timeseries(prices, labels):
    """ Make a line plot with buys and sells shaded.

    Args:
        prices (pd.Series): Time series with prices.
        labels (pd.Series|np.ndarray): Time series containing predictions on
            whether to buy or not. Values should be either 1 (buy) or -1 (sell).

    """

    # Ensure that both prices and labels are pandas time series.
    if type(labels) == np.ndarray:
        labels = pd.Series(labels, index=prices.index)

    # Plot prices.
    fig, ax = plt.subplots(figsize=(9, 3))
    prices.plot.line(ax=ax, ylabel='Price ($)')

    # Plot buys as green and sells as red.
    cumsum = (labels.shift() != labels).cumsum()
    label_starts = labels.groupby(cumsum).head(1)
    label_ends = labels.groupby(cumsum.shift().fillna(cumsum[0])).tail(1)
    for label, start, end in zip(label_starts, label_starts.index, label_ends.index):
        if label in ('buy', 1):
            ax.axvspan(start, end, color='green', alpha=0.2, lw=0)
        if label in ('sell', -1):
            ax.axvspan(start, end, color='red', alpha=0.2, lw=0)

    # Draw legend.
    legend_elements = [
        mpl.patches.Patch(label='Buy', fc='green', ec='green', alpha=0.2),
        mpl.patches.Patch(label='Sell', fc='red', ec='red', alpha=0.2),
    ]
    ax.legend(handles=legend_elements, loc='upper right')

    plt.show()
