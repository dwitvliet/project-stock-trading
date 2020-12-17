import numpy as np
import pandas as pd
import scipy as sc
import scipy.signal
import matplotlib.pyplot as plt


def label_timeseries(bars, gain_threshold):
    """ Labels price increases as 'buy' and decreases as 'buy'.

    Any increases larger than the `gain_threshold` is labeled as 'buy' whereas
    the rest is labeled as 'sell'. To distinguish when during the increase that
    a buy is profitable, and when it is only profitable to hold an existing buy,
    the last small increase before selling is labeled as 'keep'.

    Args:
        bars (pd.Series): A timeseries of prices.
        gain_threshold (float): The threshold that an increase have to be above
            to be considered profitable.

    Returns:
        pd.Series

    """
    label = pd.Series(index=bars.index)
    bars = bars.values

    # Get local minima and maxima (extrema) for the time series.
    minima_and_maxima = np.sort(np.concatenate([
        [0],
        sc.signal.argrelextrema(bars, np.less_equal, order=5)[0],
        sc.signal.argrelextrema(bars, np.greater_equal, order=5)[0]
    ]))

    # For each extrema, calculate the price difference to the next series of
    # extrema until the price has decreased below the maximum price.
    extrema_labeled = []
    for i, start_extremum in enumerate(minima_and_maxima):
        if i in extrema_labeled:
            continue
        start_price = bars[start_extremum]
        max_extremum = start_extremum
        max_price = start_price
        action = 'sell'  # end of day
        for j, future_extremum in enumerate(minima_and_maxima[i + 1:], i + 1):
            future_price = bars[future_extremum]
            if future_price > max_price:
                # Still gaining in price.
                max_extremum = future_extremum
                max_price = future_price
            if max_price - start_price > gain_threshold:
                # Gained enough to be profitable.
                if max_price - future_price > gain_threshold:
                    # But not gaining anymore.
                    action = 'buy'
                    break
            if future_price < start_price:
                # Decreased in price.
                action = 'sell'
                break

        # If the maximum price is above the threshold, buy from the start to the
        # maximum, and sell during the following decrease.
        label.iloc[start_extremum:max_extremum] = action
        label.iloc[max_extremum:future_extremum] = 'sell'

        # Separate 'buy' epochs into 'buy' and 'keep', to prevent buying shortly
        # before the price decreases.
        if action == 'buy':
            last_idx_to_buy = max_extremum
            for idx in reversed(range(start_extremum, max_extremum)):
                if max_price - bars[idx] < gain_threshold:
                    last_idx_to_buy = idx
            label.iloc[last_idx_to_buy:max_extremum] = 'keep'

        extrema_labeled.extend(range(i, j))

    # Always sell at the end of the day.
    label.iloc[-1] = 'sell'

    return label


def profits(bars, price='price', prediction='prediction', buy_cost=0):
    """ Test how profitable predictions are.
    
    Given a dataframe of prices and a prediction on whether to buy or sell at a
    certain time, calculate the profits gained during the time period.
    
    Args:
        bars (pd.DataFrame): Dataframe with each row being a time period.
        price (str, optional): The column containing prices.
        prediction (str, optional): The column containing predictions on whether
            to buy or not. Values should be either 1 (buy), 0 (keep), or
            -1 (sell).
        buy_cost (float, optional): The cost of buying and selling a stock, to
            be substracted from the profits.
            
    Returns:
        dict
    
    """

    # Determine which time periods the stock is owned.
    own = pd.Series(index=bars.index)
    own[bars[prediction].isin((1, 'buy'))] = True
    own[bars[prediction].isin((-1, 'sell'))] = False
    own = own.fillna(method='ffill').fillna(False)

    # Determine relative gain during owned epochs.
    price_by_epoch = bars[price][own].groupby(
        (own.shift() != own).cumsum()[own]
    )
    price_at_buy = price_by_epoch.head(1).values
    price_at_sell = price_by_epoch.tail(1).values
    gains_per_epoch = (price_at_sell - price_at_buy - buy_cost) / price_at_buy

    # Print result.
    active_gain = np.prod(1 + gains_per_epoch) - 1
    passive_gain = bars[price].dropna()[-1] / bars[price].dropna()[0] - 1

    return {
        'active_gain': active_gain,
        'total_buys': len(gains_per_epoch),
        'buys_with_loss': sum(gains_per_epoch < 0),
        'passive_gain': passive_gain
    }


def plot_timeseries(bars, columns, prediction='prediction'):
    """ Make a line plot with buys and sells shaded.

    Args:
        bars (pd.DataFrame): The data containing t
        columns (str or list of str): The columns to plot on the y-axis.
        prediction (str, option: The column containing buy/sell predictions.

    """

    if type(columns) not in (list, tuple):
        columns = [columns]

    plots = len(columns)

    fig, axes = plt.subplots(plots, 1, figsize=(9, plots * 3))
    if plots == 1:
        axes = [axes]

    for column, ax in zip(columns, axes):
        bars[column].plot.line(ax=ax, ylabel=column)

    predictions = bars[prediction]
    cumsum = (predictions.shift() != predictions).cumsum()
    pred_starts = predictions.groupby(cumsum).head(1)
    pred_ends = predictions.groupby(cumsum.shift().fillna(cumsum[0])).tail(1)
    for prediction, start, end in zip(pred_starts, pred_starts.index, pred_ends.index):
        for ax in axes:
            if prediction == 'buy':
                ax.axvspan(start, end, color='green', alpha=0.2, lw=0)
            if prediction == 'sell':
                ax.axvspan(start, end, color='red', alpha=0.2, lw=0)

    plt.show()
