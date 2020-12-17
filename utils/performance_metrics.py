import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


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
        'total_guys': len(gains_per_epoch),
        'buys_with_loss': sum(gains_per_epoch < 0),
        'passive_gain': passive_gain
    }


def plot_buy_sell(bars, columns, prediction='prediction'):
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
