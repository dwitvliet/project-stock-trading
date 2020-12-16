import numpy as np
import pandas as pd


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
        float
    
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

    print('Active gain:', active_gain)
    print('Total buys:', len(gains_per_epoch))
    print('Buys with loss:', sum(gains_per_epoch < 0))
    print('Passive gain:', passive_gain)
