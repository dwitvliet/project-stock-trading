import numpy as np
import pandas as pd


def profits(bars, price='price', prediction='prediction', cost=0):
    """ Test how profitable predictions are.
    
    Given a dataframe of prices and a prediction on whether to buy or sell at a
    certain time, calculate the profits gained during the time period.
    
    Args:
        bars (pd.DataFrame): Dataframe with each row being a time period.
        price (str, optional): The column containing prices.
        prediction (str, optional): The column containing predictions on whether
            to buy or not. Values should be either 1 (buy), 0 (do not sell), or
            -1 (sell).
        cost (float, optional): The cost of each transaction, which should be
            substracted from the profits.
            
    Returns:
        float
    
    """
    
    bars = bars.copy()
    
    # Determine which time periods the stock is owned
    bars['own'] = np.nan
    bars.loc[bars[prediction] == 1, 'own'] = True
    bars.loc[bars[prediction] == -1, 'own'] = False
    bars['own'] = bars['own'].fillna(method='ffill')
    bars['own'] = bars['own'].fillna(False)
    
    # Determine gain during owned periods
    bars['rel_change'] = bars[price].shift(-1) / bars['price']
    
    active_gain = bars['rel_change'][bars['own']].product() - 1
    passive_gain = bars[price].dropna()[-1]/bars[price].dropna()[0] - 1

    transaction_costs = bars['own']
    
    print('Active gain:', active_gain)
    print('Passive gain:', passive_gain)
