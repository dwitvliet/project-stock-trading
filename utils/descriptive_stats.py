import numpy as np
import pandas as pd

def weighted_mean(df, values=None, weights=None, groupby=None):
    """ Calculate the weighted average of a DataFrame column
    
    This method is significantly faster than using np.average in a lambda
    functon passed to grouped.apply().

    Args:
        df (pd.DataFrame): Data.
        values (str): Column to average.
        weights (str): Column with weights.
        groupby (str|Grouper): Colume or Grouper object to group by.

    """

    assert None not in (values, weights, groupby), 'All four arguments are required'

    df = df.copy()
    grouped = df.groupby(groupby)
    df['weighted_average'] = df[values] / grouped[weights].transform('sum') * df[weights]
    return grouped['weighted_average'].sum(min_count=1)


def weighted_median(df, values=None, weights=None, groupby=None):
    """ Calculate the weighted median of a DataFrame column
    
    This method is extremely slow (>1s for >10000 rows) and to be avoided in 
    production.

    Args:
        df (pd.DataFrame): Data.
        values (str): Column to average.
        weights (str): Column with weights.
        groupby (str|Grouper): Colume or Grouper object to group by.

    """
    
    assert None not in (values, weights, groupby), 'All four arguments are required'
    
    df = df.sort_values(values)
    
    def median(x):
        
        if x[weights].size == 0:
            return np.nan
        
        if x[weights].size == 1:
            return x[values].iloc[0]

        cumulative_sum = x[weights].cumsum()
        halfpoint = x[weights].sum() / 2
    
        is_halfpoint = cumulative_sum == halfpoint
        is_over_halfpoint = cumulative_sum >= halfpoint
        median_by_volume = is_over_halfpoint & (is_over_halfpoint.shift() == False) | is_halfpoint.shift()
    
        return x.loc[median_by_volume, values].mean()
    
    return df.groupby(groupby).apply(median)


# n = 10000

# np.random.seed(0)

# values = np.random.uniform(0, 1, size=n)
# weights = np.random.randint(0, 5, size=n)
# groupby = np.random.randint(0, 8000, size=n)

# df = pd.DataFrame({'values': values, 'weights': weights, 'groupby': groupby})


# import time

# time1 = time.time()
# print(weighted_median(df, values='values', weights='weights', groupby='groupby').head())
# print('Time for `weighted_mean`:', time.time() - time1)


# df['values'].median()
# # time1 = time.time()
# # weighted_median_by_lambda(df, values='value', weights='wt', groupby='date')
# # print('Time for `weighted_mean_by_lambda`:', time.time() - time1)