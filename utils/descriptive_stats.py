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
    df['weighted_average'] = df[values] / grouped[values].transform('sum') * df[weights]
    return grouped['weighted_average'].sum()


