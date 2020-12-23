import pandas as pd

import data.data_manager as data


def absolute_times(ticker, date, _):

    time_periods = data.get_trading_hours_index(ticker, date)

    df = pd.DataFrame(index=time_periods)
    df['time'] = (time_periods - pd.Timestamp('1970-01-01')) // pd.Timedelta('1s')
    df['year'] = time_periods.year
    df['day'] = time_periods.day
    df['hour'] = time_periods.hour
    df['minute'] = time_periods.minute
    df['second'] = time_periods.second

    categories = range(1, 4+1)
    quarters = pd.get_dummies(time_periods.quarter) \
        .T.reindex(categories).T.fillna(0).astype(int).set_index(time_periods) \
        .add_prefix('quarter_')

    categories = range(1, 12+1)
    months = pd.get_dummies(time_periods.month) \
        .T.reindex(categories).T.fillna(0).astype(int).set_index(time_periods) \
        .add_prefix('month_')

    categories = range(0, 4+1)
    weekdays = pd.get_dummies(time_periods.dayofweek) \
        .T.reindex(categories).T.fillna(0).astype(int).set_index(time_periods) \
        .add_prefix('weekday_')

    dfs = [df, quarters, months, weekdays]

    return pd.concat(dfs, axis=1, sort=False, copy=False)
