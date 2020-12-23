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

    categorical_dfs = []
    categorical_times = [
        (time_periods.quarter, 'quarter', range(1, 4 + 1)),
        (time_periods.month, 'month', range(1, 12 + 1)),
        (time_periods.dayofweek, 'dayofweek', range(1, 4 + 1)),
    ]
    for values, prefix, categories in categorical_times:
        categorical_dfs.append(
            pd.get_dummies(values)
            .T.reindex(categories).T.set_index(time_periods)
            .fillna(0).astype(int)
            .add_prefix(prefix + '_')
        )

    return pd.concat([df] + categorical_dfs, axis=1, sort=False, copy=False)
