import datetime

import numpy as np
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
        (time_periods.dayofweek, 'dayofweek', range(0, 4 + 1)),
    ]
    for values, prefix, categories in categorical_times:
        categorical_dfs.append(
            pd.get_dummies(values)
            .T.reindex(categories).T.set_index(time_periods)
            .fillna(0).astype(int)
            .add_prefix(prefix + '_')
        )

    return pd.concat([df] + categorical_dfs, axis=1, sort=False, copy=False)


def relative_times(ticker, date, _):

    time_periods = data.get_trading_hours_index(ticker, date)
    df = pd.DataFrame(index=time_periods)

    # Time from/until start/end of day
    df['since_start_of_day'] = (time_periods - time_periods[0]).seconds
    df['until_end_of_day'] = (time_periods[-1] - time_periods).seconds

    # Business days from/until first/last business day of the year/quarter/month.
    open_dates = pd.DatetimeIndex(data.get_open_dates(
        ticker,
        datetime.date(date.year, 1, 1),
        datetime.date(date.year, 12, 31),
        exclude_future=False
    ))
    quarter_dates = open_dates[open_dates.quarter == pd.Timestamp(date).quarter]
    month_dates = open_dates[open_dates.month == date.month]
    df['since_year_start'] = open_dates.date.tolist().index(date)
    df['until_year_end'] = open_dates.date[::-1].tolist().index(date)
    df['since_quarter_start'] = quarter_dates.date.tolist().index(date)
    df['until_quarter_end'] = quarter_dates.date[::-1].tolist().index(date)
    df['since_month_start'] = month_dates.date.tolist().index(date)
    df['until_month_end'] = month_dates.date[::-1].tolist().index(date)

    return df
