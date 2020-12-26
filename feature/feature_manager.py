import logging

import numpy as np
import pandas as pd

import data.data_manager as data


class FeatureManager:

    def __init__(self, ticker):
        self.ticker = ticker
        self.features = {}

    def new(self, name=None, func=None, params=None, desc=None):
        assert name is not None and func is not None, (
            'A name and function is required for a feature.'
        )
        assert name not in self.features, (
            f'Feature `{name}` ({self.ticker}) has already been registered.'
        )
        if params is None:
            params = {}
        if desc is None:
            desc = func.__doc__
        self.features[name] = {
            'name': name.replace(' ', '_'),
            'func': func,
            'desc': desc,
            'params': params,
        }

    def generate(self, date_from, date_to):
        """ Generates all registered features and stores them in the database.

        As many features use the same data fetched from database, taking
        advantage of cached objects can significantly speed up the feature
        generation process. Therefore, all features are generated for one date
        before moving on to the next.

        Args:
            date_from (Date): First date to generate features for.
            date_to (Date, optional): Last date to generate features for.

        """

        # Check which dates within the date range do not already have generated
        # features.
        dates_to_generate = pd.DataFrame(
            index=pd.date_range(date_from, date_to),
            columns=self.features.keys()
        ).fillna(False)
        for feature_name in self.features:
            dates_to_generate.loc[
                data.dates_missing_from_database(
                    self.ticker, date_from, date_to, feature_name
                ),
                feature_name
            ] = True

        # For each date with features to be generated, iterate each feature.
        dates_to_generate = dates_to_generate[dates_to_generate.sum(axis=1) > 0]
        if dates_to_generate.size == 0:
            logging.info(
                f'All days of from {date_from} to {date_to} already have the '
                f'{len(self.features)} registered feature(s) stored.'
            )

        for timestamp, features in dates_to_generate.iterrows():
            date = timestamp.date()
            logging.info(f'Generating {features.sum()} feature(s) for {date}')
            for feature_name in features[features].index:
                feature = self.features[feature_name]

                # Generate a dataframe of results for the feature.
                result = feature['func'](self.ticker, date, feature['params'])
                if type(result) == pd.Series:
                    result = result.rename('').to_frame()
                logging.info(
                    f'`{feature_name}` with {result.shape[1]} sub-features'
                )

                # Ensure no accidentally left in NaNs or infinite values.
                result = result.replace([np.inf, -np.inf], np.nan)
                nan_counts = result.isna().to_numpy().sum()
                assert nan_counts == 0, (
                    f'Feature `{feature_name}` ({self.ticker}) has {nan_counts}'
                    f' NaN values for date {date}.'
                )

                # Ensure all sub-feature names are unique.
                assert result.columns.size == result.columns.unique().size, (
                    f'Not all feature names for `{feature_name}` are unique.'
                )

                # Store results in database.
                for col_name in result.columns:
                    subfeature_name = feature_name
                    if col_name != '':
                        subfeature_name += '__' + col_name

                    values = result[col_name]
                    data.db.store_feature(
                        self.ticker, subfeature_name, values, feature['desc']
                    )
