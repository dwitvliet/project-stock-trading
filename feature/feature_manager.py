import logging

import numpy as np
import pandas as pd

import data.data_manager as data


class FeatureManager:

    def __init__(self, ticker):
        self.ticker = ticker
        self.features = {}

    def add(self, name=None, func=None, params=None, desc=None):
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
            'name': name.replace(' ', '_').lower(),
            'func': func,
            'params': params,
            'desc': desc.split('Params:')[0],  # exclude params from docstring
        }

    def add_many(self, features):
        for feature in features:
            self.add(*feature)

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
            dfs = []
            descriptions = {}
            logging.info(f'Generating {features.sum()} feature(s) for {date}:')
            for feature_name in features[features].index:
                feature = self.features[feature_name]

                # Generate a dataframe of results for the feature.
                df = feature['func'](self.ticker, date, feature['params'])
                if type(df) == pd.Series:
                    df = df.rename('').to_frame()
                logging.info(
                    f'`{feature_name}` with {df.shape[1]} sub-feature(s).'
                )

                # Ensure no accidentally left in NaNs or infinite values.
                df = df.replace([np.inf, -np.inf], np.nan)
                nan_counts = df.isna().to_numpy().sum()
                assert nan_counts == 0, (
                    f'Feature `{feature_name}` ({self.ticker}) has {nan_counts}'
                    f' NaN values for date {date}.'
                )

                # Ensure all sub-feature names are unique.
                assert df.columns.size == df.columns.unique().size, (
                    f'Not all feature names for `{feature_name}` are unique.'
                )

                # Store results in database.
                if df.columns.size > 1:
                    df.add_prefix(feature_name + '__')

                for col in df.columns:
                    descriptions[col] = feature['desc']
                dfs.append(df)

            data.db.store_features(
                self.ticker,
                pd.concat(dfs, axis=1, sort=False, copy=False),
                descriptions
            )
