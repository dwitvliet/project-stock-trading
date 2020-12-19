import logging

import pandas as pd

import data.data_manager as data


class FeatureManager:

    def __init__(self, ticker):
        self.ticker = ticker
        self.features = {}

    def new(self, name=None, func=None, desc=None, params=None):
        assert name is not None and func is not None, (
            'A name and function is required for a feature.'
        )
        assert name not in self.features, (
            f'Feature `{name}` ({self.ticker}) has already been registered.'
        )
        if params is None:
            params = {}
        self.features[name] = {
            'name': name,
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
        for timestamp, features in dates_to_generate.iterrows():
            date = timestamp.date()
            logging.info(f'Generating {features.sum()} feature(s) for {date}')
            for feature_name in features[features].index:
                feature = self.features[feature_name]

                # Generate a time series of results for the feature.
                result = feature['func'](self.ticker, date, feature['params'])

                # Ensure no accidentally left in NaNs.
                nan_counts = result.isna().sum()
                assert nan_counts == 0, (
                    f'Feature `{feature_name}` ({self.ticker}) has {nan_counts}'
                    f' NaN values for date {date}.'
                )

                # Store results in database.
                data.db.store_feature(
                    self.ticker, feature_name, result, feature['desc']
                )
