import logging
import sys
import functools

import numpy as np
import pandas as pd
import tqdm

import data.data_manager as data

tqdm = functools.partial(tqdm.tqdm, file=sys.stdout, position=0, leave=True)


class FeatureManager:

    def __init__(self, ticker):
        self.ticker = ticker
        self.features = {}

    def add(self, name=None, func=None, params=None, desc=None):
        """ Register feature to generate.

        Args:
            name (str): Name of feature.
            func (func): Function to generate feature.
            params (dict, optional): Parameters for feature genereation function.
            desc (str): Description of feature.

        """
        assert name is not None and func is not None, (
            'A name and function is required for a features.'
        )
        name = name.replace(' ', '_').lower()
        assert name not in self.features, (
            f'Feature `{name}` ({self.ticker}) has already been registered.'
        )
        if params is None:
            params = {}
        if desc is None:
            desc = func.__doc__

        self.features[name] = {
            'name': name,
            'func': func,
            'params': params,
            'desc': desc.split('Params:')[0],  # exclude params from docstring
        }

    def add_many(self, features):
        """ Register multiple features at once. """
        for feature in features:
            self.add(*feature)

    def generate(self, date_from, date_to, skip_stored=True):
        """ Generates all registered features and stores them in the database.

        As many features use the same data fetched from database, taking
        advantage of cached objects can significantly speed up the features
        generation process. Therefore, all features are generated for one date
        before moving on to the next.

        Args:
            date_from (Date): First date to generate features for.
            date_to (Date, optional): Last date to generate features for.
            skip_stored (bool, optional): Whether to skip generating features
                for dates that are already stored in the database.

        """

        # Check which dates within the date range do not already have generated
        # features.
        dates_in_range = data.get_open_dates(self.ticker, date_from, date_to)
        dates_stored = data.db.get_stored_dates_for_feature(self.ticker, '')

        logging.info('Feature generation started.')

        for date in tqdm(dates_in_range):
            if skip_stored and date in dates_stored:
                continue
            dfs = []
            descriptions = {}
            for feature_name, feature in self.features.items():

                # Generate a dataframe of results for the features.
                df = feature['func'](self.ticker, date, feature['params'])
                if type(df) == pd.Series:
                    df = df.rename(feature_name).to_frame()

                # Ensure no accidentally left in NaNs or infinite values.
                df = df.replace([np.inf, -np.inf], np.nan)
                nan_counts = df.isna().to_numpy().sum()
                assert nan_counts == 0, (
                    f'Feature `{feature_name}` ({self.ticker}) has {nan_counts}'
                    f' NaN values for date {date}.'
                )

                # Ensure all sub-features names are unique.
                assert df.columns.size == df.columns.unique().size, (
                    f'Not all features names for `{feature_name}` are unique.'
                )

                # Store results in database.
                if df.columns.size > 1:
                    df = df.add_prefix(
                        feature_name + '__'
                    )

                for col in df.columns:
                    descriptions[col] = feature['desc']
                dfs.append(df)

            df_final = pd.concat(dfs, axis=1, sort=False, copy=False)
            data.db.store_features(self.ticker, date, df_final, descriptions)

        logging.info('Feature generation completed.')
