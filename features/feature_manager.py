import logging

import numpy as np
import pandas as pd

import data.data_manager as data


class FeatureManager:

    def __init__(self, ticker):
        self.ticker = ticker
        self.features = {}
        self.df = None

    def add(self, name=None, func=None, params=None, desc=None):
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
        for feature in features:
            self.add(*feature)

    def generate(self, date_from, date_to, save_to_db=True, skip_stored=True):
        """ Generates all registered features and stores them in the database.

        As many features use the same data fetched from database, taking
        advantage of cached objects can significantly speed up the features
        generation process. Therefore, all features are generated for one date
        before moving on to the next.

        Args:
            date_from (Date): First date to generate features for.
            date_to (Date, optional): Last date to generate features for.
            save_to_db (bool, optional): Whether to store generated features in
                the database. If False, the generated features are stored in the
                memory as an object attribute.
            skip_stored (bool, optional): Whether to skip generating features
                for dates that are already stored in the database.

        """

        generated_features = []

        # Check which dates within the date range do not already have generated
        # features.
        dates_to_generate = pd.DataFrame(
            index=data.get_open_dates(self.ticker, date_from, date_to),
            columns=self.features.keys()
        )
        if skip_stored:
            dates_to_generate = dates_to_generate.fillna(False)
            for feature_name in self.features:
                dates_to_generate.loc[
                    data.dates_missing_from_database(
                        self.ticker, date_from, date_to, feature_name
                    ),
                    feature_name
                ] = True
        else:
            dates_to_generate = dates_to_generate.fillna(True)

        # For each date with features to be generated, iterate each features.
        dates_to_generate = dates_to_generate[dates_to_generate.sum(axis=1) > 0]
        if dates_to_generate.size == 0:
            logging.info(
                f'The {len(self.features)} features(s) are already stored for '
                f'{date_from} to {date_to}.'
            )

        for date, features in dates_to_generate.iterrows():
            dfs = []
            descriptions = {}
            logging.info(f'Generating {features.sum()} features(s) for {date}.')
            for feature_name in features[features].index:
                feature = self.features[feature_name]

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

            if save_to_db:
                logging.info(
                    f'Inserting {df_final.shape[1]} sub-features(s) into the '
                    f'database ({df_final.memory_usage().sum()/1024/1024:.2f} MB).'
                )
                data.db.store_features(self.ticker, df_final, descriptions)
            else:
                generated_features.append(df_final)

        if not save_to_db and generated_features:
            self.df = pd.concat(
                generated_features, axis=0, sort=False, copy=False
            )

        logging.info('Feature generation completed.')
