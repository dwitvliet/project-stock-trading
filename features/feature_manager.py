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
