class FeatureManager:

    def __init__(self, ticker):
        self.ticker = ticker
        self.features = []

    def new(self, *args):
        self.features.append(args)
