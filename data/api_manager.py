import time
import datetime
import requests
import logging

import pandas as pd


class APIManager:
    """ Fetch market data and historical data from the Polygon API.
    
    Talks to the Polygon API, fetching information on tickers and trades. For 
    trades, the ticker, time, price, and volume is stored in the database. 
    
    The frequency of requests is capped to avoid going beyond what Polygon 
    permits. The request is stalled upon reaching the maximum allowed frequency.
    
    """
    
    MAX_REQUEST_PER_MINUTE = 200
    STALL_TIME_UPON_MAX_REQUESTS = 3
    MAX_ATTEMPTS = 5
    
    def __init__(self, api_key):
        self._api_key = api_key
        self._recent_requests = []
        
    def _count_recent_requests(self):
        """ Counts the number of recent requests.

        Returns:
            int: The number of requests sent in the last minute.

        """
        self._recent_requests = [
            r for r in self._recent_requests
            if time.time() - r < 60
        ]
        return len(self._recent_requests)
        
    def _request(self, url, params=None, attempts_left=None):
        """

        Args:
            url (str, optional): The destination of the request, relative to the
                polygon domain (e.g. /v2/reference/tickers).
            params (dict, optional): Parameters to send along the request.
            attempts_left (int, optional): How many times to attempt the request
                before giving up. Defaults to class property MAX_ATTEMPTS.

        Returns:
            dict or list: The resulting object of the request, or None if failed.

        """

        if params is None:
            params = {}
        if attempts_left is None:
            attempts_left = self.MAX_ATTEMPTS - 1

        # Stall the request if it would exceed the maximum number of requests
        # allowed by the API.
        while self._count_recent_requests() >= self.MAX_REQUEST_PER_MINUTE:
            time.sleep(self.STALL_TIME_UPON_MAX_REQUESTS)
            logging.info('Stalled because of too many requests.')

        # Send the request.
        params['apiKey'] = self._api_key
        result = requests.get(f'https://api.polygon.io{url}', params=params)

        # If the request was successful, return the resulting object.
        if result.status_code == 200:
            json = result.json()
            if type(json) == list or json.get('success', True):
                return json

        # If an error happened, log it and try sending the request again if
        # the maximum number of attempts has not been reached.
        logging.error(
            f'Could not complete request {url} '
            f'(Error: {result.status_code}, attempts left: {attempts_left})'
        )
        if attempts_left > 0:
            time.sleep(5)
            return self._request(url, params, attempts_left-1)

        # If no attempts are remaining, give up.
        logging.info(f'Exited with status code {result.status_code}.')

    def _request_batch(self, url, limit, timestamp=0):
        """ Perform multiple requests to fetch paginated results.

        The Polygon API restricts the number of fetched items per request for
        trades and quotes, resulting in having to perform multiple requests to
        fetch all the results of a request.

        Args:
            url (str, optional): The destination of the request, relative to the
                polygon domain (e.g. /v2/reference/tickers).
            limit (int): The number of items to ask for per request.
            timestamp (int, optional): The offset of the first item. The time
                of the last item received should be used as the offset for the
                next request.

        Returns:
            list: The resulting object of the request, or None if failed.

        """

        # Perform the request.
        params = {
            'timestamp': timestamp,
            'limit': limit
        }
        response = self._request(url, params)
        if response is None:
            return None
        
        # If this is not the first request in the batch, exclude the first item
        # in the response as it was already present in the previous request.
        result = response['results'][int(timestamp > 0):]

        # Repeat requests and merge the results until all available items have
        # been received.
        last_timestamp = result[-1]['t']
        if response['results_count'] >= limit:
            result.extend(self._request_batch(url, limit, last_timestamp))
        
        return result

    def get_ticker_details(self, ticker):
        """ Get details about ticker, including exchange traded on.

        Args:
            ticker (str): Ticker symbol to get information about.

        Returns:
            dict: Info on ticker, for full list of keys see
                https://polygon.io/docs/get_v1_meta_symbols__stocksTicker__company_anchor

        """
        url = f'/v1/meta/symbols/{ticker}/company'
        return self._request(url)

    def get_daily_trades(self, ticker, date, data_type='trades'):
        """ Get all trades or quotes on a specific date for a ticker.

        Args:
            ticker (str): Ticker symbol fetch trades/quotes for.
            date (datetime.date or str): Date to fetch trades/quotes for.
            data_type (str, optional): What to fetch, 'trades' or 'quotes'.

        Returns:
            pd.Dataframe: All quotes/trades for the entire day, sorted by
                timestamp.

        """

        # Set the trades to fetch to the maximum set by the API.
        trades_per_request = 50000

        # Perform the request.
        if data_type == 'trades':
            url = f'/v2/ticks/stocks/trades/{ticker}/{date}'
        else:  # quotes
            url = f'/v2/ticks/stocks/nbbo/{ticker}/{date}'
        trades = self._request_batch(url, trades_per_request)

        # Create and return a dataframe with the results.
        if data_type == 'trades':
            keys_to_keep = ['t', 'p', 's']
            column_names = ['timestamp', 'price', 'volume']
        else:  # quotes
            keys_to_keep = ['t', 'P', 'S', 'p', 's']
            column_names = [
                'timestamp', 'ask_price', 'ask_volume',
                'bid_price', 'bid_volume'
            ]
        trades = pd.DataFrame(trades)[keys_to_keep]
        trades.columns = column_names

        return trades

    def get_upcoming_holidays(self):
        """ Get upcoming market holidays.

        Returns:
            list of dict: A list of holidays for each exchange.

        """
        url = f'/v1/marketstatus/upcoming'
        return self._request(url)
