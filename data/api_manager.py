import time
import datetime
import requests
import logging

import pandas as pd


class API_Manager:
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
        self._recent_requests = [r for r in self._recent_requests if time.time() - r < 60]
        return len(self._recent_requests)
        
    def _request(self, url, params={}, attempts_left=None):
        
        if attempts_left is None:
            attempts_left = self.MAX_ATTEMPTS - 1
        
        while self._count_recent_requests() >= self.MAX_REQUEST_PER_MINUTE:
            time.sleep(self.STALL_TIME_UPON_MAX_REQUESTS)
            logging.info('Stalled because of too many requests.')
        
        params['apiKey'] = self._api_key
        result = requests.get(f'https://api.polygon.io{url}', params=params)

        if result.status_code == 200:
            json = result.json()
            if json.get('success', True):
                return json
        
        if attempts_left == 0:
            logging.info(f'Exited with status code {result.status_code}.')
            return None
        
        logging.error(
            f'Could not complete request {url} '
            f'(Error: {result.status_code}, attempts left: {attempts_left})'
        )
        time.sleep(5)
        return self._request(url, params, attempts_left-1)
    
    
    def get_ticker_details(self, ticker):
        # https://polygon.io/docs/get_v1_meta_symbols__stocksTicker__company_anchor
        url = f'/v1/meta/symbols/{ticker}/company'
        return self._request(url)
    
    
    def _request_batch(self, url, limit, timestamp=0):
        
        params = {
            'timestamp': timestamp,
            'limit': limit
        }

        response = self._request(url, params)
        if response is None:
            return None
        
        # Exclude first trade in responses as it was already present in the 
        # previous request.
        result = response['results'][int(timestamp > 0):]

        # Repeat requests until all available data has been received.
        last_timestamp = result[-1]['t']
        if response['results_count'] >= limit:
            result.extend(self._request_batch(url, limit, last_timestamp))
        
        return result
    
    
    def get_daily_trades(self, ticker, date, quotes=False, start_time=0):
        # https://polygon.io/docs/get_v2_ticks_stocks_trades__ticker___date__anchor
        
        TRADES_PER_REQUEST = 50000
        
        if type(date) == datetime.date:
            date = date.strftime('%Y-%m-%d')
        
        if quotes:
            url = f'/v2/ticks/stocks/nbbo/{ticker}/{date}'
        else:
            url = f'/v2/ticks/stocks/trades/{ticker}/{date}'
                
        trades = self._request_batch(url, TRADES_PER_REQUEST)
        
        if quotes:
            keys_to_keep = ['t', 'p', 's', 'P', 'S']
            column_names = ['timestamp', 'bid_price', 'bid_volume', 'ask_price', 'ask_volume']
        else:
            keys_to_keep = ['t', 'p', 's']
            column_names = ['timestamp', 'price', 'volume']
        
        
        trades = pd.DataFrame(trades)[keys_to_keep]
        trades.columns = column_names
        

        return trades