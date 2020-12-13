import os
import datetime
import functools
import logging

import mysql.connector
import pandas as pd


class Database:
    
    """ Database class for all interactions with the MySQL database. 
    
    If the database tables do not exist, they are created upon initialization. 
    The holiday table is populated from `holidays.csv`.
    
    The database contains the tables:

    `tickers`: Minimal details on relevant tickers stored in the database.
    `holidays`: List of all holidays among different exchanges, including both 
        dates closed and dates with shorter opening hours.
    `trades`: All trades for specific tickers on specific dates. Includes date 
        of trade, time of trade (in seconds after midnight), price of trade, and
        volume of trade.
    `summary`: Summary table to quickly determine which dates and tickers exist
        in other tables
    `features`: List and description of model features (and target)
    `feature_values`: All values for features.
    
    """
    
    def __init__(self, credentials):
        self._credentials = credentials
        self._connection = None
        self._cursor_kwargs = {}
        self._create_tables()
    
    
    def __call__(self, **kwargs):
        self._cursor_kwargs = kwargs
        return self
    
    def __enter__(self):
        self._connection = mysql.connector.connect(**self._credentials, autocommit=True)
        self._cursor = self._connection.cursor(**self._cursor_kwargs)
        return self._cursor
    
    def __exit__(self, exc_type, exc_value, traceback):
        self._cursor.close()
        self._connection.close()
        self._cursor = None
        self._connection = None
        self._cursor_kwargs.clear()
        if exc_type is not None:
            logging.error(exc_type)
        
        
    def _create_tables(self):

        with self as con:
            con.execute('''
                CREATE TABLE IF NOT EXISTS tickers (
                    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    ticker VARCHAR(10) NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    sector TINYTEXT NOT NULL,
                    exchange VARCHAR(10) NOT NULL,
                    PRIMARY KEY (id),
                    KEY tickers_select_id (ticker, id)
                ) ENGINE=INNODB;
            ''')
            
            con.execute('''
                CREATE TABLE IF NOT EXISTS summary (
                    table_name VARCHAR(10) NOT NULL,
                    ticker_id TINYINT UNSIGNED NOT NULL,
                    date DATE NOT NULL,
                    PRIMARY KEY (table_name, ticker_id, date),
                    FOREIGN KEY (ticker_id) REFERENCES tickers(id)
                ) ENGINE=INNODB;
            ''')

            con.execute('''
                CREATE TABLE IF NOT EXISTS trades (
                    id INT NOT NULL AUTO_INCREMENT,
                    ticker_id TINYINT UNSIGNED NOT NULL,
                    date DATE NOT NULL,
                    timestamp BIGINT NOT NULL,
                    price FLOAT NOT NULL,
                    volume INT NOT NULL,
                    PRIMARY KEY (id),
                    FOREIGN KEY (ticker_id) REFERENCES tickers(id),
                    KEY trades_select_all (ticker_id, date, timestamp, price, volume)
                ) ENGINE=INNODB;
            ''')
            
#             con.execute('''
#                 CREATE TABLE IF NOT EXISTS bars (
#                     ticker_id TINYINT UNSIGNED NOT NULL,
#                     stat VARCHAR(10) NOT NULL,
#                     time DATETIME NOT NULL,
#                     period CHAR(3) NOT NULL,
#                     value FLOAT NOT NULL,
#                     PRIMARY KEY (ticker_id, stat, time), 
#                     FOREIGN KEY (ticker_id) REFERENCES tickers(id),
#                     KEY bars_select_all (ticker_id, stat, time, value)
#                 ) ENGINE=INNODB;
#             ''')

            con.execute('''
                CREATE TABLE IF NOT EXISTS holidays (
                    exchange VARCHAR(10) NOT NULL,
                    date DATE NOT NULL,
                    hours VARCHAR(10) NOT NULL,
                    day TEXT NOT NULL,
                    PRIMARY KEY (exchange, date),
                    KEY holidays_select_all (exchange, date, hours)
                ) ENGINE=INNODB;
            ''')
            
            con.execute('''
                CREATE TABLE IF NOT EXISTS features (
                    id INT NOT NULL AUTO_INCREMENT,
                    ticker_id TINYINT UNSIGNED NOT NULL,
                    name VARCHAR(50) NOT NULL,
                    description TEXT,
                    PRIMARY KEY (id),
                    UNIQUE KEY (ticker_id, name),
                    FOREIGN KEY (ticker_id) REFERENCES tickers(id),
                    KEY features_join (id, ticker_id, name),
                    KEY features_select_id (ticker_id, name, id)
                ) ENGINE=INNODB;
            ''')
            
            con.execute('''
                CREATE TABLE IF NOT EXISTS feature_values (
                    feature_id INT NOT NULL,
                    time DATETIME NOT NULL,
                    value DOUBLE NOT NULL,
                    PRIMARY KEY (feature_id, time), 
                    FOREIGN KEY (feature_id) REFERENCES features(id),
                    KEY trades_select_by_name (feature_id, time, value),
                    KEY trades_select_by_time (time, feature_id, value)
                ) ENGINE=INNODB;
            ''')
            
        # Populate holiday table.
        fpath = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'holidays.csv')
        holidays = pd.read_csv(fpath)
        holidays = pd.melt(
            holidays, 
            id_vars=['date', 'day'], 
            value_vars=['nye', 'ngs'],
            var_name='exchange',
            value_name='hours'
        )
        holidays = holidays.replace('13:00', 'half')
        holidays = holidays.dropna()
        holidays['exchange'] = holidays['exchange'].str.upper()

        self.insert_dataframe('holidays', holidays, replace=True)
        

    @functools.lru_cache(maxsize=None)
    def _get_ticker_id(self, ticker):
        query = f'''
            SELECT id
            FROM tickers
            WHERE ticker = "{ticker}"
        '''
        with self as con:
            con.execute(query)
            (ticker_id, ) = con.fetchone()

        return ticker_id
        

    def get_holidays(self, exchange, date_from=None, date_to=None):
        query = f'''
            SELECT date, hours
            FROM holidays
            WHERE exchange = "{exchange}"
        '''
        if date_from:
            query += f'AND date >= "{date_from}" '
        if date_to:
            query += f'AND date <= "{date_to}" '
        with self as con:
            con.execute(query)
            return con.fetchall()
        
        
    def get_open_dates(self, exchange, date_from, date_to):
        """
        Get list of dates within range where exchange is open. Weekends and
        holidays are excluded.

        Args:
            exchange (str): exchange symbol
            date_from (date|str): first date in range (inclusive)
            date_to (date|str): last date in range (inclusive)
        Returns:
            [date, ..]
        """
        
        open_dates = []
    
        # Get holidays.
        holidays = [date for date, hours in self.get_holidays(exchange, date_from, date_to) if hours == 'closed']
            
        for date in pd.date_range(date_from, date_to):
            # Skip Saturdays and Sundays.
            if date.weekday() >= 5:
                continue
            # Skip holidays where the exchange is closed.
            if date in holidays:
                continue
            # Skip today and future days.
            if (datetime.datetime.now() - date).days <= 0:
                continue
            open_dates.append(date.date())

        return open_dates
                    
    
    def get_stored_dates(self, table, ticker):
        query = f'''
            SELECT date
            FROM summary
            WHERE table_name = "{table}"
            AND ticker_id = "{self._get_ticker_id(ticker)}"
        '''
        with self as con:
            con.execute(query)
            dates = [row[0] for row in con.fetchall()]
        return dates

    
    def get_open_hours(self, dates, exchange):
        """ Determine open operating hours of exchange for a range of dates.
        
        Uses the extended hour by Robinhood/Alpaca, which includes 30 minutes of
        pre-market and 2 hours of post-market.
        
        """

        holidays = dict(self.get_holidays(exchange))
        
        hours = {}
        for date in dates:
            halfday = (date in holidays and holidays[date] == 'half')
            start_time = datetime.time(9, 0) 
            close_time = datetime.time(15 if halfday else 18, 0)
            hours[date] = (start_time, close_time)
            
        return hours
    
    
    def get_ticker_details(self, ticker):
        query = f'''
            SELECT name, sector, exchange
            FROM tickers
            WHERE ticker = "{ticker}"
        '''
        with self(dictionary=True) as con:
            con.execute(query)
            details = con.fetchall()
        return details[0] if details else None
    
    
    def store_ticker_details(self, details):
        query = f'''
            INSERT INTO tickers (ticker, name, sector, exchange) 
            VALUES (%s, %s, %s, %s)
        '''
        values = (
            details['symbol'], 
            details['name'], 
            details['sector'], 
            details['exchangeSymbol'],
        )
        with self as con:
            con.execute(query, values)
        return self.get_ticker_details(details['symbol'])
    
    
    def insert_dataframe(self, table, df, replace=False):
        # Replace NaNs with None and covert dataframe to list of tuples as
        # MySQL does not understand NumPy and Pandas data structures.
        df = df.where(df.notna(), None)
        values = list(map(tuple, df.values))
        
        with self as con:
            con.executemany(f'''
                {'REPLACE' if replace else 'INSERT'} INTO {table} (
                    {','.join(df.columns)}
                )
                VALUES (
                    {','.join(['%s'] * len(df.columns))}
                )
            ''', values)
    
    
    def store_trades(self, ticker, date, trades):
        """ Store trades
        
        Args:
            ticker (str): ticker symbol
            date (Date): date that trade happened
            trades (pd.DataFrame): with columns `timestamp`, `price`, and `volume`
        
        """
        ticker_id = self._get_ticker_id(ticker)
        
        query_summary = f'''
            INSERT INTO summary (table_name, ticker_id, date) 
            VALUES (%s, %s, %s)
        '''
        values_summary = ('trades', ticker_id, date)
        
        query = f'''
            INSERT INTO trades (ticker_id, date, timestamp, price, volume) 
            VALUES (%s, %s, %s, %s, %s)
        '''
        values = [(ticker_id, date, t.timestamp, t.price, t.volume) for t in trades.itertuples()]

        with self as con:
            con.execute(query_summary, values_summary)
            con.executemany(query, values)
            
    
#     def store_bars(self, ticker, stat, series):
#         """ Store summary stats for trades
        
#         Args:
#             ticker (str): ticker symbol
#             stat (str): name of summary stat
#             series (pd.Series|pd.DataFrame): One-dimensional with time as index
        
#         """
        
#         ticker_id = self._get_ticker_id(ticker)
        
#         # Ensure the values are in a Series and drop NaNs.
#         series = series.squeeze().dropna()

#         query = f'''
#             INSERT INTO bars (ticker_id, stat, time, value) 
#             VALUES (%s, %s, %s, %s)
#         '''
#         values = [(ticker_id, stat, time, value) for (time, value) in series.iteritems()]
        
#         with self as con:
#             con.executemany(query, values)

            
    def get_trades(self, ticker, date):
        """ Get all trades for a ticker for a specific date.
        
        The time of the trade is converted from Unix timestamp to datetime
        in the local timezome of the exchange (Eastern time).
        
        """
        
        ticker_id = self._get_ticker_id(ticker)
        
        query = f'''
            SELECT timestamp, price, volume
            FROM trades
            WHERE ticker_id = "{ticker_id}"
            AND date = "{date}"
        '''
        with self as con:
            con.execute(query)
            result = con.fetchall()
            
        trades = pd.DataFrame(result, columns=['timestamp', 'price', 'volume'])
        trades['time'] = pd.to_datetime(trades['timestamp']) \
            .dt.tz_localize('UTC') \
            .dt.tz_convert('America/New_York') \
            .dt.tz_localize(None)
        return trades[['time', 'price', 'volume']]
    
        
        
    def store_feature(self, ticker, name, series, description=None):
        
        # Ensure the values are in a Series and drop NaNs.
        series = series.squeeze().dropna()
        
        with self as con:
            # Insert feature name and description.
            query = f'''
                INSERT INTO features (ticker, name, description) 
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE description=description
            '''
            values = (ticker, name, description)
            con.execute(query, values)
            
            # Get unique id of feature.
            query = f'''
                SELECT id
                FROM features
                WHERE ticker = "{ticker}"
                AND name = "{name}"
            '''
            con.execute(query)
            (feature_id, ) = con.fetchone()
            
            # Insert feature values.
            query = f'''
                INSERT INTO feature_values (feature_id, time, value) 
                VALUES (%s, %s, %s)
            '''
            values = [(feature_id, time, value) for time, value in series.iteritems()]
            con.executemany(query, values)
        

        



# import time
# credentials = {
#   'host': 'localhost',
#   'database': 'trades',
#   'user': 'trades',
#   'password': 'password',
# }
# db = Database(credentials)
# query = f'''
#     SELECT timestamp, price, volume
#     FROM trades
# '''
# time_before = time.time()
# with db as con:
#     con.execute(query)
#     a = con.fetchall()
    
# print(time.time()-time_before)