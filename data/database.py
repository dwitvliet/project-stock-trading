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
        self._connection = mysql.connector.connect(
            **self._credentials, autocommit=True
        )
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
                    KEY trades_select_all (
                        ticker_id, date, timestamp, price, volume
                    )
                ) ENGINE=INNODB;
            ''')
            
            con.execute('''
                CREATE TABLE IF NOT EXISTS quotes (
                    id INT NOT NULL AUTO_INCREMENT,
                    ticker_id TINYINT UNSIGNED NOT NULL,
                    date DATE NOT NULL,
                    timestamp BIGINT NOT NULL,
                    ask_price FLOAT NOT NULL,
                    ask_volume INT NOT NULL,
                    bid_price FLOAT NOT NULL,
                    bid_volume INT NOT NULL,
                    PRIMARY KEY (id),
                    FOREIGN KEY (ticker_id) REFERENCES tickers(id),
                    KEY quotes_select_all (
                        ticker_id, date, timestamp, ask_price, ask_volume, 
                        bid_price, bid_volume
                    )
                ) ENGINE=INNODB;
            ''')

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

            con.execute('''
                CREATE TABLE IF NOT EXISTS feature_values_summary (
                    feature_id INT NOT NULL,
                    date DATE NOT NULL,
                    PRIMARY KEY (feature_id, date), 
                    FOREIGN KEY (feature_id) REFERENCES features(id)
                ) ENGINE=INNODB;
            ''')

        # Populate holiday table.
        file_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), 'holidays.csv'
        )
        holidays = pd.read_csv(file_path)
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

        self._insert_dataframe('holidays', holidays, replace=True)

    def _insert_dataframe(self, table, df, replace=False):
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

    @functools.lru_cache(maxsize=None)
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
    
    def _store_summary(self, table_name, ticker_id, date):
        query = f'''
            INSERT INTO summary (table_name, ticker_id, date) 
            VALUES (%s, %s, %s)
        '''
        values = (table_name, ticker_id, date)
        with self as con:
            con.execute(query, values)
    
    def store_trades(self, ticker, date, trades, data_type='trades'):
        """ Store trades
        
        Args:
            ticker (str): ticker symbol
            date (Date): date that trade happened
            trades (pd.DataFrame): with columns `timestamp`, `price`, and
                `volume`
            data_type (str, 'trades' or 'quotes'): The type or data to fetch.
        
        """
        ticker_id = self._get_ticker_id(ticker)
        self._store_summary(data_type, ticker_id, date)

        if data_type == 'trades':
            colums = ['timestamp', 'price', 'volume']
        else:  # quotes
            colums = [
                'timestamp', 'ask_price', 'ask_volume',
                'bid_price', 'bid_volume'
            ]

        query = f'''
            INSERT INTO {data_type} (ticker_id, date, {', '.join(colums)}) 
            VALUES (%s, %s, {', '.join(['%s' for _ in colums])})
        '''
        values = [(ticker_id, date, *t[1:]) for t in trades[colums].itertuples()]

        with self as con:
            con.executemany(query, values)

    def get_trades(self, ticker, date, datatype='trades'):
        """ Get all trades/quotes for a ticker for a specific date. 
        
        The time is converted from a Unix timestamp to to datetime in the local
        timezone of NYSE and Nasdaq (Eastern time).
        """
        
        if datatype == 'trades':
            columns = ['timestamp', 'price', 'volume']
        else:  # quotes
            columns = [
                'timestamp', 'ask_price', 'ask_volume',
                'bid_price', 'bid_volume'
            ]
            
        query = f'''
            SELECT {', '.join(columns)}
            FROM {datatype}
            WHERE ticker_id = "{self._get_ticker_id(ticker)}"
            AND date = "{date}"
        '''
        with self as con:
            con.execute(query)
            result = con.fetchall()
            
        df = pd.DataFrame(result, columns=columns)
        df['time'] = pd.to_datetime(df['timestamp']) \
            .dt.tz_localize('UTC') \
            .dt.tz_convert('America/New_York') \
            .dt.tz_localize(None)
        return df.drop('timestamp', axis=1)

    def store_feature(self, ticker, name, series, description=None):

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
            values = [
                (feature_id, time, value)
                for time, value
                in series.iteritems()
            ]
            con.executemany(query, values)

    @functools.lru_cache(maxsize=None)
    def _get_feature_id(self, ticker, feature):
        query = f'''
            SELECT id
            FROM features
            WHERE ticker_id = {self._get_ticker_id(ticker)}
            AND name = "{feature}"
        '''
        with self as con:
            con.execute(query)
            (feature_id, ) = con.fetchone()

        return feature_id

    def get_stored_dates_for_feature(self, ticker, feature):
        feature_id = self._get_feature_id(ticker, feature)

        query = f'''
            SELECT date
            FROM feature_values_summary
            WHERE feature_id = {feature_id}
        '''

        with self as con:
            con.execute(query)
            dates = [row[0] for row in con.fetchall()]
        return dates
#
#
#
#
#
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
