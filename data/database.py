import os
import datetime
import logging

import mysql.connector
import pandas as pd


class Database:
    
    """ Database class for all interactions with the MySQL database. 
    
    If the database tables do not exist, they are created upon initialization. 
    The holiday table is populated from `holidays.csv`.
    
    The database contains the tables:

     `tickers`: Minimal details on relevant tickers stored in the database.
     `holidays`: List of all holidays among different exchanges, including both dates closed and dates with shorter opening hours.
     `trades`: All trades for specific tickers on specific dates. Includes date of trade, time of trade (in seconds after midnight), price of trade, and volume of trade.
     `summary`: Summary table to quickly determine which dates and tickers exist in other tables
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
                    ticker VARCHAR(10) NOT NULL,
                    name TEXT NOT NULL,
                    sector TINYTEXT NOT NULL,
                    exchange VARCHAR(10) NOT NULL,
                    PRIMARY KEY (ticker)
                ) ENGINE=INNODB;
            ''')

            con.execute('''
                CREATE TABLE IF NOT EXISTS trades (
                    id INT NOT NULL AUTO_INCREMENT,
                    ticker VARCHAR(10) NOT NULL,
                    date DATE NOT NULL,
                    timestamp BIGINT NOT NULL,
                    price FLOAT NOT NULL,
                    volume INT NOT NULL,
                    PRIMARY KEY (id),
                    KEY trades_select_all (ticker, date, timestamp, price, volume)
                ) ENGINE=INNODB;
            ''')

            con.execute('''
                CREATE TABLE IF NOT EXISTS summary (
                    table_name VARCHAR(10) NOT NULL,
                    ticker VARCHAR(10) NOT NULL,
                    date DATE NOT NULL,
                    PRIMARY KEY (table_name, ticker, date)
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
                    name TEXT,
                    description TEXT,
                    PRIMARY KEY (id)
                ) ENGINE=INNODB;
            ''')
            
            con.execute('''
                CREATE TABLE IF NOT EXISTS feature_values (
                    time DATETIME(3) NOT NULL,
                    feature_id INT NOT NULL,
                    value DOUBLE NOT NULL,
                    PRIMARY KEY (time), 
                    FOREIGN KEY (feature_id) REFERENCES features(id),
                    KEY trades_select_by_time (time, feature_id, value),
                    KEY trades_select_by_feature (feature_id, time, value)
                ) ENGINE=INNODB;
            ''')
            
        # Populate holiday table.
        fpath = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'holidays.csv')
        holidays = pd.read_csv(fpath)
        holidays = pd.melt(
            holidays, 
            id_vars=['date', 'day'], 
            value_vars=['nye', 'ngs', 'sifma', 'otc'],
            var_name='exchange',
            value_name='hours'
        )
        holidays = holidays.dropna()
        holidays['exchange'] = holidays['exchange'].str.upper()

        self.insert_dataframe('holidays', holidays, replace=True)

    
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
        query = f'''
            SELECT date
            FROM holidays
            WHERE exchange = "{exchange}"
            AND date >= "{date_from}"
            AND date <= "{date_to}"
            AND hours = "closed"
        '''
        with self as con:
            con.execute(query)
            holidays = [row[0] for row in con.fetchall()]
            
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
            AND ticker = "{ticker}"
        '''
        with self as con:
            con.execute(query)
            dates = [row[0] for row in con.fetchall()]
        return dates

    
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
        df = df.where(df.notnull(), None)
        values = list(df.itertuples(index=False, name=None))
        
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
        query_summary = f'''
            INSERT INTO summary (table_name, ticker, date) 
            VALUES (%s, %s, %s)
        '''
        values_summary = ('trades', ticker, date)
        
        query = f'''
            INSERT INTO trades (ticker, date, timestamp, price, volume) 
            VALUES (%s, %s, %s, %s, %s)
        '''
        values = [(ticker, date, t['t'], t['p'], t['s']) for t in trades]
        
        with self as con:
            con.execute(query_summary, values_summary)
            con.executemany(query, values)
            
            
    def get_trades(self, ticker, date):
        query = f'''
            SELECT timestamp, price, volume
            FROM trades
            WHERE ticker = {ticker}
            AND date = {date}
        '''
        with self as con:
            con.execute(query)
            return con.fetchall()