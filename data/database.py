import os
import functools
import logging
import tempfile
import datetime

import mysql.connector
import pandas as pd


class Database:
    """ Database class for all interactions with the MySQL database.

    Should be used as a context manager, e.g.:
    database = Database(credentials)
    with database as connection:
        connection.execute(query, values)
    
    If the database tables do not exist, they are created upon initialization. 
    The holiday table is populated from `holidays.csv`.
    
    The database contains the tables:
    `tickers`: Minimal details on relevant tickers stored in the database.
    `holidays`: List of all holidays among different exchanges, including both 
        dates closed and dates with shorter opening hours.
    `trades`: All trades for specific tickers on specific dates. Includes date 
        of trade, time of trade (in seconds after midnight), price of trade, and
        volume of trade.
    `quotes: All quotes for specific tickers on specific dates.
    `summary`: Summary table to quickly determine which dates and tickers exist
        in the trades and quotes tables.
    `features`: List and description of model features (and target)
    `feature_values`: All values for features.
    `feature_values_summary`: Summary table to determine which features have
        generated for which days.
    
    """
    
    def __init__(self, credentials, store_features_as_pickle=False):
        """ Set up database and create tables if they do not exist.

        Args:
            credentials (dict): Credentials for database login.
            store_features_as_pickle (bool, optional): Whether to forgo the
                `feature_values` database and instead dump each day's generated
                features as a separate pickle file. Circumventing the database
                provides much less flexibility, but increases performance
                significantly as table quickly reaches hundreds of millions of
                rows otherwise.

        """

        self._credentials = credentials
        self._connection = None
        self._cursor_kwargs = {}
        self._create_tables()

        self.store_features_as_pickle = store_features_as_pickle
        self.database_file_path = 'database_files'
        if store_features_as_pickle and not os.path.exists(self.database_file_path):
            os.mkdir(self.database_file_path)
    
    def __enter__(self):
        """ Open database connection upon entering object context """
        self._connection = mysql.connector.connect(
            **self._credentials, autocommit=True, allow_local_infile=True
        )
        self._cursor = self._connection.cursor(**self._cursor_kwargs)
        return self._cursor
    
    def __exit__(self, exc_type, exc_value, traceback):
        """ Close database connection upon existing object context """
        self._cursor.close()
        self._connection.close()
        self._cursor = None
        self._connection = None
        self._cursor_kwargs.clear()
        if exc_type is not None:
            logging.error(exc_type)

    def __call__(self, **kwargs):
        """ Allow setting cursor arguments when connecting to the database """
        self._cursor_kwargs = kwargs
        return self

    def _create_tables(self):
        """ Create database tables if they do not exist """

        # Create tables from file.
        with open('data/create_database_tables.sql') as f:
            sql = f.read()
        with self as con:
            for query in sql.split(';'):
                if query.strip():
                    con.execute(query)

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
        """ Inserts all rows in a dataframe into a database table

        Args:
            table (str): Table to insert into.
            df (pd.DataFrame): Dataframe to insert.
            replace (bool, optional: Whether to replace any existing values.

        """

        # Replace NaNs with None and convert dataframe to list of tuples as
        # MySQL does not understand NumPy and Pandas data structures.
        df = df.where(df.notna(), None)
        values = list(map(tuple, df.to_numpy()))

        # Insert rows.
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
        """ Get id for ticker.

        Can be significantly faster than performing a join for larger tables.
        The result is cached so that it can be called multiple times without
        having to perform multiple database calls.

        Args:
            ticker (str): Ticker to get id for.

        Returns:
            int: The ticker id.

        """
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
        """ Get all the holidays for an exchange within a date range.

        Args:
            exchange (str): The symbol for an exchange.
            date_from (datetime.date or str): First date to get holidays for. If
                None, get all dates before `date_to`.
            date_to (datetime.date or str): Last date to get holidays for. If
                None, get all dates after `date_from`.

        Returns:
            list of tuple: Dates and hours for each holiday.

        """
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
        """ Get all stored dates for a table """
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
            ticker (str): Ticker symbol.
            date (datatime.date): Date that trade happened.
            trades (pd.DataFrame): With columns `timestamp`, `price`, and
                `volume`.
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

        Returns:
            pd.DataFrame

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

    def store_features(self, ticker, date, df, descriptions):
        """ Store all generated features for a specific date.

        Args:
            ticker (str): Ticker that features are generated for.
            date (datatime.date): Date that features are generated for.
            df (pd.DataFrame): All features to store, with each column being a
                feature, and each row being all the generated feature values
                for a specific timepoint.
            descriptions (dict): The description of the features.

        """
        ticker_id = self._get_ticker_id(ticker)

        with self as con:
            # Insert feature names and descriptions.
            query = f'''
                INSERT INTO features (ticker_id, name, description) 
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE description=description
            '''
            values = [(ticker_id, col, descriptions[col]) for col in df.columns]
            con.executemany(query, values)

            # Get unique id of features.
            query = f'''
                SELECT name, id
                FROM features
                WHERE ticker_id = "{ticker_id}"
                AND name IN ("{'", "'.join(df.columns)}")
            '''
            con.execute(query)
            df = df.rename(dict(con.fetchall()), axis=1)

            # Insert summary of feature values.
            query = f'''
                INSERT INTO feature_values_summary (
                    feature_id, date
                ) 
                VALUES (%s, %s)
            '''
            values = [
                (feature_id, date)
                for feature_id in df.columns
            ]
            con.executemany(query, values)

            # Store the actual feature values. If storing as a pickle, the
            # dataframe is dumped directly to the local database directory.
            if self.store_features_as_pickle:
                file_path = os.path.join(self.database_file_path, ticker)
                if not os.path.exists(file_path):
                    os.mkdir(file_path)
                df.to_pickle(os.path.join(file_path, date.isoformat() + '.pickle'))

            # If storing into the database table, the dataframe is saved as a
            # temporary text file and then stored using `LOAD DATA`, which is
            # significantly faster than performing an `INSERT` query.
            else:
                df = df.rename_axis('time', index=True) \
                    .reset_index() \
                    .melt(id_vars='time', var_name='feature_id') \
                    .sort_values(['time', 'feature_id'])

                with tempfile.NamedTemporaryFile() as temp:
                    df.to_csv(temp.name, sep='\t', index=False)
                    temp.flush()

                    con.execute(
                        f'LOAD DATA LOCAL INFILE "{temp.name}" '
                        'INTO TABLE feature_values IGNORE 1 LINES'
                    )

    def _get_feature_ids(self, ticker, feature):
        query = f'''
            SELECT id
            FROM features
            WHERE ticker_id = {self._get_ticker_id(ticker)}
            AND name LIKE "{feature}%"
        '''
        with self as con:
            con.execute(query)
            feature_ids = [row[0] for row in con.fetchall()]

        return feature_ids

    def get_stored_dates_for_feature(self, ticker, features):
        """ Get all stored dates for a list of feature values.

        Assumes that all features in the list were stored together for the same
        dates.

        Args:
            ticker (str): Ticker to get features for.
            features (list): Features to get dates for.

        """
        feature_ids = self._get_feature_ids(ticker, features)
        if len(feature_ids) == 0:
            return feature_ids

        query = f'''
            SELECT DISTINCT date
            FROM feature_values_summary
            WHERE feature_id IN ("{'", "'.join(map(str, feature_ids))}")
        '''

        with self as con:
            con.execute(query)
            dates = [row[0] for row in con.fetchall()]
        return dates

    def get_features(self, ticker, date):
        """ Get all stored feature values for a date """

        if self.store_features_as_pickle:
            file_path = os.path.join(
                self.database_file_path, ticker, date.isoformat() + '.pickle'
            )
            assert os.path.exists(file_path), (
                f'Features are not generated for {ticker} on {date}.'
            )
            result = pd.read_pickle(file_path)

        else:
            query = f'''
                SELECT time, feature_id, value
                FROM feature_values
                WHERE (
                    time BETWEEN {date}
                    AND {date + datetime.timedelta(days=1)}
                )
            '''
            with self as con:
                con.execute(query)
                result = con.fetchall()

        return result

    def feature_ids_to_names(self, ticker):
        """ Get map of features ids to feature names """
        query = f'''
            SELECT id, name
            FROM features
            WHERE ticker_id = {self._get_ticker_id(ticker)}
        '''
        with self as con:
            con.execute(query)
            result = con.fetchall()
        return dict(result)
