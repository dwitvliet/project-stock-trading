CREATE TABLE IF NOT EXISTS tickers (
    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
    ticker VARCHAR(10) NOT NULL UNIQUE,
    name TEXT NOT NULL,
    sector TINYTEXT NOT NULL,
    exchange VARCHAR(10) NOT NULL,
    PRIMARY KEY (id),
    KEY tickers_select_id (ticker, id)
) ENGINE=INNODB;

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

-- Summary table for trades and quotes.
CREATE TABLE IF NOT EXISTS summary (
    table_name VARCHAR(10) NOT NULL,
    ticker_id TINYINT UNSIGNED NOT NULL,
    date DATE NOT NULL,
    PRIMARY KEY (table_name, ticker_id, date),
    FOREIGN KEY (ticker_id) REFERENCES tickers(id)
) ENGINE=INNODB;

CREATE TABLE IF NOT EXISTS holidays (
    exchange VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    hours VARCHAR(10) NOT NULL,
    day TEXT NOT NULL,
    PRIMARY KEY (exchange, date),
    KEY holidays_select_all (exchange, date, hours)
) ENGINE=INNODB;

CREATE TABLE IF NOT EXISTS features (
    id INT NOT NULL AUTO_INCREMENT,
    ticker_id TINYINT UNSIGNED NOT NULL,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    PRIMARY KEY (id),
    UNIQUE KEY (ticker_id, name),
    FOREIGN KEY (ticker_id) REFERENCES tickers(id),
    KEY features_join (id, ticker_id, name),
    KEY features_select_id (ticker_id, name, id)
) ENGINE=INNODB;

CREATE TABLE IF NOT EXISTS feature_values (
    time DATETIME NOT NULL,
    feature_id INT NOT NULL,
    value DOUBLE NOT NULL,
    PRIMARY KEY (time, feature_id)
    -- Foreign key excluded as it slows down inserts and is
    -- enforced at the summary level.
    -- FOREIGN KEY (feature_id) REFERENCES features(id)
) ENGINE=INNODB;

-- Summary table for feature values.
CREATE TABLE IF NOT EXISTS feature_values_summary (
    feature_id INT NOT NULL,
    date DATE NOT NULL,
    PRIMARY KEY (feature_id, date),
    FOREIGN KEY (feature_id) REFERENCES features(id)
) ENGINE=INNODB;
