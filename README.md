# project-stock-trading
Sample project for collecting and predicting stock prices

## Setup

Install MySQL.

Install dependencies.
   
    python3 -m pip install requirements.txt

Setup database: 

    setup.sh database_name username password polygon_api_key | sudo -u root mysql

Config database (`/etc/mysql/mysql.conf.d/mysqld.cnf`):

    [mysqld]
    max_allowed_packet      = 1G
    secure-file-priv        = ""