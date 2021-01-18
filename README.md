# Stock price prediction
Project to collect and predict stock prices. See the [repository Jupyter Notebook](https://github.com/dwitvliet/project-stock-trading/blob/main/stock_prediction.ipynb) with the analysis and modeling. 

### Setup for running the notebook yourself 

Install MySQL.

Install dependencies:
   
    python3 -m pip install requirements.txt

Setup database: 

    setup.sh database_name username password polygon_api_key | sudo -u root mysql

Config database (`/etc/mysql/mysql.conf.d/mysqld.cnf`):

    [mysqld]
    max_allowed_packet      = 1G
    secure-file-priv        = ""