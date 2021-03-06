#!/bin/sh

if [ $# -ne 4 ]
then
    echo "Usage: $0 <DATABASE-NAME> <DATABASE-USER> <DATABASE-PASWORD> <API-KEY>"
    exit 1
fi

DATABASE="$1"
DATABASE_USER="$2"
DATABASE_PASSWORD="$3"
API_KEY="$4"

cat > config.py << EOF
database = {
  'host': 'localhost',
  'database': '$DATABASE',
  'user': '$DATABASE_USER',
  'password': '$DATABASE_PASSWORD',
}

api_key = '$API_KEY'
EOF

cat <<EOSQL
CREATE DATABASE IF NOT EXISTS $DATABASE;
CREATE USER IF NOT EXISTS '$DATABASE_USER'@'localhost' IDENTIFIED BY '$DATABASE_PASSWORD';
GRANT ALL PRIVILEGES ON $DATABASE.* to '$DATABASE_USER'@'localhost';
GRANT FILE ON $DATABASE.* to '$DATABASE_USER'@'localhost';
EOSQL

