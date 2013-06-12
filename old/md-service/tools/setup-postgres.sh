#!/bin/sh

su postgres -c "psql -U postgres -c 'CREATE USER mdsqluser'"
su postgres -c "psql -U postgres -c 'CREATE DATABASE syndicate'"
su postgres -c "psql -U postgres -c 'GRANT ALL PRIVILEGES ON syndicate TO mdsqluser'"

echo "Edit /var/lib/pgsql/data/pg_hba.conf to trust local connections."
