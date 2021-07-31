#! /bin/bash
export PGPORT=5434
echo "creating db named ... "$USER"_DB"
createdb -h localhost -p $PGPORT $USER"_DB"
pg_ctlcluster 12 myDB status

echo "Copying csv files ... "
sleep 1
cp ../data/*.csv /tmp/$USER

echo "Initializing tables .. "
sleep 1
psql -h localhost -p $PGPORT $USER"_DB" < ../sql/create.sql