#! /bin/bash
folder=/tmp/$USER
export PGDATA=$folder/myDB/data
export PGSOCKETS=$folder/myDB/sockets

echo $folder

# #clear previous 12 myDB cluster
pg_dropcluster 12 myDB
# #Clear folder
rm -rf $folder

#Initialize folders
# mkdir $folder
# mkdir $folder/myDB
# mkdir $folder/myDB/data
# mkdir $folder/myDB/sockets
# sleep 1
# cp ../data/*.csv $folder/myDB/data

#Initialize DB
#initdb
export PGPORT=5434
pg_createcluster -d $folder -s $PGSOCKETS -l $folder/logfile -p $PGPORT 12 myDB

sleep 1
#Start folder
sudo pg_ctlcluster 12 myDB start
echo "myDB server started"
# /usr/lib/postgresql/12/bin/pg_ctl -o "-c unix_socket_directories=$PGSOCKETS -p $PGPORT" -D $PGDATA -l $folder/logfile start

