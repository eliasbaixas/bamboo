#!/bin/sh

ENVDIR=$1
DBS="client_counts by_time by_guid by_guid_and_data_hash recycling"
RUNJAVA=../../../bin/run-java
DBLOAD=~/cvs/db-4.2.52/build_unix/db_load

rm -f $ENVDIR/log.* $ENVDIR/*.db $ENVDIR/*.dump

$RUNJAVA -mx64M bamboo.db.CleanShutdown $ENVDIR || exit 1;

for db in $DBS
do 
    echo "DB=$db"
    $RUNJAVA -mx256M com.sleepycat.je.util.DbDump -s $db -h $ENVDIR | \
    sed -e 's/true/1/g' | sed -e 's/false/0/g' | grep -v '^database=' | \
    $DBLOAD $ENVDIR/$db.db || exit 2;
done

$RUNJAVA -mx64M bamboo.db.RecoverFatal $ENVDIR || exit 3;

