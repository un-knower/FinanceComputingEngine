#!/bin/bash

TMP_FILE=tmp_tables
TABLES_FILE=tables.txt
echo "list" | /usr/hdp/2.6.5.0-292/hbase/bin/hbase shell > tmp_tables
sleep 2

sed '1,6d' $TMP_FILE | tac | sed '1,3d' | tac > $TABLES_FILE

sleep 2 

for table in $(cat $TABLES_FILE); do
	echo "major_compact '$table'" | /usr/hdp/2.6.5.0-292/hbase/bin/hbase shell
	sleep 1
done


