#!/bin/bash

url=jdbc:oracle:thin:@192.168.96.212:1521:orcl
uid=MyZH
pwd=MyZH
tableName=LVARLIST
#columns=
hdfsPath=/yss/guzhi/basic_list/LVARLIST

sqoop import --connect ${url} --username ${uid} --password ${pwd}  --delete-target-dir --target-dir ${hdfsPath} --m 1 --table ${tableName}


