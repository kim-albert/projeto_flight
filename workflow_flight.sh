#!/bin/bash

# hdfs repo
echo 'Create HDFS structure folders'
./desafio_flight/script/sh/hdfs_directory.sh 

# raw zone
echo 'Create and ingest raw tables'
hive -f ./desafio_flight/script/sql/create_raw.sql

# transform zone
echo 'Prepare tables'
hive -f ./desafio_flight/script/sql/transform_prepare.sql
echo 'Denormalize tables'
hive -f ./desafio_flight/script/sql/transform_denormalization.sql

# consume zone
echo 'Create views for consume'
hive -f ./desafio_flight/script/sql/create_view_consume.sql