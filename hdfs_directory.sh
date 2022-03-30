#!/bin/bash
echo 'Create hdfs folder db_flight'
hdfs dfs -mkdir -p /db_flight

echo 'Create hdfs folder tb_airport'
hdfs dfs -mkdir -p /db_flight/raw/tb_airport
echo 'Upload airports.csv into tb_airport folder'
hdfs dfs -put -f /home/cloudera/workspace/mount_cloudera/airports.csv /db_flight/raw/tb_airport

echo 'Create hdfs folder tb_airline'
hdfs dfs -mkdir -p /db_flight/raw/tb_airline
echo 'Upload airlines.csv into tb_airline folder'
hdfs dfs -put -f /home/cloudera/workspace/mount_cloudera/tb_airlines.csv /db_flight/raw/tb_airline

echo 'Create hdfs folder tb_flight'
hdfs dfs -mkdir -p /db_flight/raw/tb_flight
echo 'Upload flights.csv into tb_flight folder'
hdfs dfs -put -f /home/cloudera/workspace/mount_cloudera/flights.csv /db_flight/raw/tb_flight

echo 'Create hdfs folder tb_aeroporto_companhia'
hdfs dfs -mkdir -p /db_flight/transform/tb_aeroporto_companhia

echo 'Create hdfs folder tb_voo'
hdfs dfs -mkdir -p /db_flight/transform/tb_voo