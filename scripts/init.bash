#!/bin/bash

rm -r chk-point-dir chk-point-dir2

kafka-topics.sh --bootstrap-server localhost:9092 --topic local_skyline_topic --delete

kafka-topics.sh --bootstrap-server localhost:9092 --topic input_skyline_topic --delete

kafka-topics.sh --bootstrap-server localhost:9092 --topic output_skyline_topic --delete

sleep 1

kafka-topics.sh --bootstrap-server localhost:9092 --topic input_skyline_topic --create --partitions 5

kafka-topics.sh --bootstrap-server localhost:9092 --topic local_skyline_topic --create --partitions 5

kafka-topics.sh --bootstrap-server localhost:9092 --topic output_skyline_topic --create --partitions 1

kafka-console-producer.sh --bootstrap-server localhost:9092 --topic input_skyline_topic < /home/mike/skyline_project/data/points_D_$1_N_$2.csv
