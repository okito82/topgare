#!/usr/bin/env bash

./bin/spark-submit \
 --class org.example.topgare.topgare \
 --Master local[*] \
 /home/cloudera/Documents/JobSpark/topgare-1.0-SNAPSHOT.jar