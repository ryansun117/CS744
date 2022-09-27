#!/bin/sh
sudo /mnt/data/spark-3.3.0-bin-hadoop3/bin/spark-submit /mnt/data/part2/part2.py "hdfs://10.10.1.1:9000/export.csv" "hdfs://10.10.1.1:9000/export_result.csv"
