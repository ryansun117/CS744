#!/bin/sh
sudo /mnt/data/spark-3.3.0-bin-hadoop3/bin/spark-submit /mnt/data/part3_1/part3_1.py "hdfs://10.10.1.1:9000/web-BerkStan.txt" "hdfs://10.10.1.1:9000/part3_1_results.csv"
