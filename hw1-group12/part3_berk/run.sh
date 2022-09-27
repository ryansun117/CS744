#!/bin/sh
sudo /mnt/data/spark-3.3.0-bin-hadoop3/bin/spark-submit /mnt/data/part3_berk/part3_berk.py "hdfs://10.10.1.1:9000/web-BerkStan.txt" "hdfs://10.10.1.1:9000/part3_small_results.csv"
