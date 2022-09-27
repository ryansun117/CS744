#!/bin/sh
sudo /mnt/data/spark-3.3.0-bin-hadoop3/bin/spark-submit /mnt/data/part3_2/part3_2.py "hdfs://10.10.1.1:9000/data-part3/enwiki-pages-articles" "hdfs://10.10.1.1:9000/part3_2_results.csv"
