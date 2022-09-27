import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":

    # parse command line arguments
    if len(sys.argv) != 3:
        print("please specify input and output paths")
        sys.exit()

    ifpath = sys.argv[1]
    ofpath = sys.argv[2]
    print("paths: ", ifpath, ofpath)

    # create spark session
    spark = (SparkSession
      .builder
      .appName("part2")
      .config("spark.driver.memory", "30g")
      .config("spark.executor.memory", "30g")
      .config("spark.task.cpus", 1)
      .master("spark://10.10.1.1:7077")
      .getOrCreate())

    # read in data
    df = spark.read.option("header", True).csv(ifpath)
    # sort by country code and date
    df = df.sort("cca2", "timestamp")
    # show first rows in the log
    df.show()
    # write to output path
    df.write.mode("overwrite").option("header", True).csv(ofpath)
