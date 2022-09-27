import sys
from pyspark.sql import SparkSession
from operator import add

## some constant to be used in the program
# Task 1: PageRank with default RDD number of partitions , completion time: 30 min
TASK_NO = 1

# Task 2: PageRank with variable RDD partitions
# TASK_NO = 2
# TASK_2_NUM_PARTITION = 8 # completion time: 24 min
# TASK_2_NUM_PARTITION = 64 # completion time: 24 min
# TASK_2_NUM_PARTITION = 512 # completion time: 31 min

# Task 3: PageRank with persist RDD as in-memory objects
# TASK_NO = 3 # cache, no worker failure 23 min

# a helpter function to parse the input file
def sepLine(line):
  tokens = line.split()
  if len(tokens) != 2:
    return "<ignore>", "<ignore>"
  return tokens[0], tokens[1]

# a helper function to calculate the contributions of the links
def calcContribs(neighbors, rank):
  out_degree = len(neighbors)
  for neighbor in neighbors:
    yield (neighbor, rank / out_degree)

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
    .appName("part3")
    .config("spark.driver.memory", "30g")
    .config("spark.executor.memory", "30g")
    .config("spark.task.cpus", 1)
    .config("spark.pyspark.python", "/usr/bin/python3.7")
    .config("spark.pyspark.driver.python", "/usr/bin/python3.7")
    .master("spark://10.10.1.1:7077")
    .getOrCreate()
  )

  # read in data, now each line in lines is a link (u, v)
  lines = (spark
    .read
    .text(ifpath)
    .rdd
    .map(lambda r: r[0])
    .map(lambda r: sepLine(r))
    .filter(lambda p:p[0] != "<ignore>")
    .sortByKey()
  )

  if TASK_NO == 1:
    links = lines.groupByKey() # now each line in links is (u, [v1, v2, ...])
    ranks = links.map(lambda kv: (kv[0], 1.0)) # initialize the ranks
  elif TASK_NO == 2:
    links = lines.groupByKey(numPartitions = TASK_2_NUM_PARTITION)
    ranks = links.map(lambda kv: (kv[0], 1.0), preservesPartitioning = True)

  elif TASK_NO == 3:
    links = lines.groupByKey(numPartitions = TASK_2_NUM_PARTITION).cache()
    ranks = links.map(lambda kv: (kv[0], 1.0), preservesPartitioning = True)
    # when doing task no 3, cache the "links" variable

  num_it = 10 # run 10 iterations

  # main computation of the page rank algorithm
  for _ in range(num_it):
    if TASK_NO == 1:
      contribs = links.join(ranks).flatMap(
        lambda kv: calcContribs(neighbors=kv[1][0], rank=kv[1][1])
      )
      ranks = contribs.reduceByKey(add).mapValues(
        lambda rank: rank * 0.85 + 0.15
      )
    elif TASK_NO >= 2:
      contribs = links.join(ranks, numPartitions = TASK_2_NUM_PARTITION).flatMap(
        lambda kv: calcContribs(neighbors=kv[1][0], rank=kv[1][1])
      )
      ranks = (contribs.reduceByKey(add, numPartitions = TASK_2_NUM_PARTITION)
        .mapValues(
          lambda rank: rank * 0.85 + 0.15
        )
      )
  if TASK_NO == 1:
    ranks = ranks.sortByKey()
  elif TASK_NO >= 2:
    ranks = ranks.sortByKey(numPartitions = TASK_2_NUM_PARTITION)

  # write to output path
  if TASK_NO == 1:
    (ranks.
      toDF(schema=['url', 'pagerank'])
      .write
      .mode("overwrite")
      .option("header", True)
      .csv(ofpath)
    )
  elif TASK_NO >= 2:
    (ranks.
      toDF(schema=['url', 'pagerank'])
      .write
      .coalesce(TASK_2_NUM_PARTITION)
      .mode("overwrite")
      .option("header", True)
      .csv(ofpath)
    )
