from __future__ import print_function
from pyspark import SparkContext
import sys
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: testjob  ", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="MyTestJob")
    dataTextAll = sc.textFile(sys.argv[1])
    dataRDD = dataTextAll.map(lambda x: x.split(",")).map(lambda y: (str(y[0]), float(y[1]))).reduceByKey(lambda a, b: a + b)
    dataRDD.saveAsTextFile(sys.argv[2])
    sc.stop()