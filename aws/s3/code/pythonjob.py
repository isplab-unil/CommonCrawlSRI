from __future__ import print_function
from pyspark import SparkContext
import time


if __name__ == "__main__":
    sc = SparkContext(appName="SparkTest")

    sc.textFile("s3a://commoncrawl-sri/input/data.csv") \
        .saveAsTextFile("s3a://commoncrawl-sri/output/%i/data.csv" % int(time.time()))

    sc.stop()
