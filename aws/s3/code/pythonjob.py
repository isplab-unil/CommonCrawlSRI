import time

from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(appName="MyTestJob")
    dataTextAll = sc.textFile("s3a://commoncrawl-sri/input/data.csv")
    dataRDD = dataTextAll.map(lambda x: x.split(",")).map(lambda y: (str(y[0]), float(y[1]))).reduceByKey(lambda a, b: a + b)
    dataRDD.saveAsTextFile("s3a://commoncrawl-sri/output/%i" % int(time.time()))
    sc.stop()