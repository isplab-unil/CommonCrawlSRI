# SRI CommonCrawl PySpark

This project aims at measuring the adoption of SRI by analysing the CommonCrawl dataset with PySpark.

## Compatibility and Requirements

Tested with OpenJDK 1.8, Spark 2.4.0 and Python 3.7.

## Setup

Assuming that you are using a Mac, run the following command to install OpenJDK 1.8:

```
brew tap AdoptOpenJDK/openjdk
brew cask install adoptopenjdk8
```

Install Apache Spark by: 
-  Downloading [Spark 2.4](https://www.apache.org/dyn/closer.lua/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz).
-  Decompressing the archive.
-  Placing the uncompressed directory somewhere on your filesystem.
-  Making the $SPARK_HOME environment variable point to your Spark installation.
-  Setting the $PYSPARK_PYTHON environement variable to python3

Retrieve the Python modules by running:

```
pip3 install -r requirements.txt
```

## Get Sample Data

To develop locally, run `get-data.sh`, it will download the sample data. It also writes input files containing
* sample input as `file://` URLs
* all input of one monthly crawl as `s3://` URLs


## Running locally

Submit a job with the following command:

```
$SPARK_HOME/bin/spark-submit ./srijob.py \
	--log_level WARN \
	./input/test_warc.txt commoncrawlsri
```

This will count web server names sent in HTTP response headers for the sample WARC input and store the resulting counts in the SparkSQL table "servernames" in your ... (usually in `./spark-warehouse/servernames`).



## Executing queries

The output table can be accessed via the pyspark shell:

```
$SPARK_HOME/spark/bin/pyspark
```

```
sqlContext.read.parquet("spark-warehouse/commoncrawlsri").registerTempTable("commoncrawlsri")
sqlContext.sql("SELECT count(*) FROM commoncrawlsri WHERE exception = TRUE").show()
sqlContext.sql("SELECT count(*) FROM commoncrawlsri WHERE exception = FALSE").show()
sqlContext.sql("SELECT * FROM commoncrawlsri WHERE size(tags) > 0").show()

```

