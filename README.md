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
sqlContext.read.parquet(".").registerTempTable("checksums")

sqlContext.sql("SELECT count(*) FROM commoncrawlsri WHERE exception = TRUE").show()
sqlContext.sql("SELECT count(*) FROM commoncrawlsri WHERE exception = FALSE").show()
sqlContext.sql("SELECT * FROM commoncrawlsri WHERE size(tags) > 0").show()

```


## Amazon EMR

Create key-pair in EC2.

Create EMR cluster associated with key-pair.

```
aws emr create-cluster \
  --applications Name=Hadoop Name=Spark \
  --ec2-attributes '{"InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-06ef506668d98740f"}' \
  --release-label emr-5.17.0 --log-uri 's3n://aws-logs-329700769039-us-east-1/elasticmapreduce/' \
  --bootstrap-actions Path=s3://commoncrawl-sri/bootstrap/bootstrap.sh \
  --steps '[{"Args":["spark-submit","--deploy-mode","cluster","s3://commoncrawl-sri/jobs/pythonjob.py","s3a://commoncrawl-sri/input/data.csv","s3a://commoncrawl-sri/output/"],"Type":"CUSTOM_JAR","ActionOnFailure":"TERMINATE_CLUSTER","Jar":"command-runner.jar","Properties":"","Name":"Spark application"}]' \
  --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m1.medium","Name":"Master Instance Group"}]' \
  --configurations file://./emr-configurations.json \
  --auto-terminate \
  --service-role EMR_DefaultRole \
  --enable-debugging \
  --name 'commoncrawl-sri' \
  --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
  --region us-east-1
```

```json
"Configurations": [{"Classification": "export", "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}}]
```

https://forums.aws.amazon.com/message.jspa?messageID=882723

To create the default autoscaling group, you first have to create a cluster with the advanced wizzard, which will creates the correct default roles.

https://forums.aws.amazon.com/thread.jspa?threadID=244491

## Resources

Interesting summary form an EPFL student:

https://dlab.epfl.ch/2017-09-30-what-i-learned-from-processing-big-data-with-spark/

## Gettint started

We first need to install the command line client for AWS (awscli) and the python SDK for AWS (boto3).

```
pip3 install awscli boto3
```

When configuring the AWS client with your access key, setting the region to us-east-1 is important as the commoncrawl dataset is hosted there.

```
aws configure
AWS Access Key ID [None]: xxxxxxxxxxxxxx
AWS Secret Access Key [None]: xxxxxxxxxxxxxx
Default region name [None]: us-east-1
Default output format [None]:
```

## Download data

```
wget https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2019-09/wet.paths.gz \
    && gzip -dc wet.paths.gz | sed 's@^@s3://commoncrawl/@' \
	> input/2019-09-wet.txt 
```


