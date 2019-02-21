#!/bin/bash

rm -fr ./../output/

$SPARK_HOME/bin/spark-submit ./jobs/commoncrawlsri.py \
        --log_level WARN \
        ./input/test_warc.txt ./output/ commoncrawlsri