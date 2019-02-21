#!/bin/bash

rm -fr spark-warehouse/

$SPARK_HOME/bin/spark-submit ./commoncrawlsri.py \
        --log_level WARN \
        ./input/test_warc.txt ./spark-warehouse/ commoncrawlsri