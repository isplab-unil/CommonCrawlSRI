#!/bin/bash

rm -fr spark-warehouse/

$SPARK_HOME/bin/spark-submit ./sparksri.py \
        --log_level WARN \
        ./input/test_warc.txt srijob