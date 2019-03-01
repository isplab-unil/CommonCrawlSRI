#!/bin/bash

job=$1

rm -fr ./../output/ && \
spark-submit --py-files ./jobs/commoncrawl.py \
        $job \
        --log_level WARN \
        ./input/test_warc.txt \
        ./spark-warehouse/ \
        --partitions \
        1
