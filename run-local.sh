#!/bin/bash

rm -fr ./../output/ && \
spark-submit ./jobs/commoncrawlsri.py \
        --log_level WARN \
        ./input/test_warc.txt \
        ./spark-warehouse/ commoncrawlsri