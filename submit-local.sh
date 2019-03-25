#!/bin/bash

spark-submit --py-files ./jobs/commoncrawl.py \
        $1 \
        --log_level WARN \
        $2 \
        ./output-local/ \
        --partitions \
        1
