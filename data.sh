#!/bin/bash
rsync -avz --delete \
  --exclude '_*' \
  --exclude '.*' \
  --exclude 'output/*.parquet' \
  --exclude '*.dat' \
  --exclude '*.gz' \
  --exclude 'metastore_db' \
  --exclude 'logs' \
  commoncrawl@isplab-calcul:~/data/sri/2019-35/. ./data/sri/2019-35

