#!/usr/bin/env bash

aws s3 sync ./input/ s3://commoncrawl-sri/input/
aws s3 sync ./jobs/ s3://commoncrawl-sri/jobs/
aws emr add-steps \
  --cluster-id j-2FHD6HJKVSB1C \
  --steps file://./config/commoncrawlsri.json \
  --output text