#!/usr/bin/env bash

aws s3 sync ./bootstrap/ s3://commoncrawl-sri/bootstrap/ && \
aws s3 sync ./input/ s3://commoncrawl-sri/input/ && \
aws s3 sync ./jobs/ s3://commoncrawl-sri/jobs/ && \
aws emr add-steps \
  --cluster-id j-1W91ZGYDB3P4Q \
  --steps file://./../config/commoncrawlsri.json