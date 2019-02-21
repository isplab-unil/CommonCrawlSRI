#!/usr/bin/env bash

aws s3 sync ./s3/ s3://commoncrawl-sri && \
aws emr add-steps \
  --cluster-id j-1W91ZGYDB3P4Q \
  --steps file://./emr/commoncrawl.json