#!/usr/bin/env bash

aws s3 sync ./s3/ s3://commoncrawl-sri && \
aws emr add-steps \
  --cluster-id j-1XRUS8FEX6DHM \
  --steps file://./emr/steps.json