#!/usr/bin/env bash

aws s3 sync ./bootstrap/ s3://commoncrawl-sri/bootstrap/
aws emr create-cluster \
  --applications Name=Ganglia Name=Spark Name=Zeppelin \
  --ec2-attributes file://./config/ec2-attributes.json \
  --release-label emr-5.21.0 \
  --log-uri 's3n://commoncrawl-sri/logs/' \
  --bootstrap-actions Path=s3://commoncrawl-sri/bootstrap/bootstrap.sh \
  --service-role EMR_DefaultRole \
  --enable-debugging \
  --name 'commoncrawl-sri' \
  --instance-groups file://./config/instance-groups.json \
  --configurations file://./config/configurations.json \
  --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
  --region us-east-1 \
  --output text