#!/usr/bin/env bash

aws s3 sync ./s3/ s3://commoncrawl-sri && \
aws emr create-cluster \
  --applications Name=Hadoop Name=Spark \
  --ec2-attributes '{"InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-06ef506668d98740f","KeyName":"commoncrawl-sri"}' \
  --release-label emr-5.21.0 --log-uri 's3n://commoncrawl-sri/logs/' \
  --bootstrap-actions Path=s3://commoncrawl-sri/bootstrap/install_python_modules.sh \
  --steps file://./emr/steps.json \
  --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m1.medium","Name":"Master Instance Group"},{"InstanceCount":2,"InstanceGroupType":"CORE","InstanceType":"m1.medium","Name":"Core Instance Group"}]' \
  --configurations file://./emr/configurations.json \
  --auto-terminate \
  --service-role EMR_DefaultRole \
  --enable-debugging \
  --name 'commoncrawl-sri' \
  --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
  --region us-east-1