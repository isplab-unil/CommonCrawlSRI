#!/usr/bin/env bash

aws emr create-cluster \
  --applications Name=Ganglia Name=Spark Name=Zeppelin \
  --ec2-attributes '{"InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-06ef506668d98740f","KeyName":"commoncrawl-sri"}' \
  --release-label emr-5.21.0 \
  --log-uri 's3n://commoncrawl-sri/logs/' \
  --bootstrap-actions Path=s3://commoncrawl-sri/bootstrap/install_python_modules.sh \
  --service-role EMR_DefaultRole \
  --enable-debugging \
  --name 'commoncrawl-sri' \
  --instance-groups '[{"InstanceCount":2,"InstanceGroupType":"CORE","InstanceType":"m3.xlarge","Name":"Core Instance Group"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.xlarge","Name":"Master Instance Group"}]' \
  --configurations '[{"Classification":"spark","Properties":{},"Configurations":[]}]' \
  --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
  --region us-east-1
