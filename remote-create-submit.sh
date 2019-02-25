#!/usr/bin/env bash

name=commoncrawl-sri
region=us-east-1
logs=s3n://${name}/logs/
bootstrap=Path=s3://${name}/bootstrap/bootstrap.sh
job=`cat config/commoncrawlsri.json | sed s/commoncrawl-sri/${name}/g | tr '\n' ' ' | sed 's/ //g'`

echo "Create s3 bucket."
aws s3api create-bucket --bucket $name --region $region --output text

echo "Synchronize s3 bucket."
aws s3 sync ./bootstrap/ s3://${name}/bootstrap/
aws s3 sync ./input/ s3://${name}/input/
aws s3 sync ./jobs/ s3://${name}/jobs/

echo "Start emr cluster."
cluster=$(aws emr create-cluster \
  --applications Name=Ganglia Name=Spark Name=Zeppelin \
  --ec2-attributes '{"InstanceProfile": "EMR_EC2_DefaultRole"}' \
  --release-label emr-5.21.0 \
  --log-uri $logs \
  --bootstrap-actions $bootstrap \
  --service-role EMR_DefaultRole \
  --enable-debugging \
  --name $name \
  --instance-groups file://./config/instance-groups.json \
  --configurations file://./config/configurations.json \
  --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
  --region $region \
  --output text)
aws emr wait cluster-running --cluster-id $cluster

echo "Execute pyspark job."
step=$(aws emr add-steps \
  --cluster-id $cluster \
  --steps $job \
  --output text | awk '{print $2}')

aws emr wait step-complete --cluster-id $cluster --step-id $step

echo "Terminate emr cluster."
aws emr terminate-clusters --cluster-ids $cluster
aws emr wait cluster-terminated --cluster-id $cluster
