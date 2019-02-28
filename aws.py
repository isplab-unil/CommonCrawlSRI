#!/usr/bin/env python3

import time
import boto3

# Variable initialization
timestamp = int(time.time() * 1000)
job = 'all.py'
input = '10_warc.txt'
bucket = 'commoncrawl-%s-%s' % (job, timestamp)

# Create Amazon s3 bucket
s3 = boto3.client('s3')
s3.create_bucket(Bucket=bucket)

# Upload files to s3 bucket
s3.upload_file('bootstrap/bootstrap.sh', bucket, 'bootstrap/bootstrap.sh')
s3.upload_file('input/%s' % input, bucket, 'input/%s' % input)
s3.upload_file('jobs/commoncrawl.py', bucket, 'jobs/commoncrawl.py')
s3.upload_file('jobs/%s' % job, bucket, 'jobs/%s' % job)

# Create Amazon emr job
emr = boto3.client('emr')
cluster = emr.run_job_flow(
    Name='CommonCrawlJob',
    LogUri='s3://$s/logs' % bucket,
    ReleaseLabel='emr-5.21.0',
    Applications=[
        {'Name': 'Ganglia'},
        {'Name': 'Spark'},
        {'Name': 'Zeppelin'},
    ],
    Instances={
        'InstanceGroups': [
            {
                'InstanceCount': 1,
                'InstanceGroupType': 'MASTER',
                'InstanceType': 'm3.xlarge',
                'Name': 'Master Instance Group'
            },
            {
                'InstanceCount': 1,
                'InstanceGroupType': 'CORE',
                'InstanceType': 'm3.xlarge',
                'Name': 'Core Instance Group'
            },
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    Configurations=[
        {
            'Classification': 'spark-env',
            'Configurations': [
                {
                    'Classification': 'export',
                    'Properties': {
                        'PYSPARK_PYTHON': 'python36',
                        'PYSPARK_PYTHON_DRIVER': 'python36'
                    }
                }
            ]
        }
    ],
    BootstrapActions=[
        {
            'Name': 'BoostrapScript',
            'ScriptBootstrapAction': {
                'Path': 's3://%s/bootstrap/bootstrap.sh' % bucket,
            }
        },
    ],
    Steps=[
        {
            'Type': 'spark',
            'ActionOnFailure': 'CONTINUE',
            'Name': 'CommonCrawlJob',
            'Args': [
                '--py-files',
                's3://%s/jobs/commoncrawl.py' % bucket,
                '--deploy-mode',
                'cluster',
                '--master',
                'yarn',
                '--conf',
                'spark.yarn.submit.waitAppCompletion=true',
                's3://%s/jobs/full.py' % bucket,
                's3://%s/input/10_warc.txt' % bucket,
                's3://%s/output/' % bucket,
                'output'
            ],
        }
    ],
    JobFlowRole='EMR_EC2_DefaultRole',
    ServiceRole='EMR_DefaultRole',
    ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
    Region='us-east-1'
)

# Wait for the job to complete
waiter = emr.get_waiter('cluster_terminated')
waiter.wait(
    ClusterId=cluster.JobFlowId,
    WaiterConfig={
        'Delay': 30,
        'MaxAttempts': 60
    }
)
