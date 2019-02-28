#!/usr/bin/env python3

import datetime
import boto3

# Variable initialization
job = 'full.py'
input = '10_warc.txt'
bucket = '%s-%s' % (job.replace('.', '-'), datetime.datetime.now().strftime("%Y-%m-%d-%H-%M"))

# Create Amazon s3 bucket
s3 = boto3.client('s3')
s3.create_bucket(Bucket=bucket)

# Upload files to s3 bucket
s3.upload_file('bootstrap/bootstrap.sh', bucket, 'bootstrap/bootstrap.sh')
s3.upload_file('jobs/commoncrawl.py', bucket, 'jobs/commoncrawl.py')
s3.upload_file('jobs/%s' % job, bucket, 'jobs/%s' % job)
s3.upload_file('input/%s' % input, bucket, 'input/%s' % input)

# Create Amazon emr job
emr = boto3.client('emr')
cluster = emr.run_job_flow(
    Name='CommonCrawlJob',
    LogUri='s3://%s/logs' % bucket,
    ReleaseLabel='emr-5.21.0',
    Applications=[
        {'Name': 'Ganglia'},
        {'Name': 'Spark'},
        {'Name': 'Zeppelin'},
    ],
    Instances={
        'InstanceGroups': [
            {
                'Name': 'Master Instance Group',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm3.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'Master Instance Group',
                'InstanceRole': 'CORE',
                'InstanceType': 'm3.xlarge',
                'InstanceCount': 1,
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
            'Name': 'CommonCrawlJob',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    '/usr/bin/spark-submit',
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
            },

        }
    ],
    JobFlowRole='EMR_EC2_DefaultRole',
    ServiceRole='EMR_DefaultRole',
)

# Wait for the job to complete
waiter = emr.get_waiter('cluster_terminated')
waiter.wait(
    ClusterId=cluster['JobFlowId'],
    WaiterConfig={
        'Delay': 30,
        'MaxAttempts': 60
    }
)
