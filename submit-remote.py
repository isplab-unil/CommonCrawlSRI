#!/usr/bin/env python3

import datetime
import boto3

# Variable initialization
job = 'full.py'
input = '20_warc.txt'
master = 1
core = 3
task = 0
name = '%s-%s-%s-m%s-c%s-t%s' % (
    datetime.datetime.now().strftime("%Y-%m-%d-%H-%M"), job.replace(".", "-").replace("_", "-"), input.replace(".", "-").replace("_", "-"), master, core, task)

# Create Amazon s3 bucket
s3 = boto3.client('s3')
s3.create_bucket(Bucket=name)

# Upload files to s3 bucket
s3.upload_file('bootstrap/bootstrap.sh', name, 'bootstrap/bootstrap.sh')
s3.upload_file('jobs/commoncrawl.py', name, 'jobs/commoncrawl.py')
s3.upload_file('jobs/%s' % job, name, 'jobs/%s' % job)
s3.upload_file('input/%s' % input, name, 'input/%s' % input)

# Create Amazon emr job
emr = boto3.client('emr')
cluster = emr.run_job_flow(
    Name=name,
    LogUri='s3://%s/logs' % name,
    ReleaseLabel='emr-5.21.0',
    Applications=[
        {'Name': 'Spark'},
        # {'Name': 'Ganglia'},
        # {'Name': 'Zeppelin'},
    ],
    Instances={
        'InstanceGroups': [
            {
                'Name': 'Master Node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': master,
            },
            {
                'Name': 'Core Nodes',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': core,
            },
            # {
            #     'Name': 'Task Nodes',
            #     'Market': 'SPOT',
            #     'BidPrice': '0.08',
            #     'InstanceRole': 'TASK',
            #     'InstanceType': 'm5.xlarge',
            #     'InstanceCount': task,
            # }
        ],
        # The key pair must be created from the ec2 console
        'Ec2KeyName': 'commoncrawl-sri',
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
        'Ec2SubnetId': 'subnet-06ef506668d98740f',
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
                'Path': 's3://%s/bootstrap/bootstrap.sh' % name,
            }
        },
    ],
    Steps=[
        {
            'Name': name,
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    '/usr/bin/spark-submit',
                    '--py-files',
                    's3://%s/jobs/commoncrawl.py' % name,
                    '--deploy-mode',
                    'cluster',
                    '--master',
                    'yarn',
                    '--conf',
                    'spark.yarn.submit.waitAppCompletion=true',
                    's3://%s/jobs/full.py' % name,
                    's3://%s/input/%s' % (name, input),
                    's3://%s/output/' % name,
                    'output'
                ],
            },

        }
    ],
    JobFlowRole='EMR_EC2_DefaultRole',
    ServiceRole='EMR_DefaultRole',
    ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
    AutoScalingRole='EMR_AutoScaling_DefaultRole',
    VisibleToAllUsers=True,
)

# Wait for the job to complete (max 1 day)
waiter = emr.get_waiter('cluster_terminated')
waiter.wait(
    ClusterId=cluster['JobFlowId'],
    WaiterConfig={
        'Delay': 60,
        'MaxAttempts': 1440
    }
)
