#!/usr/bin/env python3

import datetime
import boto3
import argparse

# Parse command line arguments
parser = argparse.ArgumentParser(description='Submit a pyspark job to aws emr.')
parser.add_argument('job', help='a python script', required=True)
parser.add_argument('input', help='an input file', required=True)
parser.add_argument('partitions', type=int, help='a number of partitions', required=True)
parser.add_argument('master', type=int, help='a number of master nodes', required=True)
parser.add_argument('core', type=int, help='a number of core nodes', required=True)
parser.add_argument('task', type=int, help='a number of task nodes', required=True)
parser.add_argument('bid_price', type=int, help='a maximal bid price for task nodes', required=True)
args = parser.parse_args()

# Write startup script
with open('start-remote.sh', 'w') as file:
    file.write("#!/bin/bash\n")
    file.write("./submit-remote.py %s %s %s %s %s %s\n" % (
    args.job, args.input, args.partitions, args.master, args.core, args.task))

# Initialize the job name
name = '%s-%s-%s-m%s-c%s-t%s-p%s' % (
    datetime.datetime.now().strftime("%Y-%m-%d-%H-%M"),
    args.job.replace(".", "-").replace("_", "-"),
    args.input.replace(".", "-").replace("_", "-"),
    args.master,
    args.core,
    args.task,
    args.partitions)

# Print the cluster and bucket name
print("Job name: %s" % name)

# Create Amazon s3 bucket
s3 = boto3.client('s3')
s3.create_bucket(Bucket=name)

# Upload files to s3 bucket for reproducibility
s3.upload_file('start-remote.sh', name, 'start-remote.sh')
s3.upload_file('submit-remote.py', name, 'submit-remote.py')
s3.upload_file('bootstrap/bootstrap.sh', name, 'bootstrap/bootstrap.sh')
s3.upload_file('jobs/commoncrawl.py', name, 'jobs/commoncrawl.py')
s3.upload_file('jobs/%s' % args.job, name, 'jobs/%s' % args.job)
s3.upload_file('input/%s' % args.input, name, 'input/%s' % args.input)

# Create Amazon emr job
emr = boto3.client('emr')
cluster = emr.run_job_flow(
    Name=name,
    LogUri='s3://%s/logs' % name,
    ReleaseLabel='emr-5.21.0',
    Applications=[
        {'Name': 'Spark'},
        {'Name': 'Ganglia'},  # Monitoring
    ],
    Instances={
        'InstanceGroups': [
            {
                'Name': 'Master Node',  # The master node is only used for the coordination
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': args.master,
            },
            {
                'Name': 'Core Nodes',  # The core nodes are used for HDFS persistence
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': args.core,
            },
            {
                'Name': 'Task Nodes',  # The task nodes are only used to compute results
                'Market': 'SPOT',  # The spot instance are ephemeral and should not be used to persist data
                'BidPrice': args.bid_price,  # The bid price can be guessed using the ec2 console
                'InstanceRole': 'TASK',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': args.task,
            }
        ],
        'Ec2KeyName': 'commoncrawl-sri',  # A key pair must be created from the ec2 console
        'KeepJobFlowAliveWhenNoSteps': False,  # Shutdown the cluster after the execution of the job
        'TerminationProtected': False,  # Shutdown the cluster after the execution of the job
        'Ec2SubnetId': 'subnet-06ef506668d98740f',  # A VPC subnet must be created from the ec2 console
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
                    's3://%s/jobs/commoncrawlsri.py' % name,
                    's3://%s/input/%s' % (name, args.input),
                    's3://%s/output/' % name,
                    '--partitions',
                    str(args.partitions),
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

print("Finished!")
