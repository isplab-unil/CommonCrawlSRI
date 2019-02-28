#!/usr/bin/env python3

import datetime
import boto3

# Variable initialization
job = 'full.py'
input = '100_warc.txt'
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
                'Name': 'Master Node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'Core Nodes',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 2,
            },
            {
                'Name': 'Spot Nodes',
                'Market': 'SPOT',
                'BidPrice': '0.005',
                'InstanceRole': 'TASK',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
                'AutoScalingPolicy': {
                    'Constraints': {
                        'MinCapacity': 1,
                        'MaxCapacity': 100
                    },
                    'Rules': [
                        {
                            'Name': 'Scale-out rule',
                            'Action': {
                                'SimpleScalingPolicyConfiguration': {
                                    'AdjustmentType': 'CHANGE_IN_CAPACITY',
                                    'ScalingAdjustment': 10,
                                    'CoolDown': 100
                                }
                            },
                            'Trigger': {
                                'CloudWatchAlarmDefinition': {
                                    'ComparisonOperator': 'LESS_THAN_OR_EQUAL',
                                    'EvaluationPeriods': 1,
                                    'MetricName': 'YARNMemoryAvailablePercentage',
                                    'Namespace': 'AWS/ElasticMapReduce',
                                    'Period': 300,
                                    'Threshold': 20,
                                }
                            }
                        },
                        {
                            'Name': 'Scale-in rule',
                            'Action': {
                                'SimpleScalingPolicyConfiguration': {
                                    'AdjustmentType': 'CHANGE_IN_CAPACITY',
                                    'ScalingAdjustment': -5,
                                    'CoolDown': 100
                                }
                            },
                            'Trigger': {
                                'CloudWatchAlarmDefinition': {
                                    'ComparisonOperator': 'GREATER_THAN_OR_EQUAL',
                                    'EvaluationPeriods': 1,
                                    'MetricName': 'YARNMemoryAvailablePercentage',
                                    'Namespace': 'AWS/ElasticMapReduce',
                                    'Period': 300,
                                    'Threshold': 80,
                                }
                            }
                        },
                    ]
                }
            },
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
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
    ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
    AutoScalingRole='EMR_AutoScaling_DefaultRole',
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
