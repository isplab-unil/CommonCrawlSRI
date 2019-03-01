#!/usr/bin/env python3

import datetime
import boto3

# Variable initialization
job = 'full.py'
input = '100_warc.txt'
name = '%s-%s' % (datetime.datetime.now().strftime("%Y-%m-%d-%H-%M"), job.replace('.', '-'))

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
                'BidPrice': '0.08',
                'InstanceRole': 'TASK',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 2,
                'AutoScalingPolicy': {
                    'Constraints': {
                        'MinCapacity': 2,
                        'MaxCapacity': 10
                    },
                    'Rules': [
                        {
                            'Name': 'Scale-out rule 1',
                            'Action': {
                                'SimpleScalingPolicyConfiguration': {
                                    'AdjustmentType': 'CHANGE_IN_CAPACITY',
                                    'ScalingAdjustment': 1,
                                    'CoolDown': 300
                                }
                            },
                            'Trigger': {
                                'CloudWatchAlarmDefinition': {
                                    'MetricName': 'YARNMemoryAvailablePercentage',
                                    'ComparisonOperator': 'LESS_THAN',
                                    'Statistic': 'AVERAGE',
                                    'Period': 300,
                                    'Dimensions': [
                                        {
                                            'Value': '${emr.clusterId}',
                                            'Key': 'JobFlowId'
                                        }
                                    ],
                                    'EvaluationPeriods': 1,
                                    'Unit': 'PERCENT',
                                    'Namespace': 'AWS/ElasticMapReduce',
                                    'Threshold': 15,
                                }
                            }
                        },
                        {
                            'Name': 'Scale-out rule 2',
                            'Action': {
                                'SimpleScalingPolicyConfiguration': {
                                    'AdjustmentType': 'CHANGE_IN_CAPACITY',
                                    'ScalingAdjustment': 1,
                                    'CoolDown': 300
                                }
                            },
                            'Trigger': {
                                'CloudWatchAlarmDefinition': {
                                    'MetricName': 'ContainerPendingRatio',
                                    'ComparisonOperator': 'GREATER_THAN',
                                    'Statistic': 'AVERAGE',
                                    'Period': 300,
                                    'Dimensions': [
                                        {
                                            'Value': '${emr.clusterId}',
                                            'Key': 'JobFlowId'
                                        }
                                    ],
                                    'EvaluationPeriods': 1,
                                    'Unit': 'COUNT',
                                    'Namespace': 'AWS/ElasticMapReduce',
                                    'Threshold': 0.75,
                                }
                            }
                        },
                        {
                            'Name': 'Scale-in rule',
                            'Action': {
                                'SimpleScalingPolicyConfiguration': {
                                    'AdjustmentType': 'CHANGE_IN_CAPACITY',
                                    'ScalingAdjustment': -1,
                                    'CoolDown': 300
                                }
                            },
                            'Trigger': {
                                'CloudWatchAlarmDefinition': {
                                    'MetricName': 'YARNMemoryAvailablePercentage',
                                    'ComparisonOperator': 'GREATER_THAN',
                                    'Statistic': 'AVERAGE',
                                    'Period': 300,
                                    'Dimensions': [
                                        {
                                            'Value': '${emr.clusterId}',
                                            'Key': 'JobFlowId'
                                        }
                                    ],
                                    'EvaluationPeriods': 1,
                                    'Unit': 'PERCENT',
                                    'Namespace': 'AWS/ElasticMapReduce',
                                    'Threshold': 75,
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
                'Path': 's3://%s/bootstrap/bootstrap.sh' % name,
            }
        },
    ],
    Steps=[
        {
            'Name': name,
            'ActionOnFailure': 'CONTINUE',
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
