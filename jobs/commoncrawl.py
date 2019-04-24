# -*- coding: utf-8 -*-
__author__ = "Bertil Chapuis, Kévin Huguenin, Romain Artru"
__copyright__ = "Copyright 2019, The Information Security and Privacy Lab at the University of Lausanne (https://www.unil.ch/isplab/)"
__credits__ = ["Bertil Chapuis", "Kévin Huguenin", "Romain Artru"]

__version__ = "1"
__license__ = "MIT"
__maintainer__ = "Bertil Chapuis"
__email__ = "bertil.chapuis@unil.ch"

import argparse
import logging
import os
import re
from tempfile import TemporaryFile

import boto3
import botocore
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArchiveLoadFailed

LOGGING_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'

class CommonCrawl:
    """
    A Spark job to analyze SRI adoption on CommonCrawl.
    """

    name = "Commoncrawl"

    partitions = 1000
    log_level = 'INFO'
    logging.basicConfig(level=log_level, format=LOGGING_FORMAT)

    schema = None

    def parse_arguments(self):
        """ Returns the parsed arguments from the command line """
        description = self.name
        if self.__doc__ is not None:
            description += " - "
            description += self.__doc__
        arg_parser = argparse.ArgumentParser(description=description)
        arg_parser.add_argument("input",
                                help="Input path")
        arg_parser.add_argument("output",
                                help="Output path")
        arg_parser.add_argument("--partitions",
                                type=int,
                                default=self.partitions,
                                help="Number of partitions")
        arg_parser.add_argument("--local_temp_dir", default=None,
                                help="Local temporary directory, used to"
                                     " buffer content from S3")
        arg_parser.add_argument("--log_level", default=self.log_level,
                                help="Logging level")
        args = arg_parser.parse_args()
        self.init_logging(args.log_level)
        return args

    def init_logging(self, level=None):
        if level is None:
            level = self.log_level
        else:
            self.log_level = level
        logging.basicConfig(level=level, format=LOGGING_FORMAT)

    def get_logger(self, spark_context=None):
        """Get logger from SparkContext or (if None) from logging module"""
        if spark_context is None:
            return logging.getLogger(self.name)
        else:
            return spark_context._jvm.org.apache.log4j.LogManager.getLogger(self.name)

    def init_aggregators(self, sc):
        self.records_processed = sc.accumulator(0)
        self.warc_input_processed = sc.accumulator(0)
        self.warc_input_failed = sc.accumulator(0)

    def log_aggregators(self, sc):
        self.log_aggregator(sc, self.warc_input_processed, 'WARC input files processed = {}')
        self.log_aggregator(sc, self.warc_input_failed, 'WARC input files failed = {}')
        self.log_aggregator(sc, self.records_processed, 'WARC records processed = {}')

    def log_aggregator(self, sc, agg, descr):
        self.get_logger(sc).info(descr.format(agg.value))

    def run(self):
        self.args = self.parse_arguments()

        conf = SparkConf().setAll((
            ("spark.task.maxFailures", "10"),
            ("spark.locality.wait", "20s"),
            ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
        ))

        sc = SparkContext(appName=self.name, conf=conf)
        sqlc = SQLContext(sparkContext=sc)

        self.init_aggregators(sc)
        self.run_job(sc, sqlc)
        self.log_aggregators(sc)

        sc.stop()

    def run_job(self, sc, sqlc):
        output = sc.textFile(self.args.input, minPartitions=self.args.partitions) \
            .flatMap(self.process_warc)
        sqlc.createDataFrame(output, schema=self.schema) \
            .write \
            .format("parquet") \
            .option("compression", "gzip") \
            .option("path", self.args.output) \
            .mode("overwrite") \
            .saveAsTable("output")

    def process_warc(self, warc):
        s3pattern = re.compile('^s3://([^/]+)/(.+)')
        base_dir = os.path.abspath(os.path.dirname(__file__))

        # S3 client (not thread-safe, initialize outside parallelized loop)
        no_sign_request = botocore.client.Config(signature_version=botocore.UNSIGNED)
        s3client = boto3.client('s3', config=no_sign_request)

        warc_id = int(re.findall(r'-(\d{5})[\.|-]', warc)[0])

        self.warc_input_processed.add(1)
        if warc.startswith('s3://'):
            self.get_logger().info('Reading from S3 {}'.format(warc))
            s3match = s3pattern.match(warc)
            if s3match is None:
                self.get_logger().error("Invalid S3 URI: " + warc)
                return
            bucketname = s3match.group(1)
            path = s3match.group(2)
            warctemp = TemporaryFile(mode='w+b', dir=self.args.local_temp_dir)
            try:
                s3client.download_fileobj(bucketname, path, warctemp)
            except botocore.client.ClientError as exception:
                self.get_logger().error('Failed to download {}: {}'.format(warc, exception))
                self.warc_input_failed.add(1)
                warctemp.close()
                return
            warctemp.seek(0)
            stream = warctemp
        elif warc.startswith('hdfs://'):
            self.get_logger().error("HDFS input not implemented: " + warc)
            return
        else:
            self.get_logger().info('Reading local stream {}'.format(warc))
            if warc.startswith('file:'):
                warc = warc[5:]
            warc = os.path.join(base_dir, warc)

            try:
                stream = open(warc, 'rb')
            except IOError as exception:
                self.get_logger().error('Failed to open {}: {}'.format(warc, exception))
                self.warc_input_failed.add(1)
                return
        try:
            for record in ArchiveIterator(stream):
                for result in self.process_record(warc_id, record):
                    yield result
                self.records_processed.add(1)
        except ArchiveLoadFailed as exception:
            self.warc_input_failed.add(1)
            self.get_logger().error('Invalid WARC: {} - {}'.format(warc, exception))
        finally:
            stream.close()
