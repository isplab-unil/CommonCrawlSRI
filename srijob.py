import argparse
import logging
import os
import re

from io import BytesIO
from tempfile import TemporaryFile

import boto3
import botocore

from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArchiveLoadFailed

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

LOGGING_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'

class SRIJob(object):
    """
    A custom spark job to analyze SRI adoption.
    """

    name = "SRIJob"

    @staticmethod
    def reduce_by_key_func(a, b):
        return a + b

    @staticmethod
    def is_wet_text_record(record):
        """Return true if WARC record is a WET text/plain record"""
        return (record.rec_type == 'conversion' and
                record.content_type == 'text/plain')

    @staticmethod
    def is_wat_json_record(record):
        """Return true if WARC record is a WAT record"""
        return (record.rec_type == 'metadata' and
                record.content_type == 'application/json')

    @staticmethod
    def is_html(record):
        """Return true if (detected) MIME type of a record is HTML"""
        html_types = ['text/html', 'application/xhtml+xml']
        if (('WARC-Identified-Payload-Type' in record.rec_headers) and
                (record.rec_headers['WARC-Identified-Payload-Type'] in
                 html_types)):
            return True
        for html_type in html_types:
            if html_type in record.content_type:
                return True
        return False

    def parse_arguments(self):
        """ Returns the parsed arguments from the command line """

        description = self.name
        if self.__doc__ is not None:
            description += " - "
            description += self.__doc__
        arg_parser = argparse.ArgumentParser(description=description)

        arg_parser.add_argument("input",
                                help="Path to file listing input paths")
        arg_parser.add_argument("output",
                                help="Name of output table (saved in spark.sql.warehouse.dir)")
        arg_parser.add_argument("--num_input_partitions", type=int,
                                default=self.num_input_partitions,
                                help="Number of input splits/partitions")
        arg_parser.add_argument("--num_output_partitions", type=int,
                                default=self.num_output_partitions,
                                help="Number of output partitions")
        arg_parser.add_argument("--output_format", default="parquet",
                                help="Output format: parquet (default),"
                                " orc, json, csv")
        arg_parser.add_argument("--output_compression", default="gzip",
                                help="Output compression codec: None,"
                                " gzip/zlib (default), snappy, lzo, etc.")
        arg_parser.add_argument("--local_temp_dir", default=None,
                                help="Local temporary directory, used to"
                                " buffer content from S3")
        arg_parser.add_argument("--log_level", default=self.log_level,
                                help="Logging level")

        self.add_arguments(arg_parser)
        args = arg_parser.parse_args()
        if not self.validate_arguments(args):
            raise Exception("Arguments not valid")
        self.init_logging(args.log_level)

        return args

    def add_arguments(self, parser):
        pass

    def validate_arguments(self, args):
        if "orc" == args.output_format and "gzip" == args.output_compression:
            # gzip for Parquet, zlib for ORC
            args.output_compression = "zlib"
        return True

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
        return spark_context._jvm.org.apache.log4j.LogManager \
            .getLogger(self.name)



    def log_aggregator(self, sc, agg, descr):
        self.get_logger(sc).info(descr.format(agg.value))

    def log_aggregators(self, sc):
        self.log_aggregator(sc, self.warc_input_processed,
                            'WARC/WAT/WET input files processed = {}')
        self.log_aggregator(sc, self.warc_input_failed,
                            'WARC/WAT/WET input files failed = {}')
        self.log_aggregator(sc, self.records_processed,
                            'WARC/WAT/WET records processed = {}')



    def run(self):
        self.args = self.parse_arguments()

        conf = SparkConf().setAll((
            ("spark.task.maxFailures", "10"),
            ("spark.locality.wait", "20s"),
            ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
        ))
        
        sc = SparkContext(
            appName=self.name,
            conf=conf)
        sqlc = SQLContext(sparkContext=sc)

        self.init_accumulators(sc)

        self.run_job(sc, sqlc)

        sc.stop()

    def run_job(self, sc, sqlc):
        input_data = sc.textFile(self.args.input, minPartitions=self.args.num_input_partitions)

        output = input_data.mapPartitionsWithIndex(self.process_warcs) \
            .reduceByKey(self.reduce_by_key_func)

        sqlc.createDataFrame(output, schema=self.output_schema) \
            .coalesce(self.args.num_output_partitions) \
            .write \
            .format(self.args.output_format) \
            .option("compression", self.args.output_compression) \
            .saveAsTable(self.args.output)

        self.log_aggregators(sc)

    def process_warcs(self, id_, iterator):
        s3pattern = re.compile('^s3://([^/]+)/(.+)')
        base_dir = os.path.abspath(os.path.dirname(__file__))

        # S3 client (not thread-safe, initialize outside parallelized loop)
        no_sign_request = botocore.client.Config(
            signature_version=botocore.UNSIGNED)
        s3client = boto3.client('s3', config=no_sign_request)

        for uri in iterator:
            self.warc_input_processed.add(1)
            if uri.startswith('s3://'):
                self.get_logger().info('Reading from S3 {}'.format(uri))
                s3match = s3pattern.match(uri)
                if s3match is None:
                    self.get_logger().error("Invalid S3 URI: " + uri)
                    continue
                bucketname = s3match.group(1)
                path = s3match.group(2)
                warctemp = TemporaryFile(mode='w+b',
                                         dir=self.args.local_temp_dir)
                try:
                    s3client.download_fileobj(bucketname, path, warctemp)
                except botocore.client.ClientError as exception:
                    self.get_logger().error(
                        'Failed to download {}: {}'.format(uri, exception))
                    self.warc_input_failed.add(1)
                    warctemp.close()
                    continue
                warctemp.seek(0)
                stream = warctemp
            elif uri.startswith('hdfs://'):
                self.get_logger().error("HDFS input not implemented: " + uri)
                continue
            else:
                self.get_logger().info('Reading local stream {}'.format(uri))
                if uri.startswith('file:'):
                    uri = uri[5:]
                uri = os.path.join(base_dir, uri)
                try:
                    stream = open(uri, 'rb')
                except IOError as exception:
                    self.get_logger().error(
                        'Failed to open {}: {}'.format(uri, exception))
                    self.warc_input_failed.add(1)
                    continue

            no_parse = (not self.warc_parse_http_header)
            try:
                for record in ArchiveIterator(stream, no_record_parse=no_parse):
                    for res in self.process_record(record):
                        yield res
                    self.records_processed.add(1)
            except ArchiveLoadFailed as exception:
                self.warc_input_failed.add(1)
                self.get_logger().error(
                    'Invalid WARC: {} - {}'.format(uri, exception))
            finally:
                stream.close()

    def process_record(self, record):
        raise NotImplementedError('Processing record needs to be customized')

if __name__ == "__main__":
    job = SRIJob()
    job.run()


