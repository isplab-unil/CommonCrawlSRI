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

from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, BinaryType

LOGGING_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'


class SparkSRI(object):
    """
    A custom spark job to analyze SRI adoption.
    """

    name = "SparkSRI"

    log_level = 'INFO'
    logging.basicConfig(level=log_level, format=LOGGING_FORMAT)

    output_schema = StructType([
        StructField("uri", StringType(), True),
        StructField("encoding", StringType(), True),
        StructField("content", BinaryType(), True),
        StructField("tags", ArrayType(StructType([
            StructField("name", StringType(), True),
            StructField("target", StringType(), True),
            StructField("integrity", StringType(), True),
        ])), True),

    ])

    @staticmethod
    def is_response(record):
        """Return true if the record is a response"""
        return record.rec_type == 'response'

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

        args = arg_parser.parse_args()

        if "orc" == args.output_format and "gzip" == args.output_compression:
            # gzip for Parquet, zlib for ORC
            args.output_compression = "zlib"

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
        output = sc.textFile(self.args.input) \
            .flatMap(self.process_warc)

        sqlc.createDataFrame(output, schema=self.output_schema) \
            .write \
            .format(self.args.output_format) \
            .option("compression", self.args.output_compression) \
            .saveAsTable(self.args.output)

    def process_warc(self, uri):
        s3pattern = re.compile('^s3://([^/]+)/(.+)')
        base_dir = os.path.abspath(os.path.dirname(__file__))

        # S3 client (not thread-safe, initialize outside parallelized loop)
        no_sign_request = botocore.client.Config(signature_version=botocore.UNSIGNED)
        s3client = boto3.client('s3', config=no_sign_request)

        self.warc_input_processed.add(1)
        if uri.startswith('s3://'):
            self.get_logger().info('Reading from S3 {}'.format(uri))
            s3match = s3pattern.match(uri)
            if s3match is None:

                self.get_logger().error("Invalid S3 URI: " + uri)
                return
            bucketname = s3match.group(1)
            path = s3match.group(2)
            warctemp = TemporaryFile(mode='w+b', dir=self.args.local_temp_dir)
            try:
                s3client.download_fileobj(bucketname, path, warctemp)
            except botocore.client.ClientError as exception:
                self.get_logger().error('Failed to download {}: {}'.format(uri, exception))
                self.warc_input_failed.add(1)
                warctemp.close()
                return
            warctemp.seek(0)
            stream = warctemp
        elif uri.startswith('hdfs://'):
            self.get_logger().error("HDFS input not implemented: " + uri)
            return
        else:
            self.get_logger().info('Reading local stream {}'.format(uri))
            if uri.startswith('file:'):
                uri = uri[5:]
            uri = os.path.join(base_dir, uri)
            try:
                stream = open(uri, 'rb')
            except IOError as exception:
                self.get_logger().error('Failed to open {}: {}'.format(uri, exception))
                self.warc_input_failed.add(1)
                return

        try:
            for record in ArchiveIterator(stream):
                for result in self.process_record(record):
                    yield result
                self.records_processed.add(1)
        except ArchiveLoadFailed as exception:
            self.warc_input_failed.add(1)
            self.get_logger().error('Invalid WARC: {} - {}'.format(uri, exception))
        finally:
            stream.close()

    def process_record(self, record):
        if 'response' == record.rec_type:
            html = record.content_stream().read()

            tags = []
            encoding = None
            content = None
            if b'integrity=' in html:
                encoding = EncodingDetector.find_declared_encoding(html, is_html=True)
                soup = BeautifulSoup(html, "lxml", from_encoding=encoding)
                tags = [(tag.name, tag.get('src') or tag.get('href'), tag.get("integrity")) for tag in soup(["link", "script"]) if tag.get("integrity") is not None]
                if len(tags) > 0:
                    content = html

            yield [record.rec_headers.get_header('WARC-Target-URI'), encoding, content, tags]


if __name__ == "__main__":
    job = SparkSRI()
    job.run()
