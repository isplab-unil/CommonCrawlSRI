import argparse
import logging
import os
import re
from tempfile import TemporaryFile

import boto3
import botocore
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
from warcio.bufferedreaders import ChunkedDataReader, BufferedReader

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BinaryType, BooleanType
from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArchiveLoadFailed

LOGGING_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'


class CommonCrawlSRI():
    """
    A Spark job to analyze SRI adoption on CommonCrawl.
    """

    name = "CommonCrawlSRI"

    log_level = 'INFO'
    logging.basicConfig(level=log_level, format=LOGGING_FORMAT)

    output_schema = StructType([
        StructField("warc", StringType(), False),
        StructField("uri", StringType(), False),
        StructField("error", BooleanType(), True),
        StructField("encoding", StringType(), True),
        StructField("subresources", ArrayType(StructType([
            StructField("tag", StringType(), True),
            StructField("target", StringType(), True),
            StructField("checksum", StringType(), True),
        ])), True),
        StructField("checksums", ArrayType(StringType()), True),
        StructField("keywords", ArrayType(StringType()), True),
        StructField("content", BinaryType(), True),
    ])

    re_contains_sri = re.compile(b'(?:(integrity=)|(integrity =))')
    re_contains_checksum = re.compile(b'(?:([a-f0-9]{32})|([A-F0-9]{32}))')
    re_extract_checksums = re.compile('(?:((?<!\w)[a-f0-9]{32,128}(?!\w))|((?<!\w)[A-F0-9]{32,128}(?!\w)))')
    re_contains_letter = re.compile('([a-f]|[A-F])')
    re_contains_number = re.compile('([0-9])')
    re_contains_keywords = {('md5 ', re.compile('md5', re.IGNORECASE)),
                            ('sha ', re.compile('(?<!\w)sha(-| )?[0-9]', re.IGNORECASE)),
                            ('hash ', re.compile('(?<!\w)hash', re.IGNORECASE)),
                            ('checksum ', re.compile('checksum', re.IGNORECASE)),
                            ('download ', re.compile('download', re.IGNORECASE)),
                            ('torrent ', re.compile('torrent', re.IGNORECASE))}

    checksum_sizes = [32, 40, 56, 64, 96, 128]  # md5, sha1, sha224, sha256, sha384, sha512

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

    def process_warc(self, warc):
        s3pattern = re.compile('^s3://([^/]+)/(.+)')
        base_dir = os.path.abspath(os.path.dirname(__file__))

        # S3 client (not thread-safe, initialize outside parallelized loop)
        no_sign_request = botocore.client.Config(signature_version=botocore.UNSIGNED)
        s3client = boto3.client('s3', config=no_sign_request)

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
                for result in self.process_record(warc, record):
                    yield result
                self.records_processed.add(1)
        except ArchiveLoadFailed as exception:
            self.warc_input_failed.add(1)
            self.get_logger().error('Invalid WARC: {} - {}'.format(warc, exception))
        finally:
            stream.close()

    def extract_subresources(self, soup):
        tags = [(tag.name, tag.get('src') or tag.get('href'), tag.get("integrity"))
                for tag in soup(["link", "script"])
                if tag.get("integrity") is not None]
        return tags

    def extract_text(self, soup):
        body = soup(["body"])
        text = "" if not body else body[0].getText()
        return text

    def filter_checksum(self, checksum):
        if not len(checksum) in self.checksum_sizes:
            return False
        if re.search(self.re_contains_number, checksum) is None:
            return False
        if re.search(self.re_contains_letter, checksum) is None:
            return False
        # check number of distinct digits
        return True

    def extract_checksums(self, text):
        groups = re.findall(self.re_extract_checksums, text)
        checksums = set()
        [[checksums.add(checksum.lower())
          for checksum in group if self.filter_checksum(checksum)]
         for group in groups]
        return list(checksums)

    def extract_keywords(self, text):
        keywords = []
        [keywords.append(word) for (word, pattern) in self.re_contains_keywords if re.search(pattern, text)]
        return keywords

    def process_record(self, warc, record):
        if 'response' == record.rec_type:

            # variables initialization
            warc = warc[warc.rfind('H'):]
            uri = record.rec_headers.get_header('WARC-Target-URI')
            error = False
            encoding = None
            subresources = []
            checksums = []
            keywords = []
            content = record.content_stream().read()

            subresources_contains = re.search(self.re_contains_sri, content) is not None
            checksums_contains = re.search(self.re_contains_checksum, content) is not None

            # prune the records
            if subresources_contains or checksums_contains:
                try:
                    # detect encoding and parse content
                    encoding = EncodingDetector.find_declared_encoding(content, is_html=True)
                    soup = BeautifulSoup(content, "lxml", from_encoding=encoding)

                    # extract tags that contain an integrity attribute
                    # soup.find_all(lambda tag: 'integrity' in tag.attrs)
                    subresources = self.extract_subresources(soup)

                    # extract text
                    text = self.extract_text(soup)

                    # extract checksums from text
                    checksums = self.extract_checksums(text)

                    # extract keywords from text
                    keywords = self.extract_keywords(text)

                except:
                    error = True

            # store content only if needed
            content = bytearray(content) if len(subresources) > 0 or len(checksums) > 0 or error else None

            yield [warc, uri, error, encoding, subresources, checksums, keywords, content]


if __name__ == "__main__":
    job = CommonCrawlSRI()
    job.run()
