# -*- coding: utf-8 -*-
import re
from typing import List, Any

__author__ = "Bertil Chapuis, Kévin Huguenin, Romain Artru"
__copyright__ = "Copyright 2019, The Information Security and Privacy Lab at the University of Lausanne (https://www.unil.ch/isplab/)"
__credits__ = ["Bertil Chapuis", "Kévin Huguenin", "Romain Artru"]

__version__ = "1"
__license__ = "MIT"
__maintainer__ = "Bertil Chapuis"
__email__ = "bertil.chapuis@unil.ch"

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType, IntegerType
from commoncrawl import CommonCrawl


class Download(CommonCrawl):
    """
    A Spark job to analyze download pages on CommonCrawl's WET files.
    """

    name = "DownloadsWet"

    schema = StructType([
        StructField("warc", IntegerType(), False),
        StructField("url", StringType(), False),
        StructField("has_keyword", BooleanType(), True),
        StructField("keywords", ArrayType(StringType()), True),
        StructField("has_checksum", BooleanType(), True),
        StructField("checksums", ArrayType(StringType()), True),
        StructField("content", StringType(), True)
    ])

    def __init__(self):
        self.keywords = ["download"]
        self.keyword_patterns = [(keyword, re.compile(str(keyword), re.IGNORECASE)) for keyword in self.keywords]
        self.checksum_filter = re.compile('[a-f0-9]{32}|[A-F0-9]{32}')
        self.checksum_sizes = [32, 40, 56, 64, 96, 128]
        self.checksum_pattern = re.compile('(?:(?<!\w)[a-f0-9]{32,128}(?!\w)|(?<!\w)[A-F0-9]{32,128}(?!\w))')
        self.contains_number = re.compile('[0-9]')
        self.contains_letter = re.compile('[a-f]|[A-F]')

    def filter_checksum(self, checksum):
        if not len(checksum) in self.checksum_sizes:
            return False
        if re.search(self.contains_number, checksum) is None:
            return False
        if re.search(self.contains_letter, checksum) is None:
            return False
        return True

    @staticmethod
    def is_wet_text_record(record):
        """Return true if WARC record is a WET text/plain record"""
        return (record.rec_type == 'conversion' and
                record.content_type == 'text/plain' and
                record.rec_headers.get_header('WARC-Target-URI') is not None)

    def process_record(self, warc_id, record):
        if self.is_wet_text_record(record):
            # variables initialization
            url = record.rec_headers.get_header('WARC-Target-URI')
            content = record.content_stream().read().decode('utf-8')

            keywords = [keyword for (keyword, pattern) in self.keyword_patterns if pattern.search(content) is not None]
            has_keyword = len(keywords) > 0

            checksums = []
            has_checksum = False

            # prune on keywords
            if has_keyword:
                # extract checksums
                checksums = [checksum for checksum in self.checksum_pattern.findall(content) if self.filter_checksum(checksum)]
                has_checksum = len(checksums) > 0

            if not has_checksum:
                content = None

            yield [warc_id,
                   url,
                   has_keyword, keywords,
                   has_checksum, checksums,
                   content]


if __name__ == "__main__":
    job = Download()
    job.run()
