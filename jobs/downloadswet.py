# -*- coding: utf-8 -*-
import re

__author__ = "Bertil Chapuis, Kévin Huguenin, Romain Artru"
__copyright__ = "Copyright 2019, The Information Security and Privacy Lab at the University of Lausanne (https://www.unil.ch/isplab/)"
__credits__ = ["Bertil Chapuis", "Kévin Huguenin", "Romain Artru"]

__version__ = "1"
__license__ = "MIT"
__maintainer__ = "Bertil Chapuis"
__email__ = "bertil.chapuis@unil.ch"

from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType, IntegerType
from commoncrawl import CommonCrawl


class DownloadsWet(CommonCrawl):
    """
    A Spark job to analyze download pages on CommonCrawl's WET files.
    """

    name = "DownloadsWet"

    schema = StructType([
        StructField("warc", IntegerType(), False),
        StructField("uri", StringType(), False),

        StructField("has_keyword_filter", BooleanType(), True),
        StructField("has_keyword", BooleanType(), True),
        StructField("keywords", ArrayType(StringType()), True),

        StructField("has_checksum_filter", BooleanType(), True),
        StructField("has_checksum", BooleanType(), True),
        StructField("checksums", ArrayType(StringType()), True),

        StructField("content", StringType(), False),

        StructField("error", StringType(), True),
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

    def extract_checksums(self, text):
        checksums = [checksum for checksum in self.checksum_pattern.findall(text) if self.filter_checksum(checksum)]
        return list(set(checksums))

    def process_record(self, warc_id, record):
        # variables initialization
        uri = record.rec_headers.get_header('WARC-Target-URI')
        content = None

        has_keyword_filter = False
        has_keyword = False
        keywords = []

        has_checksum_filter = False
        has_checksum = False
        checksums = []

        error = None

        # prune on URIs
        if uri is not None:
            content = record.raw_stream.read().decode("utf-8")

            # prune on keywords
            has_keyword_filter = any([pattern.search(content) is not None for (keyword, pattern) in self.keyword_patterns])
            if has_keyword_filter:

                # prune on checksums
                has_checksum_filter = self.checksum_filter.search(content) is not None
                if has_checksum_filter:
                    try:

                        # extract keywords
                        keywords = [keyword for (keyword, pattern) in self.keyword_patterns if pattern.search(content) is not None]
                        has_keyword = len(keywords) > 0
                        if has_keyword:

                            # extract checksums
                            checksums = self.extract_checksums(content)
                            has_checksum = len(checksums) > 0

                    except Exception as e:
                        error = str(e)

        if has_checksum == False:
            content = None

        yield [warc_id,
               uri,
               has_keyword_filter, has_keyword, keywords,
               has_checksum_filter, has_checksum, checksums,
               content,
               error]


if __name__ == "__main__":
    job = DownloadsWet()
    job.run()
