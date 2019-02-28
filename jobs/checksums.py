# -*- coding: utf-8 -*-

__author__ = "Bertil Chapuis, Kévin Huguenin, Romain Artru"
__copyright__ = "Copyright 2019, The Information Security and Privacy Lab at the University of Lausanne (https://www.unil.ch/isplab/)"
__credits__ = ["Bertil Chapuis", "Kévin Huguenin", "Romain Artru"]

__version__ = "1"
__license__ = "MIT"
__maintainer__ = "Bertil Chapuis"
__email__ = "bertil.chapuis@unil.ch"

import re

from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BinaryType, BooleanType, IntegerType

from commoncrawl import CommonCrawl

class Checksums(CommonCrawl):
    """
    A Spark job to analyze SRI adoption on CommonCrawl.
    """

    name = "CommoncrawlCheckums"

    schema = StructType([
        StructField("warc", IntegerType(), False),
        StructField("uri", StringType(), False),
        StructField("error", BooleanType(), True),
        StructField("encoding", StringType(), True),
        StructField("has_download", BooleanType(), True),
        StructField("has_checksum", BooleanType(), True),
        StructField("checksums", ArrayType(StringType()), True),
    ])

    download_filter = b"download"

    checksum_sizes = [32, 40, 56, 64, 96, 128]
    checksum_filter = re.compile(b'[a-f0-9]{32}|[A-F0-9]{32}')
    checksum_extract = re.compile('(?:(?<!\w)[a-f0-9]{32,128}(?!\w)|(?<!\w)[A-F0-9]{32,128}(?!\w))')
    contains_number = re.compile('[0-9]')
    contains_letter = re.compile('[a-f]|[A-F]')

    def extract_text(self, soup):
        body = soup(["body"])
        text = "" if not body else body[0].getText()
        return text

    def filter_checksum(self, checksum):
        if not len(checksum) in self.checksum_sizes:
            return False
        if re.search(self.contains_number, checksum) is None:
            return False
        if re.search(self.contains_letter, checksum) is None:
            return False
        # check number of distinct digits
        return True

    def extract_checksums(self, text):
        checksums = [checksum for checksum in self.checksum_extract.findall(text) if self.filter_checksum(checksum)]
        return list(set(checksums))

    def process_record(self, warc_id, record):
        if 'response' == record.rec_type:

            # variables initialization
            uri = record.rec_headers.get_header('WARC-Target-URI')
            error = False
            encoding = None
            content = record.content_stream().read()
            has_checksum = self.filter_download in content and self.filter_checksum.match(content) is not None
            checksums = []

            # prune the records
            if has_checksum:
                try:
                    # detect encoding and parse content
                    encoding = EncodingDetector.find_declared_encoding(content, is_html=True)
                    soup = BeautifulSoup(content, "lxml", from_encoding=encoding)

                    # extract text and checksums
                    text = self.extract_text(soup)
                    checksums = self.extract_checksums(text)

                except:
                    error = True

            yield [warc_id, uri, error, encoding, has_checksum, checksums]

if __name__ == "__main__":
    job = Checksums()
    job.run()
