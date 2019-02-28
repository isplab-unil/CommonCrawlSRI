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
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BinaryType, BooleanType, IntegerType
from commoncrawl import CommonCrawl


class Full(CommonCrawl):
    """
    A Spark job to analyze CommonCrawl.
    """

    name = "CommoncrawlSRI"

    schema = StructType([
        StructField("warc", IntegerType(), False),
        StructField("uri", StringType(), False),
        StructField("error", BooleanType(), True),
        StructField("csp", StringType(), True),
        StructField("cors", StringType(), True),
        StructField("has_subresource", BooleanType(), True),
        StructField("subresources", ArrayType(StructType([
            StructField("name", StringType(), True),
            StructField("target", StringType(), True),
            StructField("integrity", StringType(), True),
            StructField("crossorigin", StringType(), True),
            StructField("referrerpolicy", StringType(), True)
        ])), True),
        StructField("has_checksum", BooleanType(), True),
        StructField("checksums", ArrayType(StringType()), True),
    ])

    def __init__(self):
        self.filter_subresource = b"integrity="
        self.filter_download = b"download"
        self.filter_checksum = re.compile(b'[a-f0-9]{32}|[A-F0-9]{32}')
        self.check_checksum_sizes = [32, 40, 56, 64, 96, 128]
        self.extract_checksums = re.compile('(?:(?<!\w)[a-f0-9]{32,128}(?!\w)|(?<!\w)[A-F0-9]{32,128}(?!\w))')
        self.contains_number = re.compile('[0-9]')
        self.contains_letter = re.compile('[a-f]|[A-F]')

    def extract_text(self, soup):
        body = soup(["body"])
        text = "" if not body else body[0].getText()
        return text

    def filter_checksum(self, checksum):
        if not len(checksum) in self.check_checksum_sizes:
            return False
        if re.search(self.contains_number, checksum) is None:
            return False
        if re.search(self.contains_letter, checksum) is None:
            return False
        # check number of distinct digits
        return True

    def extract_checksums(self, text):
        checksums = [checksum for checksum in self.extract_checksums.findall(text) if self.filter_checksum(checksum)]
        return list(set(checksums))

    def extract_subresources(self, soup):
        tags = list()
        for tag in soup(["link", "script"]):
            name = tag.name
            src = tag.get('src') or tag.get('href')
            integrity = tag.get('integrity')
            crossorigin = tag.get('crossorigin')
            referrerpolicy = tag.get('referrerpolicy')
            tags.append((name, src, integrity, crossorigin, referrerpolicy))
        return tags

    def process_record(self, warc_id, record):
        if 'response' == record.rec_type:

            # variables initialization
            uri = record.rec_headers.get_header('WARC-Target-URI')
            error = False
            content = record.content_stream().read()
            csp = None
            cors = None
            has_subresource = self.filter_subresource in content
            subresources = []
            has_checksum = self.filter_download in content and self.filter_checksum.search(content) is not None
            checksums = []

            if has_subresource or has_checksum:
                try:
                    # extract http headers
                    csp = record.http_headers.get_header('Content-Security-Policy')
                    cors = record.http_headers.get_header('Access-Control-Allow-Origin')

                    # detect encoding and parse content
                    encoding = EncodingDetector.find_declared_encoding(content, is_html=True)
                    soup = BeautifulSoup(content, "lxml", from_encoding=encoding)

                    # extract tags that contain an integrity attribute
                    subresources = self.extract_subresources(soup)

                    # extract text and checksums
                    text = self.extract_text(soup)
                    checksums = self.extract_checksums(text)

                except:
                    error = True

            yield [warc_id, uri, error, csp, cors, has_subresource, subresources, has_checksum, checksums]


if __name__ == "__main__":
    job = Full()
    job.run()
