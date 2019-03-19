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


class CommonCrawlSri(CommonCrawl):
    """
    A Spark job to analyze the integrity of subresources on CommonCrawl.
    """

    name = "CommoncrawlSRI"

    schema = StructType([
        StructField("warc", IntegerType(), False),

        StructField("uri", StringType(), False),
        StructField("csp", StringType(), True),
        StructField("cors", StringType(), True),

        StructField("has_subresource_filter", BooleanType(), True),
        StructField("has_subresource", BooleanType(), True),
        StructField("subresources", ArrayType(StructType([
            StructField("name", StringType(), True),
            StructField("target", StringType(), True),
            StructField("integrity", StringType(), True),
            StructField("crossorigin", StringType(), True),
            StructField("referrerpolicy", StringType(), True)
        ])), True),

        StructField("has_keyword_filter", BooleanType(), True),
        StructField("has_keyword", BooleanType(), True),
        StructField("keywords", ArrayType(StringType()), True),

        StructField("has_checksum_filter", BooleanType(), True),
        StructField("has_checksum", BooleanType(), True),
        StructField("checksums", ArrayType(StringType()), True),

        StructField("error", StringType(), True),
    ])

    def __init__(self):
        self.subresource_filters = [b"integrity="]
        self.keywords = ["download"]
        self.keyword_patterns_bytes = [re.compile(bytes(keyword, "utf-8"), re.IGNORECASE) for keyword in self.keywords]
        self.keyword_patterns_string = [(keyword, re.compile(str(keyword), re.IGNORECASE)) for keyword in self.keywords]
        self.checksum_filter = re.compile(b'[a-f0-9]{32}|[A-F0-9]{32}')
        self.checksum_sizes = [32, 40, 56, 64, 96, 128]
        self.checksum_pattern = re.compile('(?:(?<!\w)[a-f0-9]{32,128}(?!\w)|(?<!\w)[A-F0-9]{32,128}(?!\w))')
        self.contains_number = re.compile('[0-9]')
        self.contains_letter = re.compile('[a-f]|[A-F]')

    def extract_text(self, soup):
        # remove all javascript and stylesheet code
        for tag in soup(["head", "script", "style"]):
            tag.extract()
        return soup.get_text()

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
            content = record.content_stream().read()
            csp = None
            cors = None

            has_subresource_filter = any([subresource_filter in content for subresource_filter in self.subresource_filters])
            has_subresource = False
            subresources = []

            has_keyword_filter = any([pattern.search(content) is not None for pattern in self.keyword_patterns_bytes])
            has_keyword = False
            keywords = []

            has_checksum_filter = self.checksum_filter.search(content) is not None
            has_checksum = False
            checksums = []

            error = None

            # prune records
            if has_subresource_filter or (has_keyword_filter and has_checksum_filter):
                try:
                    # extract http headers
                    csp = record.http_headers.get_header('Content-Security-Policy')
                    cors = record.http_headers.get_header('Access-Control-Allow-Origin')

                    # detect encoding and parse content
                    encoding = EncodingDetector.find_declared_encoding(content, is_html=True)
                    soup = BeautifulSoup(content, "lxml", from_encoding=encoding)

                    if has_subresource_filter:
                        subresources = self.extract_subresources(soup)
                        has_subresource = len(subresources) > 0

                    if has_keyword_filter and has_checksum_filter:
                        text = self.extract_text(soup)

                        keywords = [keyword for (keyword, pattern) in self.keyword_patterns_string if
                                    pattern.search(text) is not None]
                        has_keyword = len(keywords) > 0

                        checksums = self.extract_checksums(text)
                        has_checksum = len(checksums) > 0

                except Exception as e:
                    error = str(e)

            yield [warc_id,
                   uri, csp, cors,
                   has_subresource_filter, has_subresource, subresources,
                   has_keyword_filter, has_keyword, keywords,
                   has_checksum_filter, has_checksum, checksums,
                   error]


if __name__ == "__main__":
    job = CommonCrawlSri()
    job.run()
