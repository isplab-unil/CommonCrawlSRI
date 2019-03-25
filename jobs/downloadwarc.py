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


class DownloadWarc(CommonCrawl):
    """
    A Spark job to analyze download pages on CommonCrawl's Warc files.
    This job requires to parse and extract the content from the HTML.
    """

    name = "DownloadWarc"

    schema = StructType([
        StructField("warc", IntegerType(), False),

        StructField("uri", StringType(), False),

        StructField("has_keyword_filter", BooleanType(), True),
        StructField("has_keyword", BooleanType(), True),
        StructField("keywords", ArrayType(StringType()), True),

        StructField("has_checksum_filter", BooleanType(), True),
        StructField("has_checksum", BooleanType(), True),
        StructField("checksums", ArrayType(StringType()), True),

        StructField("error", StringType(), True),
    ])

    def __init__(self):
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
        for tag in soup(["head", "style", "script", "noscript"]):
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

    def process_record(self, warc_id, record):
        if 'response' == record.rec_type:

            # variables initialization
            uri = record.rec_headers.get_header('WARC-Target-URI')
            content = record.content_stream().read()

            has_keyword_filter = any([pattern.search(content) is not None for pattern in self.keyword_patterns_bytes])
            has_keyword = False
            keywords = []

            has_checksum_filter = False
            has_checksum = False
            checksums = []

            error = None

            # prune on keywords
            if  has_keyword_filter:
                has_checksum_filter = self.checksum_filter.search(content) is not None

                # prune on checksums
                if has_checksum_filter:
                    try:
                        # detect encoding and parse content
                        encoding = EncodingDetector.find_declared_encoding(content, is_html=True)
                        soup = BeautifulSoup(content, "lxml", from_encoding=encoding)
                        text = self.extract_text(soup)

                        keywords = [keyword for (keyword, pattern) in self.keyword_patterns_string if pattern.search(text) is not None]
                        has_keyword = len(keywords) > 0

                        checksums = self.extract_checksums(text)
                        has_checksum = len(checksums) > 0

                    except Exception as e:
                        error = str(e)

            yield [warc_id,
                   uri,
                   has_keyword_filter, has_keyword, keywords,
                   has_checksum_filter, has_checksum, checksums,
                   error]


if __name__ == "__main__":
    job = DownloadWarc()
    job.run()
