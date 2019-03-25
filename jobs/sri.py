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


class Sri(CommonCrawl):
    """
    A Spark job to analyze the sub-resources integrity on CommonCrawl.
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
            StructField("referrerpolicy", StringType(), True),
            StructField("full", StringType(), True)
        ])), True),

        StructField("error", StringType(), True),
    ])

    def __init__(self):
        self.subresource_filters = b"integrity="

    def extract_subresources(self, soup):
        tags = list()
        for tag in soup(["link", "script"]):
            name = tag.name
            src = tag.get('src') or tag.get('href')
            integrity = tag.get('integrity')
            crossorigin = tag.get('crossorigin')
            referrerpolicy = tag.get('referrerpolicy')
            full = str(tag)
            tags.append((name, src, integrity, crossorigin, referrerpolicy, None))
        return tags

    def process_record(self, warc, record):
        if 'response' == record.rec_type:

            # variables initialization
            uri = record.rec_headers.get_header('WARC-Target-URI')
            content = record.content_stream().read()
            csp = None
            cors = None

            has_subresource_filter = self.subresource_filters in content
            has_subresource = False
            subresources = []

            error = None

            # prune records
            if has_subresource_filter:
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

                except Exception as e:
                    error = str(e)

            yield [warc,
                   uri,
                   csp,
                   cors,
                   has_subresource_filter,
                   has_subresource,
                   subresources,
                   error]


if __name__ == "__main__":
    job = Sri()
    job.run()
