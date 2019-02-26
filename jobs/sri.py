# -*- coding: utf-8 -*-

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

class SRI(CommonCrawl):
    """
    A Spark job to analyze SRI adoption on CommonCrawl.
    """

    name = "CommoncrawlSRI"

    schema = StructType([
        StructField("src", IntegerType(), False),
        StructField("uri", StringType(), False),
        StructField("error", BooleanType(), True),
        StructField("encoding", StringType(), True),
        StructField("content", BinaryType(), True),
        StructField("has_subresources", BooleanType(), True),
        StructField("subresources", ArrayType(StructType([
            StructField("name", StringType(), True),
            StructField("target", StringType(), True),
            StructField("integrity", StringType(), True),
            StructField("crossorigin", StringType(), True),
            StructField("referrerpolicy", StringType(), True)
        ])), True),
    ])

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
            encoding = None
            content = record.content_stream().read()
            has_subresources = b"integrity=" in content
            subresources = []

            # prune the records
            if has_subresources:
                try:
                    # detect encoding and parse content
                    encoding = EncodingDetector.find_declared_encoding(content, is_html=True)
                    soup = BeautifulSoup(content, "lxml", from_encoding=encoding)

                    # extract tags that contain an integrity attribute
                    subresources = self.extract_subresources(soup)

                except:
                    error = True

            # store content only if needed
            content = bytearray(content) if len(subresources) > 0 or error else None

            yield [warc_id, uri, error, encoding, content, has_subresources, subresources]

if __name__ == "__main__":
    job = SRI()
    job.run()
