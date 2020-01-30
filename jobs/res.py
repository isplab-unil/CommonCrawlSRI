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
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType, IntegerType, MapType
from commoncrawl import CommonCrawl


class Res(CommonCrawl):
    """
    A Spark job to analyze the subresources on CommonCrawl.
    """

    name = "Res"

    schema = StructType([
        StructField("warc", IntegerType(), False),
        StructField("url", StringType(), False),
        StructField("subresources", ArrayType(StructType([
            StructField("name", StringType(), True),
            StructField("target", StringType(), True),
            StructField("attributes", MapType(StringType(), StringType()), True),
        ])), True),
        StructField("error", StringType(), True),
    ])

    def extract_subresources(self, soup):
        tags = list()

        # iterate over the sub-resources
        for tag in soup(["link", "script", "img", "video", "audio"]):
            name = tag.name

            # extract the main attributes
            target = tag.get('src') or tag.get('href')

            # extract the other attributes as a dictionnary
            attributes = tag.attrs
            if "src" in attributes:
                del attributes['src']
            if "href" in attributes:
                del attributes['href']

            tags.append((name, target, attributes))

        return tags

    def process_record(self, warc, record):
        if 'response' == record.rec_type:

            # variables initialization
            url = record.rec_headers.get_header('WARC-Target-URI')
            content = record.content_stream().read()

            subresources = []
            error = None

            try:
                # detect encoding and parse content
                encoding = EncodingDetector.find_declared_encoding(content, is_html=True)
                soup = BeautifulSoup(content, "lxml", from_encoding=encoding)
                subresources = self.extract_subresources(soup)

            except Exception as e:
                error = str(e)

            yield [warc,
                   url,
                   subresources,
                   error]


if __name__ == "__main__":
    job = Res()
    job.run()
