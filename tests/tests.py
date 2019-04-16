# -*- coding: utf-8 -*-
import re

__author__ = "Bertil Chapuis, Kévin Huguenin"
__copyright__ = "Copyright 2019, The Information Security and Privacy Lab at the University of Lausanne (https://www.unil.ch/isplab/)"
__credits__ = ["Bertil Chapuis", "Kévin Huguenin"]

__version__ = "1"
__license__ = "MIT"
__maintainer__ = "Bertil Chapuis"
__email__ = "bertil.chapuis@unil.ch"

import unittest

from bs4 import BeautifulSoup
from jobs.sri import SRI
from jobs.checksums import Checksums

SRI1 = "sha256-95e8a199e17e995fc7cb9cb2c13ec6075f5126bd986a25ec19fb1d101559d7e1"
SRI2 = "sha256-95e8a199e17e995fc7cb9cb2c13ec6075f5126bd986a25ec19fb1d101559d7e2"
CHECKSUM1 = "95e8a199e17e995fc7cb9cb2c13ec6075f5126bd986a25ec19fb1d101559d7e1"
CHECKSUM2 = "95e8a199e17e995fc7cb9cb2c13ec6075f5126bd986a25ec19fb1d101559d7e2"

HTML = """
<html>
<head>
<title>test</title>
<script src="script.js" integrity="{0}"></script>
<link href="style.css" integrity="{1}"></script>
</head>
<body>
<p>{2}</p>
<p>{3}</p>
</body>
</html>
""".format(SRI1, SRI2, CHECKSUM1, CHECKSUM2)

checksum_filter1 = re.compile(b'[a-f0-9]{32}|[A-F0-9]{32}')
checksum_filter2 = re.compile(b'(?:([a-f0-9]{32})|([A-F0-9]{32}))')


class CommonCrawlSRITests(unittest.TestCase):

    def test_extract_text(self):
        soup = BeautifulSoup(HTML, "lxml")
        text = Checksums().extract_text(soup)
        self.assertTrue("test" not in text)

    def test_extract_subresources(self):
        soup = BeautifulSoup(HTML, "lxml")
        tags = SRI().extract_subresources(soup)
        self.assertTrue(len(tags) == 2)
        self.assertEqual(tags[0][2], SRI1)
        self.assertEqual(tags[1][2], SRI2)

    def test_filter_checksums(self):
        re.search(HTML)
        re.search(checksum_filter1, HTML)


if __name__ == '__main__':
    unittest.main()
