import os
import unittest

from bs4 import BeautifulSoup
from warcio import ArchiveIterator

from commoncrawlsri import CommonCrawlSRI

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

class CommonCrawlSRITests(unittest.TestCase):

    def test_extract_text(self):
        soup = BeautifulSoup(HTML, "lxml")
        text = CommonCrawlSRI().extract_text(soup)
        self.assertTrue("test" not in text)

    def test_extract_subresources(self):
        soup = BeautifulSoup(HTML, "lxml")
        tags = CommonCrawlSRI().extract_subresources(soup)
        self.assertTrue(len(tags) == 2)
        self.assertEqual(tags[0][2], SRI1)
        self.assertEqual(tags[1][2], SRI2)

    def test_checksums(self):
        soup = BeautifulSoup(HTML, "lxml")
        text = CommonCrawlSRI().extract_text(soup)
        checksums = CommonCrawlSRI().extract_checksums(text)
        self.assertTrue(CHECKSUM1 in checksums)
        self.assertTrue(CHECKSUM2 in checksums)

    def test_process_record(self):
        with open("/home/bchapuis/Projects/github.com/bchapuis/commoncrawl-sri/crawl-data/CC-MAIN-2017-13/segments/1490218186353.38/warc/CC-MAIN-20170322212946-00000-ip-10-233-31-227.ec2.internal.warc.gz", 'rb') as stream:
            self.assertTrue(True)

            for record in ArchiveIterator(stream):
                for result in CommonCrawlSRI().process_record(record):
                    print(result)

if __name__ == '__main__':
    unittest.main()
