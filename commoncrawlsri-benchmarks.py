from warcio import ArchiveIterator

from commoncrawlsri import CommonCrawlSRI

if __name__ == '__main__':
    with open(
            "/home/bchapuis/Projects/github.com/bchapuis/commoncrawl-sri/crawl-data/CC-MAIN-2017-13/segments/1490218186353.38/warc/CC-MAIN-20170322212946-00000-ip-10-233-31-227.ec2.internal.warc.gz",
            'rb') as stream:

        processor = CommonCrawlSRI()
        for record in ArchiveIterator(stream):
            for result in processor.process_record(record):
                print(result)

