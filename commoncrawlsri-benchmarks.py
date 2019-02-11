import argparse

from warcio import ArchiveIterator

from commoncrawlsri import CommonCrawlSRI

import cProfile

def parse_arguments():
    """ Returns the parsed arguments from the command line """
    arg_parser = argparse.ArgumentParser(description="Benchmark arguments")
    arg_parser.add_argument("input", help="Path to file listing input paths")
    return arg_parser.parse_args()

def process_warc(input):
    """ Returns the number of records in a collection of archives"""
    processor = CommonCrawlSRI()
    limit = 0
    count = 0
    with open(input) as warcs:
        for warc in warcs.read().splitlines():
            with open(warc[5:], 'rb') as stream:
                for record in ArchiveIterator(stream):
                    limit += 1
                    if limit >= 10000:
                        break;
                    for result in processor.process_record(record):
                        count += 1
    return count



if __name__ == '__main__':
    args = parse_arguments()
    cProfile.run("process_warc(\"%s\")" % args.input, "process_warc_benchmark.pstat")

