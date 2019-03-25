# -*- coding: utf-8 -*-
__author__ = "Bertil Chapuis, Kévin Huguenin"
__copyright__ = "Copyright 2019, The Information Security and Privacy Lab at the University of Lausanne (https://www.unil.ch/isplab/)"
__credits__ = ["Bertil Chapuis", "Kévin Huguenin"]

__version__ = "1"
__license__ = "MIT"
__maintainer__ = "Bertil Chapuis"
__email__ = "bertil.chapuis@unil.ch"

import argparse
#import git
import time

from warcio import ArchiveIterator

from wet import Wet

def parse_arguments():
    """ Returns the parsed arguments from the command line """
    arg_parser = argparse.ArgumentParser(description="Benchmark arguments")
    arg_parser.add_argument("input", help="Path to file listing input paths")
    arg_parser.add_argument("limit", type=int, help="The number of records to process")
    return arg_parser.parse_args()

def process_warc(input, limit):
    """ Returns the number of records in a collection of archives"""

    start = time.time()
    wark_input_processed = 0
    wark_input_failed = 0
    records_processed = 0
    response_returned = 0
    response_failed = 0
    subresources_contained = 0
    subresources_counted = 0
    checksums_contained = 0
    checksums_counted = 0
    keywords_count = 0

    # mock the behaviour of the processor
    processor = Wet()
    with open(input) as warcs:
        for warc in warcs.read().splitlines():
            with open(warc, 'rb') as stream:
                wark_input_processed += 1
                for record in ArchiveIterator(stream):
                    if (records_processed >= limit):
                        break
                    records_processed += 1
                    try:
                        for response in processor.process_record(1, record):
                            response_returned += 1

                            if (response[1]):
                                response_failed += 1

                            if (response[3]):
                                subresources_contained += 1
                            subresources_counted += len(response[4])

                            if (response[5]):
                                checksums_contained += 1
                            checksums_counted += len(response[6])

                            keywords_count += len(response[7])
                    except Exception as e:
                        print(str(e))
                        wark_input_failed += 1

    duration = time.time() - start

    # print the results in the console
    print("duration:               %s" % duration)
    print("wark_input_processed:   %s" % wark_input_processed)
    print("wark_input_failed:      %s" % wark_input_failed)
    print("record_processed:       %s" % records_processed)
    print("response_returned:      %s" % response_returned)
    print("response_failed:        %s" % response_failed)
    print("subresources contained: %s" % subresources_contained)
    print("subresources_counted:   %s" % subresources_counted)
    print("checksums_contained:    %s" % checksums_contained)
    print("checksums_counted:      %s" % checksums_counted)
    print("keywords_count:         %s" % keywords_count)

    # add the git hash to the result
    #repo = git.Repo(search_parent_directories=True)
    hash = "hash"#repo.head.object.hexsha
    benchmarkFile = "benchmark-results.csv"
    with open(benchmarkFile, "a+") as csv:
        csv.write("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12}\n".format(
            hash,
            limit,
            start,
            duration,
            wark_input_processed,
            wark_input_failed,
            records_processed,
            response_returned,
            response_failed,
            subresources_contained,
            subresources_counted,
            checksums_contained,
            checksums_counted,
            keywords_count
        ))



if __name__ == '__main__':
    args = parse_arguments()
    process_warc(args.input, args.limit)
