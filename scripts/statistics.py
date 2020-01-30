#!/usr/bin/env python

import datetime
from utils import loadSnapshot, loadSnapshots
import glob

# Initialize the data
countPages = loadSnapshots('../data/sri/2019-35/queries/count_pages/')
countDistinctPages = loadSnapshots('../data/sri/2019-35/queries/count_distinct_pages/')
countDistinctDomains = loadSnapshots('../data/sri/2019-35/queries/count_distinct_domains/')
snapshots = sorted([s.split('/')[-1] + "-1" for s in glob.glob("../data/sri/*")])
snapshots = [datetime.datetime.strptime(s, "%Y-%W-%w").strftime('%Y-%m') for s in snapshots]
print(snapshots)

# Write latex
with open("output/statistics.tex", 'w') as out:
    out.write("\def\\LatestCCSize{\\num{53.53}~TB\\xspace}\n")
    out.write("\def\\LatestCCNbURLS{\\num{" + str(int(countPages.reset_index()[-1:]['count'])) + "}\\xspace}\n")
    out.write("\def\\CCUrlDistinctN{\\num{" + str(int(countDistinctPages.reset_index()[-1:]['count'])) + "}\\xspace}\n")
    out.write("\def\\CCDomainN{\\num{" + str(int(countDistinctDomains.reset_index()[-1:]['count'])) + "}\\xspace}\n")
    out.write("\def\\CCSnapshotLatest{" + snapshots[-1] + "\\xspace}\n")
    out.write("\def\\CCSnapshotEarliest{" + snapshots[0] + "\\xspace}\n")
    out.write("\def\\CCSnapshotDates{" + ", ".join(snapshots) + "\\xspace}\n")
    out.write("\def\\CCSnapshotN{" + str(len(snapshots)) + "\\xspace}\n")
    out.write("\def\\EarliestCCDate{2011-01\\xspace}\n")
