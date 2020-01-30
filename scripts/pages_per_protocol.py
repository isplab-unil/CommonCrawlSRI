#!/usr/bin/env python

import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.cbook as cbook
from utils import loadSnapshots

# Initialize the data
data = loadSnapshots('../data/sri/*/queries/pages_per_protocol/')
data = data.loc[data['protocol'] == 'https']

top1m = loadSnapshots('../data/sri/*/queries/pages_per_protocol_top1m/')
top1m = top1m.loc[top1m['protocol'] == 'https']

milestones = [
    ("Let's Encrypt", datetime.date(2016, 4, 12), 40),
    ("ACME v1", datetime.date(2016, 4, 12), 32),
    ("Firefox 52", datetime.date(2017, 3, 7), 8),
    ("ACME v2", datetime.date(2018, 3, 13), 48),
    ("Safari 11.1.2", datetime.date(2018, 7, 9), 8),
    ("Chrome 68", datetime.date(2018, 7, 24), 24),
]

# Draw the plot
plt.rc('font',**{'family':'serif','size':'16'})
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42
plt.rcParams["figure.figsize"] = (10, 5)
plot, ax = plt.subplots(1, 1)

top1m['percentage'].plot(legend="True", style='bo--')
data['percentage'].plot(legend="True", style='go-')

for milestone in milestones:
    plt.text(milestone[1] + datetime.timedelta(days=10), milestone[2], milestone[0])
    plt.axvline(x=milestone[1], color='k', linestyle='--', linewidth=1)

ax.set_yticklabels(['{:,.0%}'.format(x / 100) for x in ax.get_yticks()])
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.yaxis.grid()

plt.ylim(0, 100)
plt.xlabel('')
plt.ylabel('prop. webpages')
plt.xticks(rotation=30)
plt.xlim(datetime.date(2015, 8, 1), datetime.date(2019, 10, 1))

leg = plt.legend(["top1m", "all"], loc='upper left', title="HTTPS")
leg._legend_box.align = "left"

ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
ax.xaxis.set_minor_locator(mdates.MonthLocator())

plt.savefig('output/pages_per_protocol.pdf', bbox_inches = 'tight')

# Write latex
with open("output/pages_per_protocol.tex", 'w') as out:
    lastCrawl = data.reset_index()[-1:]
    lastCrawlTop1M = top1m.reset_index()[-1:]
    out.write("\def\\CCUrlN{\\num{" + str(int(lastCrawl['total'])) + "}\\xspace}\n")
    out.write("\def\\CCUrlHttpsN{\\num{" + str(int(lastCrawl['number'])) + "}\\xspace}\n")
    out.write("\def\\CCUrlHttpsP{\\num{" + ("%0.2f" % lastCrawl['percentage']) + "}\%\\xspace}\n")
    out.write("\def\\CCUrlHttpsTopMN{\\num{" + str(int(lastCrawlTop1M['number'])) + "}\\xspace}\n")
    out.write("\def\\CCUrlHttpsTopMP{\\num{" + ("%0.2f" % lastCrawlTop1M['percentage']) + "}\%\\xspace}\n")

# Write json
with open("output/pages_per_protocol.json", 'w') as out:
    out.write(data.to_json(orient='split'))
    out.close()

with open("output/pages_per_protocol_milestones.json", 'w') as out:
    json = pd.DataFrame(milestones, columns = ['Title', 'Date', 'Position'])
    out.write(json.to_json(orient='split'))