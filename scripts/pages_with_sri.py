#!/usr/bin/env python

import re
import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.cbook as cbook
from utils import loadSnapshots

# Initialize the data
data = loadSnapshots('../data/sri/*/queries/pages_with_sri/')
script = loadSnapshots('../data/sri/*/queries/pages_with_sri_script/')
link = loadSnapshots('../data/sri/*/queries/pages_with_sri_link/')
top1m = loadSnapshots('../data/sri/*/queries/pages_with_sri_top1m/')

milestones = [
    ("Chrome 45", datetime.date(2015, 9, 1), 0.2),
    ("Bootstrap Snippet", datetime.date(2015, 10, 1), 0.6),
    ("Firefox 43", datetime.date(2015, 12, 14), 1),
    ("jQuery Snippet", datetime.date(2016, 3, 1), 1.4),
    ("SRI W3C Recommendation", datetime.date(2016, 6, 23), 1.8),
    ("grunt-sri (v0.1.0)", datetime.date(2016, 12, 9), 2.3),
    ("webpack-sri (v1.0)", datetime.date(2017, 6, 29), 2.8),
    ("Font Awesome Snippet", datetime.date(2018, 3, 15), 3.3), # https://github.com/FortAwesome/Font-Awesome/issues/12170
]

# Draw the plot
plt.rc('font',**{'family':'serif','size':'16'})
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42
plt.rcParams["figure.figsize"] = (10, 5)

plot, ax = plt.subplots(1, 1)

top1m['percentage'].plot(legend="True", label='any (top1m)', style='bo--')
data['percentage'].plot(legend="True", label='any (all)', style='go-')
script['percentage'].plot(legend="True", label='script (all)', style='cs-')
link['percentage'].plot(legend="True", label='link (all)', style='mD-')

for milestone in milestones:
    plt.text(milestone[1] + datetime.timedelta(days=10), milestone[2], milestone[0])
    plt.axvline(x=milestone[1], color='k', linestyle='--', linewidth=1)

ax.set_yticklabels(['{:,.0%}'.format(x / 100) for x in ax.get_yticks()])
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.yaxis.grid()

plt.xlabel('')
plt.ylabel('prop. webpages')
plt.xticks(rotation=30)
plt.xlim(datetime.date(2015, 8, 1), datetime.date(2019, 10, 1))

leg = plt.legend(loc=('upper left'),title="SRI")
leg._legend_box.align = "left"

ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
ax.xaxis.set_minor_locator(mdates.MonthLocator())

plt.savefig('output/pages_with_sri.pdf', bbox_inches = 'tight')

# Write latex 
with open("output/pages_with_sri.tex", 'w') as out: 
    lastData = data.reset_index()[-1:]
    out.write("\def\\CCUrlSriN{\\num{" + str(lastData['number']) + "}\\xspace}\n")
    out.write("\def\\CCUrlSriP{\\num{" + ("%0.2f" % lastData['percentage']) + "}\%\\xspace}\n")

    lastScript = script.reset_index()[-1:]
    out.write("\def\\CCUrlSriScriptN{\\num{" + str(lastScript['number']) + "}\\xspace}\n")
    out.write("\def\\CCUrlSriScriptP{\\num{" + ("%0.2f" % lastScript['percentage']) + "}\%\\xspace}\n")

    lastLink = link.reset_index()[-1:]
    out.write("\def\\CCUrlSriLinkN{\\num{" + str(lastLink['number']) + "}\\xspace}\n")
    out.write("\def\\CCUrlSriLinkP{\\num{" + ("%0.2f" % lastLink['percentage']) + "}\%\\xspace}\n")

    lastTopM = top1m.reset_index()[-1:]
    out.write("\def\\CCUrlSriTopMN{\\num{" + str(lastTopM['number']) + "}\\xspace}\n")
    out.write("\def\\CCUrlSriTopMP{\\num{" + ("%0.2f" % lastTopM['percentage']) + "}\%\\xspace}\n")

# Write json
with open("output/pages_with_sri.json", 'w') as out:
    json = pd.concat([data, script, link, top1m], keys=['all', 'script', 'link', 'top1m'])
    out.write(json.to_json(orient='split'))

with open("output/page_with_sri_milestones.json", 'w') as out:
    json = pd.DataFrame(milestones, columns = ['Title', 'Date', 'Position'])
    out.write(json.to_json(orient='split'))
