#!/usr/bin/env python

import re
import datetime
import dateutil
from glob import iglob
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.cbook as cbook
from pyspark.shell import sqlContext

# Intitialize the data
frames = []
for file in iglob('../data/sri/*/queries/top_sri_domain/', recursive=True):
    crawl = re.findall(r'/(\d{4}-\d{2})/', file)
    date = datetime.datetime.strptime(crawl[0] + "-1", "%Y-%W-%w")
    frame = sqlContext.read.parquet(file).toPandas()
    frame['date'] = date
    frame['rank'] = frame.index + 1
    frames.append(frame[:100])
data = pd.concat(frames, ignore_index=True)

limit = 10
top10 = []

for name, group in data.groupby(['domain']):
    now = dateutil.parser.parse("2019-09-02")
    rank = max(group['rank'].loc[group['date']==now], default=1000)
    if (rank <= limit):
        group = group.set_index('date')
        top10.append(group)
top10 = pd.concat(top10)

# Draw the Figure

plt.rc('font',**{'family':'serif','size':'16'})
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42
plt.rcParams["figure.figsize"] = (6, 5)

plot, ax = plt.subplots(1, 1)


groups = data.groupby(['domain'])
for name, group in groups:
    now = dateutil.parser.parse("2019-09-02")
    rank = max(group['rank'].loc[group['date']==now], default=1000)
    if (rank <= limit):
        group = group.set_index('date')
        ax = group['rank'].plot(style='o-')
        color = ax.get_lines()[-1].get_color()
        plt.text(dateutil.parser.parse("2019-09-02") + datetime.timedelta(days=30), rank + 0.1, name, color=color)
        
plt.gca().invert_yaxis()
plt.yticks(range(1, limit + 1))
plt.xticks(rotation=30)
plt.xlabel('')
plt.ylabel('popularity rank')
plt.xlim(datetime.date(2016, 4, 1), datetime.date(2019, 10, 1))
plt.ylim(limit + 1, 0)

ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.yaxis.grid()

ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
ax.xaxis.set_minor_locator(mdates.MonthLocator())

plt.savefig('output/top_sri_domain_evolution.pdf', bbox_inches = 'tight')

# Write the latex variables
with open("output/top_sri_domain_evolution.tex", 'w') as out:
    out.write("\def\\CCTopTenDomainP{\\num{" + ("%0.2f" % data[:10]['percentage'].sum()) + "}\%\\xspace}\n")

# Write the json file
with open("output/top_sri_domain_evolution.json", 'w') as out:
    out.write(top10.to_json(orient='split'))

