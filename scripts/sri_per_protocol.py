#!/usr/bin/env python

import re
from glob import iglob
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.shell import sqlContext

# Initialize the data
data = sqlContext.read.parquet('../data/sri/2019-09/queries/06_sri_per_protocol').toPandas()
data['percentage'] = data['sri'] / data['sri'].sum() * 100
data = data.set_index('protocol')

# Draw the Figure
plt.rc('font',**{'family':'serif','size':'16'})
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42
plt.rcParams["figure.figsize"] = (10, 5)

fig, ax = plt.subplots(1, 1)
data['percentage'].plot.bar(ax=ax, sharex=True, rot='xticks')

for i, v in enumerate(data['percentage']):
    ax.text(i-0.25, v + 2, '{:.2f}%'.format(v))

plt.xticks(rotation=90)
plt.ylim(0,75)

plt.xlabel('')
plt.ylabel('Values')

ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.yaxis.grid()

ax.set_yticklabels(['{:,.0%}'.format(x / 100) for x in ax.get_yticks()])

plt.savefig('output/sri_per_protocol.pdf', bbox_inches='tight')

