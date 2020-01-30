#!/usr/bin/env python

import re
from glob import iglob
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.shell import sqlContext

# Initialize the data

data = sqlContext.read.parquet('../data/sri/2019-35/queries/top_sri_domain/').toPandas()
data = data.set_index('domain')
data = data.sort_values(['percentage'], ascending=False)
data = data[0:10]

# Draw the Figure

plt.rc('font',**{'family':'serif','size':'16'})
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42
plt.rcParams["figure.figsize"] = (10, 5)

fig, ax = plt.subplots(1, 1)

data['percentage'].plot.bar(ax=ax, sharex=True, rot='xticks')
#data['percentage'].cumsum().plot.line(ax=ax, sharex=True)

plt.xticks(rotation=90)
plt.xlim(-0.5,9.5)
plt.xlabel('')
plt.ylabel('SRI')

ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.yaxis.grid()

ax.set_yticklabels(['{:,.0%}'.format(x / 100) for x in ax.get_yticks()])

plt.xticks(rotation=45, ha="right")

plt.savefig('output/top_sri_domain.pdf', bbox_inches='tight')

