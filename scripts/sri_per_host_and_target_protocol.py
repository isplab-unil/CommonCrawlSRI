#!/usr/bin/env python

import re
from glob import iglob
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.shell import sqlContext
import numpy as np

# Intitialize the data
data = sqlContext.read.parquet('../data/sri/2019-35/queries/sri_per_host_and_target_protocol').toPandas()
data['percentage'] = data['sri'] / data['sri'].sum() * 100

https_all_count = data.loc[data['host'] == 'https://']['sri'].sum()
https_inheritence_count = data.loc[(data['host'] == 'https://') & (data['target'] == '//')]['sri'].sum()
https_inheritence_percentage = https_inheritence_count / https_all_count * 100

# Draw the Figure
plt.rcParams["figure.figsize"] = (10, 5)
plt.rc('font',**{'family':'serif','size':'16'})
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42

ax = data.pivot(index='host', columns='target', values='percentage').plot.bar(stacked=True, label=False)

ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.yaxis.grid()

plt.ylim(0, 100)
plt.legend(title=None)
plt.xlabel('Webpages')
plt.ylabel('SRI')

ax.set_yticklabels('{}%'.format(int(y)) for y in ax.get_yticks())

handles, labels = ax.get_legend_handles_labels()
leg = plt.legend(reversed(handles), reversed(labels),loc='upper left',title="Paths")
leg._legend_box.align = "left"
plt.savefig('output/sri_per_host_and_target_protocol.pdf', bbox_inches='tight')

# Write the Latex variables
with open("output/sri_per_host_and_target_protocol.tex", 'w') as out:
    out.write("\def\\CCSriHttpsInheritanceN{\\num{" + str(https_inheritence_count) + "}\\xspace}\n")
    out.write("\def\\CCSriHttpsInheritanceP{\\num{" + ("%0.2f" % https_inheritence_percentage) + "}\%\\xspace}\n")
