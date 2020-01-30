#!/usr/bin/env python

import re
import pandas as pd
import matplotlib.pyplot as plt
from utils import loadSnapshot

# Initialize the data
data = loadSnapshot('../data/sri/2019-35/queries/sri_per_algorithm/')
data['percentage'] = data['number'] / data['number'].sum() * 100
data = data.groupby('algorithms').sum().sort_values(by=['percentage'], ascending=False)

empty = data[data.index.isin([''])].rename(index={'': 'empty'})
valid = data.filter(regex='^(\+{0,1}(sha256|sha384|sha512))(\+(sha256|sha384|sha512))*$', axis=0)
malformed = data[~data.index.isin(valid.index.append(empty.index))].groupby(lambda x: 'malformed').sum()
other = valid[7:].groupby(lambda x: 'other').sum()
data = valid[:7].append(other).append(empty).append(malformed)
data.index = data.index.str.replace('\+sha','+')

# Draw the figure
pd.set_option('display.max_rows', 300)
plt.rc('font',**{'family':'serif','size':'16'})
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42
plt.rcParams["figure.figsize"] = (10, 3.5)

fig, ax = plt.subplots(1, 1)

data['percentage'].plot.bar(ax=ax, sharex=True, rot='xticks', color='#2077B4')

ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.yaxis.grid()

for i, v in enumerate(data['percentage']):
    ax.text(i-0.4, v + 2, '{:.2f}%'.format(v))

ax.set_yticklabels(['{:,.0%}'.format(x / 100) for x in ax.get_yticks()])
    
plt.xlim(-0.5,len(data) - 0.5)
plt.ylim(0,58)

plt.xlabel('')
plt.ylabel('prop. of subresources\nw/ integrity')

plt.xticks(rotation=30,  ha="right")
plt.savefig('output/sri_per_algorithm.pdf', bbox_inches='tight')

# Write Latex
with open("output/sri_per_algorithm.tex", 'w') as out:
    out.write("\def\\CCSriOtherDiffAlgN{\\num{" + str(int(valid[3:7]['number'].sum())) + "}\\xspace}\n")
    out.write("\def\\CCSriOtherDiffAlgP{\\num{" + ("%0.2f" % valid[3:7]['percentage'].sum()) + "}\%\\xspace}\n")
    out.write("\def\\CCSriOtherSameAlgN{\\num{" + str(int(valid[7:]['number'].sum())) + "}\\xspace}\n")
    out.write("\def\\CCSriOtherSameAlgP{\\num{" + ("%0.4f" % valid[7:]['percentage'].sum()) + "}\%\\xspace}\n")
    out.write("\def\\CCSriEmptyN{\\num{" + str(int(empty['number'].sum())) + "}\\xspace}\n")
    out.write("\def\\CCSriEmptyP{\\num{" + ("%0.2f" % empty['percentage'].sum()) + "}\%\\xspace}\n")
    out.write("\def\\CCSriMalformedN{\\num{" + str(int(malformed['number'].sum())) + "}\\xspace}\n")
    out.write("\def\\CCSriMalformedP{\\num{" + ("%0.2f" % malformed['percentage'].sum()) + "}\%\\xspace}\n")
    for index, row in valid[7:].iterrows():
        out.write("\def\\CCSriOther"+ index.replace("+", "").upper() +"N{\\num{" + str(int(row['number']))+ "}\\xspace}\n")
        out.write("\def\\CCSriOther"+ index.replace("+", "").upper() +"P{\\num{" + ("%0.6f" % row['percentage']) + "}\%\\xspace}\n")

# Write JSON
with open("output/sri_per_algorithm.json", 'w') as out:
    out.write(data.to_json(orient='split'))
