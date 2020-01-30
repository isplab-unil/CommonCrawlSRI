#!/usr/bin/env python

import re
import pandas as pd
import matplotlib.pyplot as plt
from utils import loadSnapshot

# Initialize the data
def loadSnapshotExt(file):
    data = loadSnapshot(file)
    data['percentage'] = data['number'] / data['number'].sum() * 100
    data = data.set_index('sri')
    return data

dataMixed = loadSnapshotExt('../data/sri/2019-35/queries/sri_per_page2/')
dataLink = loadSnapshotExt('../data/sri/2019-35/queries/sri_per_page_link/')
dataScript = loadSnapshotExt('../data/sri/2019-35/queries/sri_per_page_script/')

# Draw the Figure
plt.rc('font',**{'family':'serif','size':'16'})
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42
plt.rcParams["figure.figsize"] = (10, 3.5)

fig, ax = plt.subplots(1, 1)

ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.yaxis.grid()

ax.plot(dataLink.index-0.33, dataLink['percentage'].cumsum(), "mD-", label="link")
ax.plot(dataScript.index, dataScript['percentage'].cumsum(), "cs-", label="script")
ax.plot(dataMixed.index+0.33, dataMixed['percentage'].cumsum(), "go-", label="any")

ax.bar(dataLink.index-0.33, dataLink['percentage'], 0.27, label='', color="#BF01BF")
ax.bar(dataScript.index, dataScript['percentage'], 0.27, label='', color="#00BFC0")
ax.bar(dataMixed.index+0.33, dataMixed['percentage'], 0.27, label='', color="green")

ax.set_yticklabels(['{:,.0%}'.format(x / 100) for x in ax.get_yticks()])

plt.xlim(0, 10)
plt.xlabel('num. of subresources w/ integrity')
plt.ylabel('prop. of webpages')

leg = plt.legend(loc='lower right',title="Element")
leg._legend_box.align = "left"

plt.xticks(dataMixed.index[:10])

plt.savefig('output/sri_per_page.pdf', bbox_inches='tight')

# Write the Latex variable
with open("output/sri_per_page.tex", 'w') as out:
    out.write("\def\\CCOneSriPerWebpageN{\\num{" + ("%0.2f" % dataMixed['number'][1]) + "}\\xspace}\n")
    out.write("\def\\CCOneSriPerWebpageP{\\num{" + ("%0.2f" % dataMixed['percentage'][1]) + "}\%\\xspace}\n")
    out.write("\def\\CCTwoSriPerWebpageN{\\num{" + ("%0.2f" % dataMixed['number'][2]) + "}\\xspace}\n")
    out.write("\def\\CCTwoSriPerWebpageP{\\num{" + ("%0.2f" % dataMixed['percentage'][2]) + "}\%\\xspace}\n")
    out.write("\def\\CCOneLinkSriPerWebpageN{\\num{" + ("%0.2f" % dataLink['number'][1]) + "}\\xspace}\n")
    out.write("\def\\CCOneLinkSriPerWebpageP{\\num{" + ("%0.2f" % dataLink['percentage'][1]) + "}\%\\xspace}\n")

# Write JSON
with open("output/sri_per_page.json", 'w') as out:
    json = pd.concat([dataMixed, dataLink, dataScript, dataMixed.cumsum(), dataLink.cumsum(), dataScript.cumsum()], keys=['any', 'link', 'script', 'any_cdf', 'link_cdf', 'script_cdf'])
    out.write(json.to_json(orient='split'))

