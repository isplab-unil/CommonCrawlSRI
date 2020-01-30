#!/usr/bin/env python

import re
import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.cbook as cbook
import numpy as np 
from sklearn.linear_model import LinearRegression
from utils import loadSnapshots

# Initialize data
data = loadSnapshots('../data/sri/*/queries/sri_per_page_evolution/')

# Draw the plot
plt.rcParams["figure.figsize"] = (10, 3.5)
plt.rc('font',**{'family':'serif','size':'16'})
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42

ax = data['all_mean'].plot(label='total', style='bo-')
plt.fill_between(data.index, data['all_mean'] - data['all_stddev'], data['all_mean'] + data['all_stddev'], alpha=0.4, color="blue")

data['sri_mean'].plot(label='w/ integrity', style='gs-')
plt.fill_between(data.index, data['sri_mean'] - data['sri_stddev'], data['sri_mean'] + data['sri_stddev'], alpha=0.4, color="green")

ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

plt.xlabel('')
plt.ylabel('num. of subresources\nper webpage')
plt.xticks(rotation=30)

leg = plt.legend(loc='upper left',title="Subresources")
leg._legend_box.align = "left"

plt.ylim(-5, 95)
plt.xlim(datetime.date(2016, 4, 1), datetime.date(2019, 10, 1))

ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
ax.xaxis.set_minor_locator(mdates.MonthLocator())

plt.savefig('output/sri_per_page_evolution.pdf', bbox_inches = 'tight')

# Compute the regression coefficient
plt.clf()
model = LinearRegression()
X = data['sri_mean'].index.astype(int).values.reshape(-1, 1) 
Y = data['sri_mean'].values.reshape(-1, 1)
model.fit(X, Y)
Y_pred = model.predict(X)
plt.scatter(X, Y)
plt.plot(X, Y_pred, color='red')
plt.savefig('output/sri_per_page_evolution_coefficient.pdf', bbox_inches = 'tight')

# Write the latex variables
with open("output/sri_per_page_evolution.tex", 'w') as out:
    lastCrawl = data.reset_index()[-1:]
    out.write("\def\\CCSubresourcesPerPageN{\\num{" + ("%0.2f" % lastCrawl['all_mean']) + "}\\xspace}\n")
    out.write("\def\\CCSubresourcesPerPageWithSriN{\\num{" + ("%0.2f" % lastCrawl['sri_mean']) + "}\\xspace}\n")

# Write the json file
with open("output/sri_per_page_evolution.json", 'w') as out:
    out.write(data.to_json(orient='split'))
