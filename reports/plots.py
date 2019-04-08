import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

data = pd.read_csv('../output-sri/03_page_per_sri.csv/part-00000-2f849758-124d-42a9-b901-7ff7d3a97727-c000.csv')
axis = range(min(data.sri), max(data.sri) + 1)

data['pdf'] = data.page / data.page.sum() * 100
data['cdf'] = data.pdf.cumsum()

plt.plot('sri', 'cdf', data=data)
plt.bar('sri', 'pdf', data=data)
plt.xticks(axis)
plt.xlabel("# sri")
plt.ylabel("% pages")

plt.show()
