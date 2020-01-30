#!/usr/bin/env python
import random
import pandas as pd
from urllib.parse import urlparse

# Initialize the data
data = pd.read_csv('../data/sri/2019-35/queries/require_sri_for_pages/part-00000-47105db6-e7e0-41b9-a2b1-4a9c68c22bd0-c000.csv', header=None)

urls = []
previous = None
for url in data[0].sort_values().iteritems():
    url = url[1]
    domain = urlparse(url).netloc
    if previous != domain:
        previous = domain
        urls.append(url)

with open("output/require_sri_for_sample.csv", 'w') as out:
    out.write(pd.Series(urls).sample(frac=1, random_state=1).to_csv())
