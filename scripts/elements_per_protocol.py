#!/usr/bin/env python

import re
from glob import iglob
import pandas as pd
import matplotlib.pyplot as plt
from IPython.display import display, Math
from pyspark.shell import sqlContext

# Initialize the data
data = sqlContext.read.parquet('../data/sri/2019-35/queries/07_elements_per_protocol/').toPandas().reindex()
data['src'] = data['_1'].apply(lambda t: t[0])
data['dst'] = data['_1'].apply(lambda t: t[1])
data['loc'] = data['_1'].apply(lambda t: t[2])
data['num'] = data['_2']
data = data.loc[((data['src'] == 'http') | (data['src'] == 'https')) & 
         ((data['dst'] == 'http') | (data['dst'] == 'https')) &
         ((data['loc'] == 'l') | (data['loc'] == 'r'))]
data = data.filter(['src', 'dst', 'loc', 'num', 'per'])
data['per'] = data['num'] / data['num'].sum() *100

total = data['num'].sum()
src = data.groupby(['src'])
dst = data.groupby(['dst'])
srcdst = data.groupby(['src', 'dst'])
srcdstloc = data.groupby(['src', 'dst', 'loc'])

# Write Latex
out = open("output/elements_per_protocol.tex", 'w')

def cap(x):
    return x[0].upper() + x[1:]

def print_latex(name, var):
    out.write("\def\\" + name + "{\\num{" + ("%0.2f" % var) + "}\%}\n")

print_latex('CCTreeTotal', 100)

for name, group in src:
    num = group['num'].sum()
    per = num / total * 100
    rel = num / total * 100
    print_latex('CCTree' + cap(name), per)
    print_latex('CCTree' + cap(name) + "Rel", rel)
    
for (s, d), group in srcdst:
    num = group['num'].sum()
    per = num / total * 100
    rel = num / src.get_group(s)['num'].sum() * 100
    print_latex('CCTree' + cap(s) + cap(d), per)
    print_latex('CCTree' + cap(s) + cap(d) + "Rel", rel)
    
for (s, d, l), group in srcdstloc:
    num = group['num'].sum()
    per = num / total * 100
    rel = num / srcdst.get_group((s, d))['num'].sum() * 100
    l = 'Local' if l == 'l' else 'Remote'
    print_latex('CCTree' + cap(s) + cap(d) + l, per)
    print_latex('CCTree' + cap(s) + cap(d) + l + "Rel", rel)

out.close()
