#!/usr/bin/env python

import re
from glob import iglob
import pandas as pd
import matplotlib.pyplot as plt
from IPython.display import display, Math
from pyspark.shell import sqlContext

# Initialize the data
data = sqlContext.read.parquet('../data/sri/2019-35/queries/require_sri_for/').toPandas().reindex()

# Write the latex variables
with open("output/require_sri_for.tex", 'w') as out:
    out.write("\def\\CCRequireSriForP{\\num{" + ("%0.2f" % data['percentage']) + "}\%}\n")
    out.close()
