import re
import datetime
from glob import iglob
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.cbook as cbook
from pyspark.shell import sqlContext

def loadSnapshots(path):
    frames = []
    for file in iglob(path, recursive=True):
        date = re.findall(r'/(\d{4}-\d{2})/', file)
        date = datetime.datetime.strptime(date[0] + "-1", "%Y-%W-%w")
        frame = loadSnapshot(file)
        frame['date'] = date
        frames.append(frame)
    return pd.concat(frames, ignore_index=True).set_index('date').sort_index()

def loadSnapshot(path):
    return sqlContext.read.parquet(path).toPandas()

    
