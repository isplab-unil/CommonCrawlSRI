from pyspark.shell import sqlContext
import sys

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
sqlContext.read.parquet("*.parquet").registerTempTable("cc")

def saveResults(name, sql):
    sqlContext.sql(sql).repartition(1).write.mode('overwrite').parquet(name)

def loadResult(name):
    return sqlContext.read.parquet(name)

def sql(sql):
    sqlContext.sql(sql).show(n=20, truncate=False)

sql("""
SELECT * FROM cc LIMIT 10
""")