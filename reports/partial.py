from pyspark.shell import sqlContext
import sys

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
sqlContext.read.parquet("../output/*.parquet").registerTempTable("cc")

def saveResults(name, sql):
    sqlContext.sql(sql).repartition(1).write.mode('overwrite').parquet(name)

def loadResult(name):
    return sqlContext.read.parquet(name)

def sql(sql):
    sqlContext.sql(sql).show(n=20, truncate=False)

saveResults("top_resources", """
SELECT 
substr(subresource.target, instr(subresource.target, '//') + 2) AS path, 
    count(*) AS number
FROM cc LATERAL VIEW explode(subresources) T AS subresource
GROUP BY path
HAVING path LIKE '%.js' OR path LIKE '%.css'
ORDER BY number DESC
""")
