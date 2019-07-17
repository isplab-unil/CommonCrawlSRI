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
SELECT substr(target, instr(target, '//') + 2) AS path, count(*) AS number
FROM (
    SELECT 
        subresource.target AS target
    FROM cc LATERAL VIEW explode(subresources) T AS subresource
    WHERE
        (subresource.target LIKE 'http://%' OR subresource.target LIKE 'https://%' OR subresource.target LIKE '//%') AND
        (subresource.target LIKE '%.js' OR subresource.target LIKE '%.css') 
)
GROUP BY path
ORDER BY number DESC
""")


saveResults("top_resources", """
SELECT substr(target, instr(target, '//') + 2) AS path, count(*) AS number
FROM (
    SELECT 
        subresource.target AS target
    FROM cc LATERAL VIEW explode(subresources) T AS subresource
    WHERE
        (subresource.target LIKE 'http://%' OR subresource.target LIKE 'https://%' OR subresource.target LIKE '//%') AND
        (subresource.target LIKE '%.js' OR subresource.target LIKE '%.css') AND 
        parse_url(url, 'HOST') != parse_url(subresource.target, 'HOST')
)
GROUP BY path
ORDER BY number DESC
""")