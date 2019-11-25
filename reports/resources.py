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

#---

sql("""
SELECT 
 count(*) as webpages,
 sum(if(size(cc.subresources) > 0, 1, 0)) as webpages_with_subresources,
 sum(if(size(cc.subresources) > 0, 1, 0)) / count(*) * 100 as webpages_with_subresources_percentate,
 mean(size(cc.subresources)) as subresources_per_webpage_mean,
 stddev(size(cc.subresources)) as subresources_per_webage_stddev
FROM cc
""")

sql("""
SELECT 
 count(*) as webpages,
 sum(if(size(subresources) > 0, 1, 0)) as webpages_with_subresources,
 sum(if(size(subresources) > 0, 1, 0)) / count(*) * 100 as webpages_with_subresources_percentate,
 mean(size(subresources)) as subresources_per_webpage_mean,
 stddev(size(subresources)) as subresources_per_webage_stddev
FROM (
    SELECT 
        url as url, 
        filter(subresources, s -> s.target IS NOT NULL AND parse_url(s.target, 'HOST') NOT LIKE parse_url(cc.url, 'HOST')) as subresources
    FROM cc
)
""")

