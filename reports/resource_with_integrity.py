from pyspark.shell import sqlContext
import sys

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

# ---------------------------
# --------- DATA ------------
# ---------------------------

# Load the parquet files and register tables
sqlContext.read.parquet("../output/*.parquet").registerTempTable("cc")

# ---------------------------
# ------ UTILITIES ----------
# ---------------------------


def sql(sql):
    sqlContext.sql(sql).show(n=20, truncate=False)

def saveResults(name, sql):
    sqlContext.sql(sql).repartition(1).write.mode('overwrite').parquet(name)


# ---------------------------
# ------ Queries ------------
# ---------------------------


saveResults("resource_with_integrity", """
SELECT 
    size(filter(subresources, s -> s.integrity IS NOT NULL)) / size(subresources) AS per,
    count(*) AS num
FROM cc 
WHERE size(filter(subresources, s -> s.integrity IS NOT NULL)) > 0 
GROUP BY per
ORDER BY per ASC 
""")

sql("""
SELECT 
    sri.integrity
FROM cc LATERAL VIEW explode(subresources) T AS sri
""")
