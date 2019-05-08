from pyspark.shell import sqlContext

# Load the parquet files
sqlContext.read.parquet("*.parquet").registerTempTable("cc")

def sql(sql):
    sqlContext \
        .sql(sql) \
        .show(200, False)


def csv(file, sql):
    sqlContext \
        .sql(sql) \
        .repartition(1) \
        .write.format("csv") \
        .option("header", "true") \
        .save(file)


sql("""
SELECT url, el.target
FROM cc LATERAL VIEW explode(subresources) T AS el
""")



