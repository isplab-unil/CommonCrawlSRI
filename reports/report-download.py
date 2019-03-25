from pyspark.shell import sqlContext

# Load the parquet files
sqlContext.read.parquet("output-local/*.parquet").registerTempTable("cc")
sqlContext.read.parquet("../output-local/*.parquet").registerTempTable("cc")

sqlContext.sql("""
SELECT count(*) FROM cc
""").show(20, False)

sqlContext.sql("""
SELECT count(*) FROM cc WHERE has_keyword
""").show(20, False)

sqlContext.sql("""
SELECT count(*) FROM cc WHERE has_checksum
""").show(20, False)

sqlContext.sql("""
SELECT url, content, checksums FROM cc WHERE has_checksum AND url LIKE "%filehippo%"
""").show(100, False)