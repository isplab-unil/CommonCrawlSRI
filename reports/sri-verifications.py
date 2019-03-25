from pyspark.shell import sqlContext

# Load the parquet files
sqlContext.read.parquet("../output-local/*.parquet").registerTempTable("cc")

sqlContext.sql("""
SELECT count(*) FROM cc
""").show(20, False)

sqlContext.sql("""
SELECT count(*) FROM cc WHERE error is not NULL 
""").show(20, False)

sqlContext.sql("""
SELECT count(*) FROM cc WHERE has_subresource
""").show(20, False)

sqlContext.sql("""
SELECT subresources.attributes FROM cc WHERE has_subresource
""").show(20, False)

sqlContext.sql("""
SELECT csp FROM cc WHERE csp is not NULL 
""").show(20, False)

sqlContext.sql("""
SELECT cors FROM cc WHERE cors is not NULL 
""").show(20, False)

sqlContext.sql("""
SELECT subresources.crossorigin FROM cc WHERE size(filter(subresources, s -> s.crossorigin IS NOT NULL)) > 0
""").show(100, False)

sqlContext.sql("""
SELECT subresources.referrerpolicy FROM cc WHERE size(filter(subresources, s -> s.referrerpolicy IS NOT NULL)) > 0
""").show(100, False)
