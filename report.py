from pyspark.shell import sqlContext

sqlContext.read.parquet("output/output/*.parquet").registerTempTable("cc")

# Count the number of URIs
sqlContext.sql("select count(*) as uris from cc").show()

# Count the number of warc files
sqlContext.sql("select count(DISTINCT warc) as warcs from cc").show()

# Count the URIs by origin
sqlContext.sql("select if(uri like 'https%', 'https', if(uri like 'http%', 'http', 'other')) as source, count(*) as number, 100.0 * count(*) / (select count(*) FROM cc) as percentage from cc group by source").show()

# Count the number of sites having SRIs
sqlContext.sql("select count(*) as number, count(*) / (select count(*) as uris from cc) as percentage from cc where size(subresources) > 0").show()

# Distribution of the number of SRI per number of pages
sqlContext.sql("select size(filter(subresources, subresource -> subresource.integrity != '')) as sri, count(*) as number from cc where has_subresource = true and size(filter(subresources, subresource -> subresource.integrity != '')) > 0 group by sri order by sri asc").show()


