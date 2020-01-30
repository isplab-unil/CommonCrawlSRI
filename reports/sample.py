def saveCsv(name, sql):
    sqlContext.sql(sql).repartition(1).write.format("csv").option("header", "false").save(name)

saveCsv("sample", "SELECT url FROM cc WHERE hash(url) % 1000000 == 0 ORDER BY hash(url) LIMIT 1000")
