from pyspark.shell import sqlContext

# ---------------------------
# --------- DATA ------------
# ---------------------------

# Load the parquet files
sqlContext.read.parquet("*.parquet").registerTempTable("cc")
sqlContext.read.csv('../input/top-1m-cisco.csv').registerTempTable('top1m')


# ---------------------------
# ----- VERIFICATIONS -------
# ---------------------------

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


# ---------------------------
# -------- QUERIES ----------
# ---------------------------

# What is the number of pages by protocol?
sqlContext.sql("""
SELECT 
    if(url LIKE 'https%', 'https', if(url LIKE 'http%', 'http', 'other')) AS protocol, 
    round(100 * count(*) / (SELECT count(*) FROM cc), 2) AS percentage 
FROM cc 
GROUP BY protocol
""").show(20, False)

# What is the percentage of pages that includes at least one tag with the integrity attribute?
sqlContext.sql("""
SELECT 
    round(100 * count(*) / (SELECT count(*) FROM cc), 2) AS percentage 
FROM cc 
WHERE size(filter(subresources, s -> s.integrity IS NOT NULL)) > 0
""").show(20, False)

# What is the percentage of pages that includes at least one script with the integrity attribute?
sqlContext.sql("""
SELECT 
    round(100 * count(*) / (SELECT count(*) FROM cc), 2) AS percentage 
FROM cc 
WHERE size(filter(subresources, s -> s.name == 'script' AND s.integrity IS NOT NULL)) > 0
""").show(20, False)

# What is the percentage of pages that includes at least one link with the integrity attribute?
sqlContext.sql("""
SELECT 
    round(100 * count(*) / (SELECT count(*) FROM cc), 2) AS percentage 
FROM cc 
WHERE size(filter(subresources, s -> s.name == 'link' AND s.integrity IS NOT NULL)) > 0
""").show(20, False)

# ---------------------------

# What is the distribution of pages by number of SRI tags?
sqlContext.sql("""
SELECT 
    size(filter(subresources, s -> s.integrity IS NOT NULL)) AS tags, 
    count(*) AS number 
FROM cc 
WHERE size(filter(subresources, s -> s.integrity IS NOT NULL)) > 0 
GROUP BY tags 
ORDER BY tags ASC
""").show(20, False)

# What is the distribution of pages by script tags?
sqlContext.sql("""
SELECT 
    size(filter(subresources, s -> s.name == 'script' AND s.integrity IS NOT NULL)) AS tags, 
    count(*) AS number 
FROM cc 
WHERE size(filter(subresources, s -> s.name == 'script' AND s.integrity IS NOT NULL)) > 0 
GROUP BY tags 
ORDER BY tags ASC
""").show(20, False)

# What is the distribution of pages by link tags?
sqlContext.sql("""
SELECT 
    size(filter(subresources, s -> s.name == 'link' AND s.integrity IS NOT NULL)) AS tags, 
    count(*) AS number 
FROM cc 
WHERE size(filter(subresources, s -> s.name == 'link' AND s.integrity IS NOT NULL)) > 0 
GROUP BY tags 
ORDER BY tags ASC
""").show(20, False)

# ---------------------------

# What is the distribution of tags with integrity attribute per protocol?
sqlContext.sql("""
SELECT if(sri.target LIKE 'https%', 'https', if(sri.target LIKE 'http%', 'http', if(url LIKE 'https%', 'https', 'http'))) AS protocol, count(*) as tag_targets FROM (
    SELECT url, filter(subresources, s -> s.integrity IS NOT NULL) AS subresources 
    FROM cc 
    WHERE size(filter(subresources, s -> s.integrity IS NOT NULL)) > 0
) LATERAL VIEW explode(subresources) T AS sri
GROUP BY protocol
ORDER BY tag_targets DESC
""").show(20, False)

# What is the distribution of script tags with integrity attribute per protocol?
sqlContext.sql("""
SELECT if(sri.target LIKE 'https%', 'https', if(sri.target LIKE 'http%', 'http', if(url LIKE 'https%', 'https', 'http'))) AS protocol, count(*) as tag_targets FROM (
    SELECT url, filter(subresources, s -> s.name == 'script' AND s.integrity IS NOT NULL) AS subresources 
    FROM cc 
    WHERE size(filter(subresources, s -> s.name == 'script' AND s.integrity IS NOT NULL)) > 0
) LATERAL VIEW explode(subresources) T AS sri
GROUP BY protocol
ORDER BY tag_targets DESC
""").show(20, False)

# What is the distribution of link tags with integrity attribute per protocol?
sqlContext.sql("""
SELECT if(sri.target LIKE 'https%', 'https', if(sri.target LIKE 'http%', 'http', if(url LIKE 'https%', 'https', 'http'))) AS protocol, count(*) as tag_targets FROM (
    SELECT url, filter(subresources, s -> s.name == 'link' AND s.integrity IS NOT NULL) AS subresources 
    FROM cc 
    WHERE size(filter(subresources, s -> s.name == 'link' AND s.integrity IS NOT NULL)) > 0
) LATERAL VIEW explode(subresources) T AS sri
GROUP BY protocol
ORDER BY tag_targets DESC
""").show(20, False)

# ---------------------------

# What is the percentage of tags that specifies the integrity attribute on a page that includes at least one tag with the integrity attribute?
sqlContext.sql("""
SELECT 
    round(100 * sum(size(filter(subresources, s -> s.integrity IS NOT NULL))) / sum(size(subresources)), 2) AS percentage
FROM cc 
WHERE size(filter(subresources, s -> s.integrity IS NOT NULL)) > 0
""").show(20, False)

# What is the percentage of script tags that specifies the integrity attribute on a page that includes at least one tag with the integrity attribute?
sqlContext.sql("""
SELECT 
    round(100 * sum(size(filter(subresources, s -> s.name == 'script' AND s.integrity IS NOT NULL))) / sum(size(subresources)), 2) AS percentage
FROM cc 
WHERE size(filter(subresources, s -> s.name == 'script' AND s.integrity IS NOT NULL)) > 0
""").show(20, False)

# What is the percentage of link tags that specifies the integrity attribute on a page that includes at least one tag with the integrity attribute?
sqlContext.sql("""
SELECT 
    round(100 * sum(size(filter(subresources, s -> s.name == 'link' AND s.integrity IS NOT NULL))) / sum(size(subresources)), 2) AS percentage
FROM cc 
WHERE size(filter(subresources, s -> s.name == 'link' AND s.integrity IS NOT NULL)) > 0
""").show(20, False)


# ---------------------------

# What is the number of pages by hashing algorithms?
sqlContext.sql("""
SELECT 
    substr(trim(sri.integrity), 0, 6) as hash, 
    count(*)
FROM cc LATERAL VIEW explode(subresources) T AS sri
WHERE size(filter(subresources, s -> s.integrity IS NOT NULL)) > 0
  AND sri.integrity IS NOT NULL
GROUP BY hash
""").show(20, False)

# The list of webpages that contains a tag with an empty integrity attribute
sqlContext.sql("""
SELECT 
    cc.url,
    sri.integrity
FROM cc LATERAL VIEW explode(subresources) T AS sri
WHERE sri.integrity IS NOT NULL
  AND sri.integrity LIKE ''
""").show(100, False)

# ---------------------------

# What are the most popular sub-resources urls included in pages
sqlContext.sql("""
SELECT 
    substr(sri.target, instr(sri.target, '//') + 2) AS library, 
    count(*) AS number
FROM cc LATERAL VIEW explode(subresources) T AS sri
WHERE sri.target IS NOT NULL 
  AND sri.target LIKE 'http%'
  AND sri.integrity IS NOT NULL
GROUP BY library
ORDER BY number DESC
""").show(20, False)

# What are the most popular sub-resources domains included in pages
sqlContext.sql("""
SELECT 
    substring_index(substring_index(sri.target, '/', 3), '/', -1) AS domain, 
    count(*) AS number
FROM cc LATERAL VIEW explode(subresources) T AS sri
WHERE sri.target IS NOT NULL 
  AND sri.target LIKE 'http%'
  AND sri.integrity IS NOT NULL
GROUP BY domain
ORDER BY number DESC
""").show(20, False)

# What are the most popular script sub-resources domains included in pages
sqlContext.sql("""
SELECT 
    substring_index(substring_index(sri.target, '/', 3), '/', -1) AS domain, 
    count(*) AS number
FROM cc LATERAL VIEW explode(subresources) T AS sri
WHERE sri.name == 'script' 
  AND sri.target IS NOT NULL 
  AND sri.target LIKE 'http%'
  AND sri.integrity IS NOT NULL
GROUP BY domain
ORDER BY number DESC
""").show(20, False)

# What are the most popular link sub-resources domains included in pages
sqlContext.sql("""
SELECT 
    substring_index(substring_index(sri.target, '/', 3), '/', -1) AS domain, 
    count(*) AS number
FROM cc LATERAL VIEW explode(subresources) T AS sri
WHERE sri.name == 'link' 
  AND sri.target IS NOT NULL 
  AND sri.target LIKE 'http%'
  AND sri.integrity IS NOT NULL
GROUP BY domain
ORDER BY number DESC
""").show(20, False)

# The list of web pages having a csp policy
sqlContext.sql("""
SELECT url, csp
FROM cc
WHERE csp IS NOT NULL
""").show(100, False)

# The list of web pages having a cors policy
sqlContext.sql("""
SELECT url, cors
FROM cc
WHERE cors IS NOT NULL
  AND url LIKE '%login%'
""").show(100, False)

