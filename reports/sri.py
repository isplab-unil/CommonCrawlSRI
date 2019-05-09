from pyspark.shell import sqlContext

# ---------------------------
# --------- DATA ------------
# ---------------------------

# Load the parquet files
sqlContext.read.parquet("../output/*.parquet").registerTempTable("cc")


# ---------------------------
# ------ UTILITIES ----------
# ---------------------------

def saveResults(name, sql):
    sqlContext.sql(sql).repartition(1).write.mode('overwrite').parquet(name)


# ---------------------------
# ----- VERIFICATIONS -------
# ---------------------------

saveResults("00_count.csv", "SELECT count(*) as count FROM cc")

# ---------------------------
# -------- QUERIES ----------
# ---------------------------

# Q1: What is the number of pages by protocol?

saveResults("01_pages_per_protocol", """
SELECT 
    if(url LIKE 'https%', 'https', if(url LIKE 'http%', 'http', 'other')) AS protocol, 
    count(*) AS number,
    (SELECT count(*) FROM cc) AS total,
    round(100 * count(*) / (SELECT count(*) FROM cc), 2) AS percentage 
FROM cc 
GROUP BY protocol
""")

# ---------------------------

# Q2: What is the number of pages that include at least one SRI?

saveResults("02_pages_with_sri", """
SELECT 
    count(*) AS number,
    (SELECT count(*) FROM cc) AS total,
    round(100 * count(*) / (SELECT count(*) FROM cc), 2) AS percentage 
FROM cc 
WHERE size(filter(subresources, s -> s.integrity IS NOT NULL)) > 0
""")

saveResults("02_pages_with_sri_script", """
SELECT 
    count(*) AS number,
    (SELECT count(*) FROM cc) AS total,
    round(100 * count(*) / (SELECT count(*) FROM cc), 2) AS percentage 
FROM cc 
WHERE size(filter(subresources, s -> s.name == 'script' AND s.integrity IS NOT NULL)) > 0
""")

saveResults("02_pages_with_sri_link", """
SELECT 
    count(*) AS number,
    (SELECT count(*) FROM cc) AS total,
    round(100 * count(*) / (SELECT count(*) FROM cc), 2) AS percentage 
FROM cc 
WHERE size(filter(subresources, s -> s.name == 'link' AND s.integrity IS NOT NULL)) > 0
""")

# ---------------------------

# Q3: What is the number of pages per number of number SRI?
saveResults("03_page_per_sri", """
SELECT 
    size(filter(subresources, s -> s.integrity IS NOT NULL)) AS sri, 
    count(*) AS number,
    round(100 * count(*) / (
        SELECT count(*)
        FROM cc
        WHERE size(filter(subresources, s -> s.integrity IS NOT NULL)) > 0
    ), 2) AS percentage
FROM cc 
WHERE size(filter(subresources, s -> s.integrity IS NOT NULL)) > 0 
GROUP BY sri 
ORDER BY sri ASC
""")

saveResults("03_page_per_sri_script", """
SELECT 
    size(filter(subresources, s -> s.name == 'script' AND s.integrity IS NOT NULL)) AS sri, 
    count(*) AS number,
    round(100 * count(*) / (
        SELECT count(*)
        FROM cc
        WHERE size(filter(subresources, s -> s.name == 'script' AND s.integrity IS NOT NULL)) > 0
    ), 2) AS percentage
FROM cc 
WHERE size(filter(subresources, s -> s.name == 'script' AND s.integrity IS NOT NULL)) > 0 
GROUP BY sri 
ORDER BY sri ASC
""")

saveResults("03_page_per_sri_link", """
SELECT 
    size(filter(subresources, s -> s.name == 'link' AND s.integrity IS NOT NULL)) AS sri, 
    count(*) AS number,
    round(100 * count(*) / (
        SELECT count(*)
        FROM cc
        WHERE size(filter(subresources, s -> s.name == 'link' AND s.integrity IS NOT NULL)) > 0
    ), 2) AS percentage
FROM cc 
WHERE size(filter(subresources, s -> s.name == 'link' AND s.integrity IS NOT NULL)) > 0 
GROUP BY sri 
ORDER BY sri ASC
""")

# ---------------------------

# Q4: What is the number of SRI per hash algorithm?

saveResults("04_sri_per_alg", """
SELECT 
    substring_index(trim(hash), '-', 1) as alg, 
    count(*) as number
FROM cc LATERAL VIEW explode(subresources) T AS sri LATERAL VIEW explode(split(sri.integrity, ' ')) AS hash
WHERE size(filter(subresources, s -> s.integrity IS NOT NULL)) > 0
  AND sri.integrity IS NOT NULL
GROUP BY alg
ORDER BY number DESC
""")

# ---------------------------

# Q5: Are there invalid integrity attributes in the dataset?
saveResults("05_invalid_integrity_attributes", """
SELECT
    cc.url,
    sri.target,
    trim(hash) as hash,
    length(trim(hash)) as length
FROM cc LATERAL VIEW explode(subresources) T AS sri LATERAL VIEW explode(split(sri.integrity, ' ')) AS hash
WHERE hash IS NOT NULL
  AND trim(hash) != ""
  AND length(trim(hash)) != 95 -- sha512
  AND length(trim(hash)) != 71 -- sha384
  AND length(trim(hash)) != 51 -- sha256
""")

# ---------------------------

# Q6: What is the distribution of SRI per protocol?

saveResults("06_sri_per_protocol", """
SELECT if(sri.target LIKE 'https%', 'https', if(sri.target LIKE 'http%', 'http', if(url LIKE 'https%', 'https', 'http'))) AS protocol, count(*) as sri FROM (
    SELECT url, filter(subresources, s -> s.integrity IS NOT NULL) AS subresources 
    FROM cc 
    WHERE size(filter(subresources, s -> s.integrity IS NOT NULL)) > 0
) LATERAL VIEW explode(subresources) T AS sri
GROUP BY protocol
ORDER BY protocol DESC
""")

# ---------------------------

# Q7: What is the number of elements per target protocol?

from urllib.parse import urljoin
from urllib.parse import urlparse
from operator import add

select = sqlContext.sql("""
SELECT 
    url as url,
    sri.target as sri
FROM cc LATERAL VIEW explode(subresources) T AS sri
WHERE sri.integrity IS NOT NULL
""")

def parse(r):
    h = urlparse(r.url)
    t = urlparse(urljoin(r.url, r.sri))
    return ((h.scheme, t.scheme, 'l' if h.netloc == t.netloc else 'r'), 1)


select.rdd.map(parse).reduceByKey(add).write.mode('overwrite').parquet("08_elements_per_protocol")

# ---------------------------

# Q8: Top-k urls and domains among sri

saveResults("08_topk_sri_url", """
SELECT 
    substr(sri.target, instr(sri.target, '//') + 2) AS library, 
    count(*) AS number,
    round(100 * count(*) / (
        SELECT count(*) 
        FROM cc LATERAL VIEW explode(subresources) T AS sri
        WHERE sri.target IS NOT NULL 
        AND instr(substring_index(substring_index(sri.target, '/', 3), '/', -1), '.') > 0
        AND sri.integrity IS NOT NULL
    ), 2) AS percentage 
FROM cc LATERAL VIEW explode(subresources) T AS sri
WHERE sri.target IS NOT NULL 
  AND instr(substring_index(substring_index(sri.target, '/', 3), '/', -1), '.') > 0
  AND sri.integrity IS NOT NULL
GROUP BY library
ORDER BY number DESC
""")

saveResults("08_topk_sri_domain", """
SELECT 
    substring_index(substring_index(sri.target, '/', 3), '/', -1) AS domain, 
    count(*) AS number,
    round(100 * count(*) / (
        SELECT count(*) 
        FROM cc LATERAL VIEW explode(subresources) T AS sri
        WHERE sri.target IS NOT NULL 
        AND instr(substring_index(substring_index(sri.target, '/', 3), '/', -1), '.') > 0
        AND sri.integrity IS NOT NULL
    ), 2) AS percentage 
FROM cc LATERAL VIEW explode(subresources) T AS sri
WHERE sri.target IS NOT NULL 
  AND instr(substring_index(substring_index(sri.target, '/', 3), '/', -1), '.') > 0
  AND sri.integrity IS NOT NULL
GROUP BY domain
ORDER BY number DESC
""")

# ---------------------------

# Q9: What is the distribution of the values for the crossorigin attribute?

saveResults("09_crossorigin_values", """
SELECT
    trim(sri.crossorigin) AS value,
    count(*) AS number,
    round(100 * count(*) / (
        SELECT count(*) 
        FROM cc LATERAL VIEW explode(subresources) T AS sri
        WHERE sri.crossorigin IS NOT NULL 
    ), 2) AS percentage  
FROM cc LATERAL VIEW explode(subresources) T AS sri
WHERE sri.crossorigin IS NOT NULL 
GROUP BY trim(sri.crossorigin)
ORDER BY number DESC
""")

saveResults("09_crossorigin_use_credentials", """
SELECT
    cc.url,
    sri.target,
    sri.crossorigin
FROM cc LATERAL VIEW explode(subresources) T AS sri
WHERE sri.crossorigin = 'use-credentials'
  AND substring_index(substring_index(url, '/', 3), '/', -1) != substring_index(substring_index(sri.target, '/', 3), '/', -1)
""")

# ---------------------------

# Q10: Among the pages that contains SRI, how many of them specify the require-sri-for CSP?

saveResults("10_require_sri_for", """
SELECT  
    count(DISTINCT cc.url) as number,
    round(100 * count(DISTINCT cc.url) / (
        SELECT count(DISTINCT cc.url)
        FROM cc LATERAL VIEW explode(subresources) T AS sri
        WHERE sri.integrity IS NOT NULL 
    ), 4) AS percentage  
FROM cc LATERAL VIEW explode(subresources) T AS sri
WHERE sri.integrity IS NOT NULL AND csp LIKE "%require-sri-for%"
""")




