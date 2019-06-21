from pyspark.shell import sqlContext
from urllib.parse import urljoin
from urllib.parse import urlparse
from operator import add
import sys

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

# ---------------------------
# --------- DATA ------------
# ---------------------------

# Load the parquet files and register tables
sqlContext.read.parquet("../output/*.parquet").registerTempTable("cc")
sqlContext.read.csv('../../../top-1m-cisco.csv').registerTempTable('top1m')
sqlContext.read.csv('../../../top-1k-cisco.csv').registerTempTable('top1k')



# ---------------------------
# ------ UTILITIES ----------
# ---------------------------

def saveResults(name, sql):
    sqlContext.sql(sql).repartition(1).write.mode('overwrite').parquet(name)


def loadResult(name):
    return sqlContext.read.parquet(name)


def sql(sql):
    sqlContext.sql(sql).show(n=20, truncate=False)


# ---------------------------
# ----- VERIFICATIONS -------
# ---------------------------

saveResults("00_count", "SELECT count(warc, url) as count FROM cc")
saveResults("00_count_distinct", "SELECT count(DISTINCT warc, url) as count FROM cc")
saveResults("00_count_domains", "SELECT count(DISTINCT substring_index(substring_index(url, '/', 3), '//', -1)) as count FROM cc")

# ---------------------------
# -------- QUERIES ----------
# ---------------------------

# 01: What is the number of pages by protocol?

saveResults("01_pages_per_protocol", """
SELECT 
    if(url LIKE 'https%', 'https', if(url LIKE 'http%', 'http', 'other')) AS protocol, 
    count(*) AS number,
    (SELECT count(*) FROM cc) AS total,
    round(100 * count(*) / (SELECT count(*) FROM cc), 2) AS percentage 
FROM cc 
GROUP BY protocol
""")

saveResults("01_pages_per_protocol_top1m", """
SELECT 
    if(url LIKE 'https%', 'https', if(url LIKE 'http%', 'http', 'other')) AS protocol, 
    count(*) AS number,
    (SELECT count(*) FROM cc, top1m WHERE substring_index(substring_index(url, '/', 3), '/', -1) = _c1) AS total,
    round(100 * count(*) / (SELECT count(*) FROM cc, top1m WHERE substring_index(substring_index(url, '/', 3), '/', -1) = _c1), 2) AS percentage 
FROM cc, top1m
WHERE substring_index(substring_index(url, '/', 3), '/', -1) = _c1
GROUP BY protocol
""")

saveResults("01_pages_per_protocol_top1k", """
SELECT 
    if(url LIKE 'https%', 'https', if(url LIKE 'http%', 'http', 'other')) AS protocol, 
    count(*) AS number,
    (SELECT count(*) FROM cc, top1k WHERE substring_index(substring_index(url, '/', 3), '/', -1) = _c1) AS total,
    round(100 * count(*) / (SELECT count(*) FROM cc, top1k WHERE substring_index(substring_index(url, '/', 3), '/', -1) = _c1), 2) AS percentage 
FROM cc, top1k
WHERE substring_index(substring_index(url, '/', 3), '/', -1) = _c1
GROUP BY protocol
""")

# ---------------------------

# 02: What is the number of pages that include at least one SRI?

saveResults("02_pages_with_sri", """
SELECT 
    count(*) AS number,
    (SELECT count(*) FROM cc) AS total,
    round(100 * count(*) / (SELECT count(*) FROM cc), 2) AS percentage 
FROM cc 
WHERE size(filter(subresources, s -> s.integrity IS NOT NULL)) > 0
""")

saveResults("02_pages_with_sri_top1m", """
SELECT 
    count(*) AS number,
    (SELECT count(*) FROM cc, top1m WHERE substring_index(substring_index(url, '/', 3), '/', -1) = _c1) AS total,
    round(100 * count(*) / (SELECT count(*) FROM cc, top1m WHERE substring_index(substring_index(url, '/', 3), '/', -1) = _c1), 2) AS percentage 
FROM cc, top1m
WHERE size(filter(subresources, s -> s.integrity IS NOT NULL)) > 0
AND substring_index(substring_index(url, '/', 3), '/', -1) = _c1
""")

saveResults("02_pages_with_sri_top1k", """
SELECT 
    count(*) AS number,
    (SELECT count(*) FROM cc, top1k WHERE substring_index(substring_index(url, '/', 3), '/', -1) = _c1) AS total,
    round(100 * count(*) / (SELECT count(*) FROM cc, top1k WHERE substring_index(substring_index(url, '/', 3), '/', -1) = _c1), 2) AS percentage 
FROM cc, top1k
WHERE size(filter(subresources, s -> s.integrity IS NOT NULL)) > 0
AND substring_index(substring_index(url, '/', 3), '/', -1) = _c1
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

# 03: What is the number of pages per number of number SRI?

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

saveResults("03_page_per_sri_evolution_all", """
SELECT 
    sum(size(subresources)) AS all_count,
    max(size(subresources)) AS all_max,
    min(size(subresources)) AS all_min,
    mean(size(subresources)) AS all_mean, 
    stddev(size(subresources)) AS all_stddev,
    sum(size(filter(subresources, s -> s.integrity IS NOT NULL AND s.target IS NOT NULL))) AS sri_count,
    max(size(filter(subresources, s -> s.integrity IS NOT NULL AND s.target IS NOT NULL))) AS sri_max,
    min(size(filter(subresources, s -> s.integrity IS NOT NULL AND s.target IS NOT NULL))) AS sri_min,
    mean(size(filter(subresources, s -> s.integrity IS NOT NULL AND s.target IS NOT NULL))) AS sri_mean, 
    stddev(size(filter(subresources, s -> s.integrity IS NOT NULL AND s.target IS NOT NULL))) AS sri_stddev,
    sum(size(filter(subresources, s -> s.integrity IS NOT NULL AND s.target IS NOT NULL AND s.name == 'link'))) AS link_count,
    max(size(filter(subresources, s -> s.integrity IS NOT NULL AND s.target IS NOT NULL AND s.name == 'link'))) AS link_max,
    min(size(filter(subresources, s -> s.integrity IS NOT NULL AND s.target IS NOT NULL AND s.name == 'link'))) AS link_min,
    mean(size(filter(subresources, s -> s.integrity IS NOT NULL AND s.target IS NOT NULL AND s.name == 'link'))) AS link_mean, 
    stddev(size(filter(subresources, s -> s.integrity IS NOT NULL AND s.target IS NOT NULL AND s.name == 'link'))) AS link_stddev,
    sum(size(filter(subresources, s -> s.integrity IS NOT NULL AND s.target IS NOT NULL AND s.name == 'script'))) AS script_count,
    max(size(filter(subresources, s -> s.integrity IS NOT NULL AND s.target IS NOT NULL AND s.name == 'script'))) AS script_max,
    min(size(filter(subresources, s -> s.integrity IS NOT NULL AND s.target IS NOT NULL AND s.name == 'script'))) AS script_min,
    mean(size(filter(subresources, s -> s.integrity IS NOT NULL AND s.target IS NOT NULL AND s.name == 'script'))) AS script_mean, 
    stddev(size(filter(subresources, s -> s.integrity IS NOT NULL AND s.target IS NOT NULL AND s.name == 'script'))) AS script_stddev
FROM cc 
WHERE size(filter(subresources, s -> s.integrity IS NOT NULL AND s.target IS NOT NULL)) > 0 
""")

# ---------------------------

# 04: What is the number of SRI per hash algorithm?

saveResults("04_sri_per_hash", """
SELECT 
    algorithms,
    count(*) AS number
FROM (
    SELECT 
        url, 
        target, 
        index,
        concat_ws("+", sort_array(collect_list(substring(hash, 0, 6)))) as algorithms
    FROM (
        SELECT DISTINCT
            cc.warc,
            cc.url, 
            sri.target,
            index,
            encode(hash, 'utf-8') as hash
        FROM cc LATERAL VIEW posexplode(subresources) exploded AS index, sri LATERAL VIEW explode(split(trim(sri.integrity), ' ')) AS hash
        WHERE sri.integrity IS NOT NULL
    )
    GROUP BY warc, url, target, index
)
GROUP BY algorithms
ORDER BY number DESC 
""")

saveResults("04_sri_with_md5", """
SELECT DISTINCT
    cc.url, 
    sri.target,
    index,
    encode(hash, 'utf-8') as hash
FROM cc LATERAL VIEW posexplode(subresources) exploded AS index, sri LATERAL VIEW explode(split(trim(sri.integrity), ' ')) AS hash
WHERE sri.integrity IS NOT NULL
  AND hash LIKE "md5%"
""")

# ---------------------------

# 05: Are there invalid integrity attributes in the dataset?

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
  AND length(trim(hash)) != 94 -- sha512
  AND length(trim(hash)) != 71 -- sha384
  AND length(trim(hash)) != 70 -- sha384
  AND length(trim(hash)) != 51 -- sha256
  AND length(trim(hash)) != 50 -- sha256
""")

# ---------------------------

# 06: What is the distribution of SRI per protocol?

saveResults("06_sri_per_protocol", """
SELECT 
    if(sri.target LIKE 'https://%', 'https://', if(sri.target LIKE 'http://%', 'http://', if(sri.target LIKE '//%', '//', if(sri.target LIKE '/%', '/', '.')))) AS protocol, 
    count(*) as sri 
FROM (
    SELECT url, filter(subresources, s -> s.integrity IS NOT NULL) AS subresources 
    FROM cc 
    WHERE size(filter(subresources, s -> s.integrity IS NOT NULL)) > 0
) LATERAL VIEW explode(subresources) T AS sri
GROUP BY protocol
ORDER BY sri DESC
""")

saveResults("06_sri_per_protocol_from_host", """
SELECT 
    if(sri.target LIKE 'https://%', 'https', if(sri.target LIKE 'http://%', 'http', if(url LIKE 'https://%', 'https', 'http'))) AS protocol, 
    count(*) as sri 
FROM (
    SELECT url, filter(subresources, s -> s.integrity IS NOT NULL) AS subresources 
    FROM cc 
    WHERE size(filter(subresources, s -> s.integrity IS NOT NULL)) > 0
) LATERAL VIEW explode(subresources) T AS sri
GROUP BY protocol
ORDER BY sri DESC
""")

saveResults("06_sri_per_host_and_target_protocol", """
SELECT 
    if(url LIKE 'https://%', 'https://', if(url LIKE 'http://%', 'http://', 'other')) AS host, 
    if(sri.target LIKE 'https://%', 'https://', if(sri.target LIKE 'http://%', 'http://', if(sri.target LIKE '//%', '//', if(sri.target LIKE '/%', '/', '.')))) AS target, 
    count(*) as sri 
FROM (
    SELECT url, filter(subresources, s -> s.integrity IS NOT NULL) AS subresources 
    FROM cc 
    WHERE size(filter(subresources, s -> s.integrity IS NOT NULL)) > 0
) LATERAL VIEW explode(subresources) T AS sri
GROUP BY host, target
ORDER BY sri DESC
""")

# ---------------------------

# 07: What is the number of elements per target protocol?

select = sqlContext.sql("""
SELECT 
    url as host,
    sri.target as target
FROM cc LATERAL VIEW explode(subresources) T AS sri
WHERE sri.integrity IS NOT NULL
""")


def parse(r):
    h = urlparse(r.host)
    t = urlparse(urljoin(r.host, r.target))
    return ((h.scheme, t.scheme, 'l' if h.netloc == t.netloc else 'r'), 1)


select.rdd.map(parse).reduceByKey(add).toDF().repartition(1).write.mode('overwrite').parquet("07_elements_per_protocol")

# ---------------------------

# 08: Top-k urls and domains among sri

saveResults("08_topk_sri_url", """
SELECT 
    substr(sri.target, instr(sri.target, '//') + 2) AS library, 
    count(*) AS number,
    round(100 * count(*) / (
        SELECT count(*)
        FROM cc LATERAL VIEW explode(subresources) T AS sri
        WHERE instr(substring_index(substring_index(sri.target, '/', 3), '/', -1), '.') > 0 -- is a domain
            AND sri.integrity IS NOT NULL
    ), 2) AS percentage
FROM cc LATERAL VIEW explode(subresources) T AS sri
WHERE instr(substring_index(substring_index(sri.target, '/', 3), '/', -1), '.') > 0 -- is a domain
  AND sri.integrity IS NOT NULL
GROUP BY library
ORDER BY number DESC
""")

saveResults("08_topk_sri_domain", """
SELECT 
    substring_index(substring_index(sri.target, '/', 3), '//', -1) AS domain, 
    count(*) AS number,
    round(100 * count(*) / (
        SELECT count(*)
        FROM cc LATERAL VIEW explode(subresources) T AS sri
        WHERE instr(substring_index(substring_index(sri.target, '/', 3), '/', -1), '.') > 0 -- is a domain
        AND sri.integrity IS NOT NULL
    ), 2) AS percentage
FROM cc LATERAL VIEW explode(subresources) T AS sri
WHERE instr(substring_index(substring_index(sri.target, '/', 3), '/', -1), '.') > 0 -- is a domain
  AND sri.integrity IS NOT NULL
GROUP BY domain
ORDER BY number DESC
""")

saveResults("08_topk_sri_file", """
SELECT 
    substring_index(sri.target, '/', -1) AS file, 
    count(*) AS number,
    round(100 * count(*) / (
        SELECT count(*)
        FROM cc LATERAL VIEW explode(subresources) T AS sri
        WHERE instr(substring_index(substring_index(sri.target, '/', 3), '/', -1), '.') > 0 -- is a domain
        AND sri.integrity IS NOT NULL
    ), 2) AS percentage
FROM cc LATERAL VIEW explode(subresources) T AS sri
WHERE instr(substring_index(substring_index(sri.target, '/', 3), '/', -1), '.') > 0 -- is a domain
  AND sri.integrity IS NOT NULL
GROUP BY file
ORDER BY number DESC
""")

saveResults("08_topk_url", """
SELECT 
    substr(sri.target, instr(sri.target, '//') + 2) AS library, 
    count(*) AS number
FROM cc LATERAL VIEW explode(subresources) T AS sri
GROUP BY library
ORDER BY number DESC
""")

# ---------------------------

# 09: What is the distribution of the values for the crossorigin attribute?

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

# 010: Among the pages that contains SRI, how many of them specify the require-sri-for CSP?

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
