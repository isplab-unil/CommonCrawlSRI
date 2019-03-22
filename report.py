from pyspark.shell import sqlContext

sqlContext.read.csv('input/top-1m.csv').registerTempTable('top1m')

sqlContext.read.parquet("output/*.parquet").registerTempTable("cc")

# ---------------------------
# -------- GENERAL ----------
# ---------------------------

# What is the number of pages that have been processed?
sqlContext.sql("""
SELECT count(*) FROM cc
""").show(20, False)

sqlContext.sql("""
SELECT count(*) FROM top1m
""").show(20, False)


sqlContext.sql("""
SELECT count(*) AS number FROM cc
WHERE has_keyword_filter = true
""").show(20, False)

sqlContext.sql("""
SELECT count(*) AS number FROM cc
WHERE has_keyword_filter = true 
AND has_keyword = false
""").show(20, False)

# What is the number of warc files that have been processed?
sqlContext.sql("""
SELECT count(DISTINCT warc) AS number FROM cc
""").show(20, False)

# What is the number of pages by protocol?
sqlContext.sql("""
SELECT 
    if(uri LIKE 'https%', 'https', if(uri LIKE 'http%', 'http', 'other')) AS protocol, 
    round(100 * count(*) / (SELECT count(*) FROM cc), 2) AS percentage 
FROM cc 
GROUP BY protocol
""").show(20, False)

# ---------------------------
# ----- VERIFICATIONS -------
# ---------------------------

sqlContext.sql("""
SELECT subresources  FROM cc WHERE has_subresource LIMIT 10
""").show(20, False)

sqlContext.sql("""
SELECT checksums  FROM cc WHERE has_checksum LIMIT 10
""").show(20, False)

sqlContext.sql("""
SELECT keywords  FROM cc WHERE has_keyword LIMIT 10
""").show(20, False)

# ---------------------------
# ---------- SRI ------------
# ---------------------------

# What is the percentage of pages that includes at least one tag with the integrity attribute?
sqlContext.sql("""
SELECT 
    round(100 * count(*) / (SELECT count(*) AS uris FROM cc), 2) AS percentage 
FROM cc 
WHERE has_subresource = true 
  AND size(filter(subresources, s -> s.integrity IS NOT NULL)) > 0
""").show(20, False)

# What is the percentage of pages that includes at least one script with the integrity attribute?
sqlContext.sql("""
SELECT 
    round(100 * count(*) / (SELECT count(*) AS uris FROM cc), 2) AS percentage 
FROM cc 
WHERE has_subresource = true 
  AND size(filter(subresources, s -> s.name == 'script' AND s.integrity IS NOT NULL)) > 0
""").show(20, False)

# What is the percentage of pages that includes at least one link with the integrity attribute?
sqlContext.sql("""
SELECT 
    round(100 * count(*) / (SELECT count(*) AS uris FROM cc), 2) AS percentage 
FROM cc 
WHERE has_subresource = true 
  AND size(filter(subresources, s -> s.name == 'link' AND s.integrity IS NOT NULL)) > 0
""").show(20, False)

# ---------------------------

# What is the distribution of pages by number of SRI tags?
sqlContext.sql("""
SELECT 
    size(filter(subresources, s -> s.integrity IS NOT NULL)) AS tags, 
    count(*) AS number 
FROM cc 
WHERE has_subresource = true 
  AND size(filter(subresources, s -> s.integrity IS NOT NULL)) > 0 
GROUP BY tags 
ORDER BY tags ASC
""").foreach(lambda r: print(str(r.tags) + "\t" + str(r.number)))

# What is the distribution of pages by scripts?
sqlContext.sql("""
SELECT 
    sri.target AS script, 
    count(*) AS number 
FROM (
    SELECT filter(subresources, s -> s.name == 'script' AND s.target IS NOT NULL AND s.integrity IS NOT NULL) AS subresources 
    FROM cc 
    WHERE has_subresource = true 
      AND size(filter(subresources, s -> s.name == 'script' AND s.target IS NOT NULL AND s.integrity IS NOT NULL)) > 0
) LATERAL VIEW explode(subresources) T AS sri 
GROUP BY script 
ORDER BY number DESC
""").show(20, False)

# What is the distribution of pages by links?
sqlContext.sql("""
SELECT 
    sri.target AS link, 
    count(*) AS number 
FROM (
    SELECT filter(subresources, s -> s.name == 'link' AND s.target IS NOT NULL AND s.integrity IS NOT NULL) AS subresources 
    FROM cc 
    WHERE has_subresource = true 
      AND size(filter(subresources, s -> s.name == 'link' AND s.target IS NOT NULL AND s.integrity IS NOT NULL)) > 0
) LATERAL VIEW explode(subresources) T AS sri 
GROUP BY link 
ORDER BY number DESC
""").show(20, False)

# ---------------------------

# What is the distribution of tags with integrity attribute per protocol?
sqlContext.sql("""
SELECT if(sri.target LIKE 'https%', 'https', if(sri.target LIKE 'http%', 'http', if(uri LIKE 'https%', 'https', 'http'))) AS protocol, count(*) as tag_targets FROM (
    SELECT uri, filter(subresources, s -> s.integrity IS NOT NULL) AS subresources 
    FROM cc 
    WHERE has_subresource = true 
      AND size(filter(subresources, s -> s.integrity IS NOT NULL)) > 0
) LATERAL VIEW explode(subresources) T AS sri
GROUP BY protocol
ORDER BY tag_targets DESC
""").show(20, False)

# What is the distribution of scripts with integrity attribute per protocol?
sqlContext.sql("""
SELECT if(sri.target LIKE 'https%', 'https', if(sri.target LIKE 'http%', 'http', if(uri LIKE 'https%', 'https', 'http'))) AS protocol, count(*) as script_targets FROM (
    SELECT uri, filter(subresources, s -> s.name == 'script' AND s.integrity IS NOT NULL) AS subresources 
    FROM cc 
    WHERE has_subresource = true 
      AND size(filter(subresources, s -> s.name == 'script' AND s.integrity IS NOT NULL)) > 0
) LATERAL VIEW explode(subresources) T AS sri
GROUP BY protocol
ORDER BY script_targets DESC
""").show(20, False)

# What is the distribution of links with integrity attribute per protocol?
sqlContext.sql("""
SELECT if(sri.target LIKE 'https%', 'https', if(sri.target LIKE 'http%', 'http', if(uri LIKE 'https%', 'https', 'http'))) AS protocol, count(*) as link_targets FROM (
    SELECT uri, filter(subresources, s -> s.name == 'link' AND s.integrity IS NOT NULL) AS subresources 
    FROM cc 
    WHERE has_subresource = true 
      AND size(filter(subresources, s -> s.name == 'link' AND s.integrity IS NOT NULL)) > 0
) LATERAL VIEW explode(subresources) T AS sri
GROUP BY protocol
ORDER BY link_targets DESC
""").show(20, False)

# ---------------------------

# What is the percentage of tags that specifies the integrity attribute on a page that includes at least one tag with the integrity attribute?
sqlContext.sql("""
SELECT 
    round(100 * sum(size(filter(subresources, s -> s.integrity IS NOT NULL))) / sum(size(subresources)), 2) AS percentage
FROM cc 
WHERE has_subresource = true 
  AND size(filter(subresources, s -> s.integrity IS NOT NULL)) > 0
""").show(20, False)

# What is the percentage of scripts that specifies the integrity attribute on a page that includes at least one script with the integrity attribute?
sqlContext.sql("""
SELECT 
    round(100 * sum(size(filter(subresources, s -> s.name == 'script' AND s.integrity IS NOT NULL))) / sum(size(filter(subresources, s -> s.name == 'script'))), 2) AS percentage
FROM cc 
WHERE has_subresource = true 
  AND size(filter(subresources, s -> s.name == 'script' AND s.integrity IS NOT NULL)) > 0
""").show(20, False)

# What is the percentage of links that specifies the integrity attribute on a page that includes at least one link with the integrity attribute?
sqlContext.sql("""
SELECT 
    round(100 * sum(size(filter(subresources, s -> s.name == 'link' AND s.integrity IS NOT NULL))) / sum(size(filter(subresources, s -> s.name == 'link'))), 2) AS percentage
FROM cc 
WHERE has_subresource = true 
  AND size(filter(subresources, s -> s.name == 'link' AND s.integrity IS NOT NULL)) > 0
""").show(20, False)

# ---------------------------

# What is the number of pages by hashing algorithms?
sqlContext.sql("""
SELECT 
    substr(sri.integrity, 0, 6) as hash, 
    count(*)
FROM cc LATERAL VIEW explode(subresources) T AS sri
WHERE has_subresource = true 
  AND sri.integrity IS NOT NULL
GROUP BY hash
""").show(20, False)

# ---------------------------

# What are the most popular subresources included in pages
sqlContext.sql("""
SELECT 
    substr(sri.target, instr(sri.target, '//') + 2) AS library, 
    count(*) AS number
FROM cc LATERAL VIEW explode(subresources) T AS sri
WHERE sri.target IS NOT NULL AND sri.integrity IS NOT NULL
GROUP BY library
ORDER BY number DESC
""").show(20, False)

# ---------------------------
# ------- CHECKSUMS ---------
# ---------------------------

sqlContext.sql("""
SELECT 
    round(100 * count(*) / (SELECT count(*) FROM cc), 2) AS percentage 
FROM cc 
WHERE uri LIKE '%download%' 
""").show(20, False)

sqlContext.sql("""
SELECT 
    round(100 * count(*) / (SELECT count(*) FROM cc), 2) AS percentage 
FROM cc 
WHERE has_checksum
""").show(20, False)

sqlContext.sql("""
SELECT 
    round(100 * count(*) / (SELECT count(*) FROM cc), 4) AS percentage 
FROM cc 
WHERE has_keyword AND has_checksum
""").show(20, False)

sqlContext.sql("""
SELECT 
    round(100 * count(*) / (SELECT count(*) FROM cc), 4) AS percentage 
FROM cc 
WHERE  has_keyword AND has_checksum AND uri LIKE '%download%'  
""").show(20, False)

sqlContext.sql("""
SELECT COUNT(*)
FROM cc JOIN top1m ON ( substring_index(substring_index(uri, '/', 3), '/', -1) = _c1)
WHERE  has_keyword AND has_checksum AND uri LIKE '%download%'  
""").show(1000, False)

# ---------------------------
# -------- HEADERS ----------
# ---------------------------

sqlContext.sql("""
SELECT csp, count(*) as number
FROM cc
GROUP BY csp
ORDER BY number DESC
""").show(20, False)

sqlContext.sql("""
SELECT cors, count(*) as number
FROM cc
GROUP BY cors
ORDER BY number DESC
""").show(20, False)

sqlContext.sql("""
SELECT COUNT(*)
FROM cc JOIN top1m ON ( substring_index(substring_index(uri, '/', 3), '/', -1) = _c1)
WHERE has_checksum = true AND has_keyword = true
""").show(1000, False)

# ---------------------------
# --------- TOP1M -----------
# ---------------------------


from selenium import webdriver

def render(id, url, checksums):
    driver = webdriver.Chrome()
    driver.get(url)
    driver.implicitly_wait(10)
    for checksum in checksums:
        try:
            element = driver.find_element_by_xpath("//*[contains(text(),'" + checksum + "')]")
            driver.execute_script("arguments[0].setAttribute('style', arguments[1]);", element, "border: 3px solid red;")
        except Exception as e:
            print(str(e))
    body = driver.find_element_by_tag_name('body')
    body_png = body.screenshot_as_png
    file = str(id) + ".png"
    with open(file, "wb") as file:
        file.write(body_png)
    driver.quit()

render('776426eeb4f0752fa3f9750ddaf29f1364bccc84',
       'https://www.phpbb.com/downloads/?sid=c0325e3272376031d283c19c3d8da7fb',
       ['7706292fe4b2f7eb988a7b688c29cbe9c8e86f7f51c759c5aab9fc176e695f44'])

render('776426eeb4f0752fa3f9750ddaf29f1364bccc84',
       'https://www.phpbb.com/downloads/?sid=c0325e3272376031d283c19c3d8da7fb',
       ['4c50f8657a6f19e73468bac563c1804e112c54c1f700d24803cacc22d080d08b'])


render('776426eeb4f0752fa3f9750ddaf29f1364bccc84',
       'https://www.phpbb.com/downloads/?sid=c0325e3272376031d283c19c3d8da7fb',
       ['7706292fe4b2f7eb988a7b688c29cbe9c8e86f7f51c759c5aab9fc176e695f44',
        '4c50f8657a6f19e73468bac563c1804e112c54c1f700d24803cacc22d080d08b'])

sqlContext.sql("""
SELECT sha1(uri) as id, uri, checksums
FROM cc JOIN top1m ON ( substring_index(substring_index(uri, '/', 3), '/', -1) = _c1)
WHERE has_checksum = true AND has_keyword = true AND uri LIKE '%download%'
ORDER BY CAST(_c0 as INT)
LIMIT 10
""").show(3, False)
#.foreach(lambda r: render(r.id, r.uri, r.checksums))



sqlContext.sql("""
SELECT _c0, uri
FROM cc JOIN top1m ON ( substring_index(substring_index(uri, '/', 3), '/', -1) = _c1)
WHERE has_checksum = true AND has_keyword = true
ORDER BY CAST(_c0 as INT)
""").show(20, False)

sqlContext.sql("""
SELECT sha1(uri) as id, uri, checksums
FROM cc JOIN top1m ON ( substring_index(substring_index(uri, '/', 3), '/', -1) = _c1)
WHERE has_checksum = true AND has_keyword = true AND uri LIKE '%download%'
ORDER BY CAST(_c0 as INT)
""").foreach(lambda r: render(r.id, r.uri, r.checksums))


sqlContext.sql("""
SELECT COUNT(*)
FROM cc JOIN top1m ON ( substring_index(substring_index(uri, '/', 3), '/', -1) = _c1)
WHERE has_checksum = true AND has_keyword = true
""").show(20, False)

sqlContext.sql("""
SELECT count(*)
FROM cc 
""").show(20, False)
