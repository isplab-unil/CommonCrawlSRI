from pyspark.shell import sqlContext

# Load the parquet files
sqlContext.read.parquet("../download/*.parquet").registerTempTable("cc")
sqlContext.read.csv('../top1m/top-1m-cisco.csv').registerTempTable('top1m')


def sql(sql):
    sqlContext \
        .sql(sql) \
        .show(20, False)


def csv(file, sql):
    sqlContext \
        .sql(sql) \
        .repartition(1) \
        .write.format("csv") \
        .option("header", "true") \
        .save(file)


# ---------------------------
# --------- QUERIES ---------
# ---------------------------

csv("00_page_count.csv", """
SELECT count(*) FROM cc
""")

sql("00_checksum_count.csv", """
SELECT 
    count(*)
FROM cc LATERAL VIEW explode(checksums) T AS checksum
WHERE has_checksum 
""")

csv("00_has_keyword_count.csv", """
SELECT count(*) FROM cc WHERE has_keyword
""")

csv("00_has_keyword_and_checksum_count.csv", """
SELECT count(*) FROM cc WHERE has_checksum
""")

csv("01_pages_per_checksums.csv", """
SELECT 
    size(checksums) as number, 
    count(*) as count
FROM cc 
WHERE has_checksum
GROUP BY number
ORDER BY number
""")

sql("""
SELECT 
    length(checksum),
    count(*)
FROM cc LATERAL VIEW explode(checksums) T AS checksum
WHERE has_checksum 
GROUP BY length(checksum)
""")


# -

sql("""
SELECT 
    length(checksum),
    count(*)
FROM cc LATERAL VIEW explode(checksums) T AS checksum
WHERE has_checksum AND (
    lower(content) LIKE '%md5%' OR
    lower(content) LIKE '%sha1%' OR lower(content) LIKE '%sha-1%' OR
    lower(content) LIKE '%sha224%' OR lower(content) LIKE '%sha-224%' OR
    lower(content) LIKE '%sha256%' OR lower(content) LIKE '%sha-256%' OR
    lower(content) LIKE '%sha384%' OR lower(content) LIKE '%sha-384%' OR
    lower(content) LIKE '%sha512%' OR lower(content) LIKE '%sha-512%' 
)
GROUP BY length(checksum)
""")

sql("""
SELECT url, checksum
FROM cc LATERAL VIEW explode(checksums) T AS checksum
WHERE has_checksum AND length(checksum) = 32 AND lower(content) LIKE '%md5%'
""")

sql("""
SELECT url, checksum
FROM cc LATERAL VIEW explode(checksums) T AS checksum
WHERE has_checksum AND length(checksum) = 40 AND (lower(content) LIKE '%sha1%' OR lower(content) LIKE '%sha-1%')
""")

sql("""
SELECT url, checksum
FROM cc LATERAL VIEW explode(checksums) T AS checksum
WHERE has_checksum AND length(checksum) = 56 AND (lower(content) LIKE '%sha224%' OR lower(content) LIKE '%sha-224%')
""")

sql("""
SELECT url, checksum
FROM cc LATERAL VIEW explode(checksums) T AS checksum
WHERE has_checksum AND length(checksum) = 64 AND (lower(content) LIKE '%sha256%' OR lower(content) LIKE '%sha-256%')
""")

sql("""
SELECT url, checksum
FROM cc LATERAL VIEW explode(checksums) T AS checksum
WHERE has_checksum AND length(checksum) = 96 AND (lower(content) LIKE '%sha384%' OR lower(content) LIKE '%sha-384%')
""")

sql("""
SELECT url, checksum
FROM cc LATERAL VIEW explode(checksums) T AS checksum
WHERE has_checksum AND length(checksum) = 128 AND (lower(content) LIKE '%sha512%' OR lower(content) LIKE '%sha-512%')
""")



csv("top-download-pages.csv", """
SELECT 
    _c0 as rank, _c1 as host, url as url
FROM cc
JOIN top1m ON (substring_index(substring_index(url, '/', 3), '/', -1) = _c1)
WHERE has_checksum AND (
    lower(content) LIKE '%md5%' OR
    lower(content) LIKE '%sha1%' OR lower(content) LIKE '%sha-1%' OR
    lower(content) LIKE '%sha224%' OR lower(content) LIKE '%sha-224%' OR
    lower(content) LIKE '%sha256%' OR lower(content) LIKE '%sha-256%' OR
    lower(content) LIKE '%sha384%' OR lower(content) LIKE '%sha-384%' OR
    lower(content) LIKE '%sha512%' OR lower(content) LIKE '%sha-512%' 
)
""")

csv("top-download-urls.csv", """
SELECT 
    _c0 as rank, _c1 as host, url as url
FROM cc
JOIN top1m ON (substring_index(substring_index(url, '/', 3), '/', -1) = _c1)
WHERE url LIKE '%download%'
ORDER BY _c0 ASC
""")




# ---------------------------
# ------- QUERIES ---------
# ---------------------------

# Percentage of web pages containing 'download' in their url
sqlContext.sql("""
SELECT 
    round(100 * count(*) / (SELECT count(*) FROM cc), 2) AS percentage 
FROM cc 
WHERE url LIKE '%download%' 
""").show(20, False)

# Percentage of web pages containing keywords in their content
sqlContext.sql("""
SELECT 
    round(100 * count(*) / (SELECT count(*) FROM cc), 2) AS percentage 
FROM cc 
WHERE has_keyword
""").show(20, False)

# Percentage of web pages containing checksums in their content
sqlContext.sql("""
SELECT 
    round(100 * count(*) / (SELECT count(*) FROM cc), 2) AS percentage 
FROM cc 
WHERE has_checksum
""").show(20, False)


sqlContext.sql("""
SELECT COUNT(*)
FROM cc JOIN top1m ON ( substring_index(substring_index(url, '/', 3), '/', -1) = _c1)
WHERE  has_keyword AND has_checksum AND url LIKE '%download%'  
""").show(20, False)


sqlContext.sql("""
SELECT COUNT(*)
FROM cc JOIN top1m ON ( substring_index(substring_index(url, '/', 3), '/', -1) = _c1)
WHERE has_checksum = true AND has_keyword = true
""").show(20, False)

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

#.foreach(lambda r: render(r.id, r.url, r.checksums))

sqlContext.sql("""
SELECT sha1(url) as id, url, checksums
FROM cc JOIN top1m ON ( substring_index(substring_index(url, '/', 3), '/', -1) = _c1)
WHERE has_checksum = true AND has_keyword = true AND url LIKE '%download%'
ORDER BY CAST(_c0 as INT)
LIMIT 10
""").show(20, False)


sqlContext.sql("""
SELECT sha1(url) as id, _c0, url
FROM cc JOIN top1m ON ( substring_index(substring_index(url, '/', 3), '/', -1) = _c1)
WHERE has_checksum = true AND has_keyword = true
ORDER BY CAST(_c0 as INT)
""").show(20, False)

sqlContext.sql("""
SELECT sha1(url) as id, url, checksums
FROM cc JOIN top1m ON ( substring_index(substring_index(url, '/', 3), '/', -1) = _c1)
WHERE has_checksum = true AND has_keyword = true AND url LIKE '%download%'
ORDER BY CAST(_c0 as INT)
""").show(20, False)


sqlContext.sql("""
SELECT COUNT(*)
FROM cc JOIN top1m ON ( substring_index(substring_index(url, '/', 3), '/', -1) = _c1)
WHERE has_checksum = true AND has_keyword = true
""").show(20, False)


sql("""
SELECT * FROM top1m
""")

