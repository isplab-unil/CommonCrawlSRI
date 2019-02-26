import re
import time

CHECKSUM1 = '95e8a199e17e995fc7cb9cb2c13ec607'
CHECKSUM2 = '95e8a199e17e995fc7cb9cb2c13ec6075f5126bd986a25ec19fb1d101559d7e2'
CHECKSUM3 = '95e8a199e17e995fc7cb9cb2c13ec6075f5126bd986a25ec19fb1d101559d7e295e8a199e17e995fc7cb9cb2c13ec6075f5126bd986a25ec19fb1d101559d7e2'

HTML = """
<html>
<head>
<title>test</title>
</head>
<body>
<p>Ut quisquam ipsum dolorem eius magnam. Quaerat velit ut quisquam quisquam. Eius amet amet labore. Porro consectetur est dolor quisquam. Sed amet quiquia sit adipisci quiquia. Dolorem modi consectetur consectetur non est dolor. Porro voluptatem sed dolorem. Etincidunt quiquia dolorem sed aliquam labore modi neque.</p>
<p>Ut quisquam ipsum dolorem eius magnam. Quaerat velit ut quisquam quisquam. Eius amet amet labore. Porro consectetur est dolor quisquam. Sed amet quiquia sit adipisci quiquia. Dolorem modi consectetur consectetur non est dolor. Porro voluptatem sed dolorem. Etincidunt quiquia dolorem sed aliquam labore modi neque.</p>
<p>Ut quisquam ipsum dolorem eius magnam. Quaerat velit ut quisquam quisquam. Eius amet amet labore. Porro consectetur est dolor quisquam. Sed amet quiquia sit adipisci quiquia. Dolorem modi consectetur consectetur non est dolor. Porro voluptatem sed dolorem. Etincidunt quiquia dolorem sed aliquam labore modi neque.</p>
<p>{0}</p>
<p>{1}</p>
<p>{2}</p>
<p>{3}</p>
<p>{4}</p>
<p>{5}</p>
</body>
</html>
""".format(CHECKSUM1, CHECKSUM2, CHECKSUM3, CHECKSUM1.upper(), CHECKSUM2.upper(), CHECKSUM3.upper())

BYTES = bytearray(HTML, "utf-8")

ITERATIONS = 1000


def benchmarkSearch(pattern):
    print("---------------")
    print("pattern: %s" % pattern)
    regex = re.compile(pattern)
    t1 = time.time()
    for i in range(0, ITERATIONS):
        result = regex.search(BYTES)
        # print(result)
    t2 = time.time()
    print("duration: %s" % (t2 - t1))


def benchmarkMatch(pattern):
    print("---------------")
    print("pattern: %s" % pattern)
    regex = re.compile(pattern)
    t1 = time.time()
    for i in range(0, ITERATIONS):
        result = regex.findall(HTML)
        # print(result)
    t2 = time.time()
    print("duration: %s" % (t2 - t1))


benchmarkSearch(b'[a-f0-9]{32}|[A-F0-9]{32}')
benchmarkSearch(b'(?:([a-f0-9]{32})|([A-F0-9]{32}))')
benchmarkMatch('[a-f0-9]{128}|[A-F0-9]{128}|[a-f0-9]{64}|[A-F0-9]{64}|[a-f0-9]{32}|[A-F0-9]{32}')
benchmarkMatch('(?:((?<!\w)[a-f0-9]{32,128}(?!\w))|((?<!\w)[A-F0-9]{32,128}(?!\w)))')
benchmarkMatch('(?:(?<!\w)[a-f0-9]{32,128}(?!\w)|(?<!\w)[A-F0-9]{32,128}(?!\w))')
