#!/usr/bin/env python

import re
from glob import iglob
import datetime
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.shell import sqlContext
import csv
from collections import defaultdict
import itertools
from itertools import islice

# Import the sample of common crawl

sqlContext.read.parquet("../data/res/2019-09/queries/top_resources_remote/").registerTempTable("resources")

def sql(sql):
    return sqlContext.sql(sql).toPandas()

all = sql("""
SELECT sum(number) as total
FROM resources
""")['total'][0]

subset = sql("""
SELECT sum(number) as total
FROM resources
WHERE parse_url(CONCAT('//', path), 'HOST') IN (
    SELECT parse_url(CONCAT('//', path), 'HOST') as host
    FROM resources 
    GROUP BY host
    ORDER BY sum(number) DESC
    LIMIT 100
)
""")['total'][0]

data = sql("""
SELECT *
FROM resources
WHERE parse_url(CONCAT('//', path), 'HOST') IN (
    SELECT parse_url(CONCAT('//', path), 'HOST') as host
    FROM resources 
    GROUP BY host
    ORDER BY sum(number) DESC
    LIMIT 100
)
""")

# Categorize the resources based on the most popular domains 
# and on the URL structure.

total = 0
groups = defaultdict(int)

for index, row in data.iterrows():
    try:    
        number = int(row['number'])
        total += number           
        path = row['path']

        if "s.pubmine.com" in path:
            groups['pubmine'] += number

        elif "www.linkwithin.com" in path:
            groups['linkwithin'] += number

        elif "wcs.naver.net" in path:
            groups['naver'] += number

        elif "secure.gravatar.com" in path:
            groups['gravatar'] += number

        elif "cdn.pool.st-hatena.com" in path:
            groups['hatena'] += number

        elif "ssl.google-analytics.com" in path:
            groups['google analytics'] += number

        elif "assets.pinterest.com" in path:
            groups['pinterest'] += number

        elif "b.st-hatena.com" in path:
            groups['hatena'] += number

        elif "platform.linkedin.com" in path:
            groups['linkedin'] += number

        elif "blogroll.livedoor.net" in path:
            groups['livedoor'] += number

        elif "www.statcounter.com" in path:
            groups['statcounter'] += number

        elif "cdn.onesignal.com" in path:
            groups['onesignal'] += number

        elif "ads.exosrv.com" in path:
            groups['exosrv'] += number

        elif "www.googleadservices.com" in path:
            groups['google ad services'] += number

        elif "www.googletagservices.com" in path:
            groups['google tag services'] += number

        elif "widgets.outbrain.com" in path:
            groups['outbrain'] += number

        elif "static.criteo.net" in path:
            groups['criteo'] += number

        elif "mc.yandex.ru" in path:
            groups['yandex'] += number

        elif "getmylanding.site" in path:
            groups['getmylanding'] += number

        elif "www.google-analytics.com" in path:
            groups['google analytics'] += number

        elif "ws.sharethis.com" in path:
            groups['sharethis'] += number

        elif "cdn-images.mailchimp.com" in path:
            groups['mailchimp'] += number

        elif "libs.pixfs.net" in path:
            groups['pixfs'] += number

        elif "partner.googleadservices.com" in path:
            groups['google ad services'] += number

        elif "dsms0mj1bbhn4.cloudfront.net" in path:
            groups['shareaholic'] += number

        elif "w.sharethis.com" in path:
            groups['sharethis'] += number

        elif "assets.jimstatic.com" in path:
            groups['jimstatic'] += number

        elif "radscriptcdn.sharpschool.com" in path:
            groups['sharpschool'] += number

        elif "apis.google.com" in path:
            groups['google apis'] += number

        elif "parts.blog.livedoor.jp" in path:
            groups['livedoor'] += number

        elif "s0.wp.com" in path:
            groups['wordpress'] += number

        elif "platform.instagram.com" in path:
            groups['instagram'] += number

        elif "d133rs42u5tbg.cloudfront.net" in path:
            groups['d133rs42u5tbg.cloudfront.net'] += number

        elif "static.vecteezy.com" in path:
            groups['vecteezy'] += number

        elif "static.addtoany.com" in path:
            groups['addtoany'] += number

        elif "bt-wpstatic.freetls.fastly.net" in path:
            groups['fastly'] += number

        elif "static.blogg.se" in path:
            groups['blogg'] += number

        elif "cdn.schoolloop.com" in path:
            groups['schoolloop'] += number

        elif "image.excite.co.jp" in path:
            groups['excite'] += number

        elif "platform.twitter.com" in path:
            groups['twitter'] += number

        elif "i.plug.it" in path:
            groups['plugit'] += number

        elif "static.fc2.com" in path:
            groups['fc2'] += number

        elif "static.websimages.com" in path:
            groups['websimages'] += number

        elif "s.pinimg.com" in path:
            groups['pinimg'] += number

        elif "stackpath.bootstrapcdn.com" in path:
            group = path.split('/')[1]
            groups[group] += number

        elif "ssl.c.photoshelter.com" in path:
            groups['photoshelter'] += number

        elif "s2.wp.com" in path:
            groups['wordpress'] += number

        elif "pagead2.googlesyndication.com" in path:
            groups['google syndication'] += number

        elif "cdn.ampproject.org" in path:
            groups['ampproject'] += number

        elif "static.zdassets.com" in path:
            groups['zdassets'] += number

        elif "illiweb.com" in path:
            groups['illiweb'] += number

        elif "s7.addthis.com" in path:
            groups['addthis'] += number

        elif "d1ulmmr4d4i8j4.cloudfront.net" in path:
            groups['d1ulmmr4d4i8j4.cloudfront.net'] += number

        elif "connect.facebook.net" in path:
            groups['facebook'] += number

        elif "st.tistatic.com" in path:
            groups['tistatic'] += number

        elif "js.skyscnr.com" in path:
            groups['skyscnr'] += number

        elif "stats.wp.com" in path:
            groups['wordpress'] += number

        elif "netdna.bootstrapcdn.com" in path:
            group = path.split('/')[1]
            groups[group] += number

        elif "www.google.com" in path:
            group = path.split('/')[1]
            groups['google ' + group] += number

        elif "www.blogblog.com" in path:
            groups['blogblog'] += number

        elif "static.squarespace.com" in path:
            groups['squarespace'] += number

        elif "www.blogger.com" in path:
            groups['blogger'] += number

        elif "yandex.st" in path:
            groups['yandex'] += number

        elif "js-sec.indexww.com" in path:
            groups['indexww'] += number

        elif "maxcdn.bootstrapcdn.com" in path:
            group = path.split('/')[1]
            groups[group] += number

        elif "t1.daumcdn.net" in path:
            groups['daumcdn'] += number

        elif "static.parastorage.com" in path:
            groups['wix'] += number

        elif "a0.muscache.com" in path:
            groups['airbnb'] += number

        elif "strato-editor.com" in path:
            groups['strato'] += number

        elif "style.bosscdn.com" in path:
            groups['bosscdn'] += number

        elif "yastatic.net" in path:
            group = path.split('/')[1]
            groups[group] += number

        elif "code.jquery.com" in path:
            groups['jquery'] += number

        elif "s.yimg.com" in path:
            groups['yimg'] += number

        elif "ajax.aspnetcdn.com" in path:
            group = path.split('/')[1]
            groups[group] += number

        elif "s.yimg.jp" in path:
            groups['yimg'] += number
        
        elif "www.gstatic.com" in path:
            groups['google static'] += number

        elif "cdn.datatables.net" in path:
            groups['datatables'] += number

        elif "unpkg.com" in path:
            group = path.split('/')[1].split('@')[0]
            groups[group] += number

        elif "c0.wp.com" in path:
            groups['wordpress'] += number

        elif "d2wldr9tsuuj1b.cloudfront.net" in path:
            groups['d2wldr9tsuuj1b.cloudfront.net'] += number

        elif "static.tacdn.com" in path:
            groups['tacdn'] += number

        elif "www.f-cdn.com" in path:
            groups['f-cdn'] += number

        elif "ajax.googleapis.com" in path:
            if (len(path.split('/')) >= 4):
                group = path.split('/')[3]
                groups[group] += number
            else:
                groups['google ajax cdn'] += number

        elif "cdn.shopify.com" in path:
            groups['shopify'] += number

        elif "storage.googleapis.com" in path:
            groups['google storage'] += number

        elif "cdn.optimizely.com" in path:
            groups['optimizely'] += number

        elif "assets.adobedtm.com" in path:
            groups['adobe'] += number

        elif "cdn.jsdelivr.net/npm" in path:
            group = path.split('/')[2].split('@')[0]
            groups[group] += number

        elif "cdn.jsdelivr.net/webjars" in path:
            group = path.split('/')[2]
            groups[group] += number

        elif "cdn.jsdelivr.net/gh" in path:
            group = path.split('/')[2]
            groups[group] += number

        elif "cdn.jsdelivr.net/wp" in path:
            groups['wordpress'] += number

        elif "cdn.jsdelivr.net" in path:
            group = path.split('/')[1]
            groups[group] += number

        elif "cdn10.bigcommerce.com" in path:
            groups['bigcommerce'] += number

        elif "cdn.sendpulse.com" in path:
            groups['sendpulse'] += number

        elif "static1.squarespace.com" in path:
            groups['squarespace'] += number

        elif "cdn9.bigcommerce.com" in path:
            groups['bigcommerce'] += number

        elif "s3.amazonaws.com" in path:
            groups['amazon s3'] += number

        elif "js.users.51.la" in path:
            groups['lao national internet center'] += number
        
        elif "cdnjs.cloudflare.com" in path:
            group = path.split('/')[3]
            groups[group] += number

        elif "use.fontawesome.com" in path:
            groups['fontawesome'] += number

        elif "cdn11.bigcommerce.com" in path:
            groups['bigcommerce'] += number

        elif "cdn.smugmug.com" in path:
            groups['smugmug'] += number

        elif "hb.wpmucdn.com" in path:
            groups['wpmu cdn'] += number

        elif "use.typekit.net" in path:
            groups['adobe fonts'] += number

        else:
            print(row)
            break
    except:
        print("error: " + row)

resources = pd.DataFrame(sorted(groups.items(), key=lambda x: x[1], reverse=True), columns = ['name', 'number'])
resources['percentage'] = resources['number'] / all

# Write the Latex variables 

with open("output/top_resources.tex", 'w') as out:
    out.write("\def\\CCSampleN{\\num{" + str(all) + "}\\xspace}\n")
    out.write("\def\\CCSampleSubsetN{\\num{" + str(subset) + "}\\xspace}\n")
    out.write("\def\\CCSampleSubsetP{\\num{" + ("%0.2f" % (subset / all * 100)) + "}\%\\xspace}\n")

    for row in resources[:20].iterrows():
        out.write("\def\\CCSampleSubset" + row[1]['name'].title().replace(" ", "").replace("-", "") + "P{\\num{" + ("%0.2f" % (row[1]['percentage'] * 100)) + "}\%\\xspace}\n")

