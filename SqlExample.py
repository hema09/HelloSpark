from pyspark import SparkContext, SparkConf
from pyspark import HiveContext, Row

import json
import sys

conf = SparkConf().setAppName("SparkSQLTwitter")
sc = SparkContext(conf=conf)

hiveCtx = HiveContext(sc)
inputFile = sys.argv[1]
print "inputfile = ",  inputFile
input = hiveCtx.jsonFile(inputFile)
input.registerTempTable("tweets")

tweetFields = hiveCtx.sql("describe tweets")
print "tweetfields = ", tweetFields

topTweets = hiveCtx.sql("SELECT text, retweetCount FROM tweets ORDER BY retweetCount LIMIT 10")
print topTweets.collect()

topTweetText = topTweets.map(lambda row: row.text)
print topTweetText.collect()


######

happyPeopleRDD = sc.parallelize([Row(name="holden", favouriteBeverage="coffee")])
happyPeopleSchemaRDD = hiveCtx.inferSchema(happyPeopleRDD)
happyPeopleSchemaRDD.registerTempTable("happy_people")

print happyPeopleSchemaRDD.collect()






