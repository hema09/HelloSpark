from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("UnionApp")
sc = SparkContext(conf=conf)

from pyspark.sql import HiveContext

hiveCtx = HiveContext(sc)
tweets = hiveCtx.jsonFile("file:///root/tweets.json")
tweets.registerTempTable("tweets")
results = hiveCtx.sql("SELECT user.name, text FROM tweets")