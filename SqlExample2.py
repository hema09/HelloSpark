from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext

conf = SparkConf().setAppName("SparkSQLHive")
sc = SparkContext(conf=conf)

hiveCtx = HiveContext(sc)
rows = hiveCtx.sql("select * from sample_07 LIMIT 10")
keys = rows.map(lambda row: row[0])

print keys.collect()

####


