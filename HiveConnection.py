from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("UnionApp")
sc = SparkContext(conf = conf)

from pyspark.sql import HiveContext
hiveCtx = HiveContext(sc)
rows = hiveCtx.sql("SELECT name, age FROM users")
firstRow = rows.first()
print firstRow.name