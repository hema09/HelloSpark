from pyspark import SparkContext, SparkConf
from operator import add

conf = SparkConf().setAppName("foldApp")
sc = SparkContext(conf = conf)

fold1 = sc.parallelize([1,2,3,4,5]).fold(0, add)
print fold1

fold2 = sc.parallelize([10,20,40,30,50]).fold(0, lambda a,b: a+b)
print fold2

rdd = sc.parallelize([("ash",10), ("bina",30), ("ash", 25)])

sumbykey = rdd.foldByKey(0, lambda a,b: a+b)
print sumbykey.collect()