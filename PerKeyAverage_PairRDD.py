from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("testapp")
sc = SparkContext(conf=conf)

rdd = sc.parallelize([(1, 2), (3, 4), (3, 6)])

# (1,(2,1)), (3,(4,1)), (3,(6,1))
rdd1 = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
print rdd1.collect()

rdd2 = rdd1.mapValues(lambda x: x[0]/x[1])

# (1,(2,1)), (3,(10,2))

print rdd2.collect()
# (1, 5), (3,5)

