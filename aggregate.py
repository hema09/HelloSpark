from pyspark import SparkContext, SparkConf
from operator import add

conf = SparkConf().setAppName("aggregate")
sc = SparkContext(conf = conf)

rdd1 = sc.parallelize([1,2,3,4,5,6,7,8,9,10])

sumCount = rdd1.aggregate((0,0),
                         (lambda acc, value : (acc[0] + value, acc[1] + 1)),        # seqOp
                         (lambda acc1, acc2 : (acc1[0] + acc2[0], acc1[1] + acc2[1])))  #combineOp

print (sumCount[0]/float(sumCount[1]))

