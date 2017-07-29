from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("squareApp")
sc = SparkContext(conf = conf)

nums = sc.parallelize([1,2,3,4])

squared = nums.map(lambda x: x * x).collect()

for num in squared:
    print "%i " %(num)