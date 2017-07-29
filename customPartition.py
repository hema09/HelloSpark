import urlparse
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("CustomPartitionApp")
sc = SparkContext(conf = conf)

def hash_domain(url):
    return hash(urlparse.urlparse(url).netloc)

input = sc.parallelize(["http://google.com", "http://baidu.com", "http://mapr.com"])

rdd = input.partitionBy(3, hash_domain)

rdd.collect()
