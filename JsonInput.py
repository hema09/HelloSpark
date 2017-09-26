from pyspark import SparkContext, SparkConf
import json

conf = SparkConf().setAppName("filterApp")
sc = SparkContext(conf=conf)

# copy jsonsample.txt form this folder to hdfs /tmp folder

#read with json.loads
# each line in jsonsample is json record
input = sc.textFile("hdfs:///tmp/jsonsample.txt")
input = input.map(lambda x : json.loads(x))

print input.collect()

#write with json.dumps
input.map(lambda x: json.dumps(x)).saveAsTextFile("hdfs:///tmp/jsonsampleoutput")


ip = sc.textFile("hdfs:///tmp/jsonsample.txt")
input = input.map(lambda x: json.load(x))

input.map(lambda x: json.dumps(x)).saveAsTextFile("hdfs:///tmp/jsonoutput")

