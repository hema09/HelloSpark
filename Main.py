from pyspark import SparkContext, SparkConf

# set up configuration and context
conf = SparkConf().setAppName("MyFirstStandAloneApp")
sc = SparkContext(conf=conf)

#read file and get RDD of strings as text_file
text_file = sc.textFile("hdfs:///tmp/shakespeare.txt")

# flatmap the text_file string, map each word as (word, 1), reduce by key. return count of each word
counts = text_file.flatMap(lambda line: line.split(" "))\
    .map(lambda word: (word, 1))\
    .reduceByKey(lambda a, b: a + b)

def printline(line):
    print(line)

counts.foreach(printline)
counts.saveAsTextFile("hdfs:///tmp/shakespeareWordCount")





