from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("AccumulatorApp")
sc = SparkContext(conf=conf)

file = sc.textFile("hdfs:///tmp/jsonsample.txt")
blankLines = sc.accumulator(0)

def extractCallSigns(line):
    global blankLines
    if(line == ""):
        blankLines += 1
    return line.split(" ")

callSign = file.flatMap(extractCallSigns)

print "blankLines", blankLines
