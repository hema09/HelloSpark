from pyspark import SparkContext, SparkConf
import csv
import StringIO

conf = SparkConf.setAppName("CsvApp")
sc = SparkContext(conf=conf)

# read from text file and convert to csv

def loadrecord(record) :
    rec = StringIO.StringIO(record)
    reader = csv.DictReader(rec, fieldnames=["name","animalkind"])
    return reader.next()

input = sc.textFile("hdfs:///tmp/csvsample.txt").map(loadrecord)

input.collect()

# now convert to csv back and write to file

def writeRecords(records):
    output = StringIO.StringIO()
    writer = csv.DictWriter(output, fieldnames=["name","animalkind"])
    for record in records:
        writer.writerow(record)
    return [output.getvalue()]

input.mapPartitions(writeRecords).saveAsTextFile("hdfs:///tmp/csvout")


