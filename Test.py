from pyspark import SparkContext, SparkConf
import re
import bisect

conf = SparkConf().setAppName("testapp")
sc = SparkContext(conf=conf)

file = sc.textFile("hdfs:///tmp/callsigns")
blankLines = sc.accumulator(0)
validSignCount = sc.accumulator(0)
invalidSignCount = sc.accumulator(0)

def countBlank(line):
    global blankLines
    if(line == ""):
        blankLines += 1
    return line.split(" ")

def filterCallSigns(sign):
    global validSignCount, invalidSignCount
    if re.match(r"\A\d?[a-zA-Z]{1,2}\d{1,4}[a-zA-Z]{1,3}\Z", sign):
        validSignCount += 1
        return True
    else:
        invalidSignCount += 1
        return False

callSigns = file.flatMap(countBlank)
filteredSigns = callSigns.filter(filterCallSigns)
contactCount = filteredSigns.map(lambda sign: (sign,1)).reduceByKey(lambda a, b: a + b)

contactCount.count()
if invalidSignCount < 0.1 * validSignCount:
    contactCount.saveAsTextFile("contactCount.txt")
else:
    print "too many errors %d in %d" % (invalidSignCount.value, validSignCount.value)

def loadCallSignTable():
    f = open("/root/call_sign_table_sorted", "r")
    return f.readlines()

signPrefixes = sc.broadcast(loadCallSignTable())

def processSignCount(signCount, signPrefixes):
    code = signCount[0]
    pos = bisect.bisect_left(signPrefixes.value, code)
    country = signPrefixes[pos].split(",")[1]
    return (country, signCount[1])

countryContactCount = contactCount.map(lambda signCount: processSignCount(signCount, signPrefixes))\
                        .reduceByKey(lambda x,y: x + y)

countryContactCount.saveAsTextFile("hdfs:///countryContactCount.txt")



