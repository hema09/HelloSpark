from pyspark import SparkContext, SparkConf
import re
import bisect

conf = SparkConf().setAppName("ErrorCount")
sc = SparkContext(conf=conf)

validSign = sc.accumulator(0)
invalidSign = sc.accumulator(0)
blankLine = sc.accumulator(0)

file = sc.textFile("hdfs:///tmp/callsigns")


def extractCallSign(line):
    global blankLine
    if(line == "") :
        blankLine += 1
    return  line.split(" ")

callSigns = file.flatMap(extractCallSign)

def validCallSign(sign):
    global validSign, invalidSign
    if re.match(r"\A\d?[a-zA-Z]{1,2}\d{1,4}[a-zA-Z]{1,3}\Z", sign):
        validSign += 1
        return True
    else:
        invalidSign += 1
        return False

validSigns = callSigns.filter(validCallSign)

# contactCount will now contain map of <sign, count>
contactCount = validSigns.map(lambda sign: (sign, 1)).reduceByKey(lambda (x,y) : x + y)

contactCount.count() # force evaluation

if invalidSign.value < 0.1 * validSign.value:
    contactCount.saveAsTextFile("hdfs:///tmp/contactCount")
else:
    print ("Too many errors %d in %d" %
           (invalidSign.value, validSign.value))

#contactcount is now like {sign,count}

#sorted list so that we can use bisect on it (bisect uses binary search)
def loadCallSignTable():
    f = open("/root/call_sign_table_sorted", "r")
    return f.readlines()

# signPrefixes = sorted list of {sign, country name}
signPrefixes = sc.broadcast(loadCallSignTable())

def lookupCountry(sign, prefixes):
    pos = bisect.bisect_left(prefixes, sign)
    return prefixes[pos].split(",")[1]


def processSignCount(signcount, signPrefixes):
    country = lookupCountry(signcount[0], signPrefixes.value) #broadcast variables are accessible inside task with 'value' property
    count = signcount[1]
    return (country, count)

countryContactCounts = contactCount\
                        .map(lambda signCount: processSignCount(signCount, signPrefixes))\
                        .reduceByKey(lambda x,y: x + y)

countryContactCounts.saveAsTextFile("hdfs:///tmp/countries.txt")


