from pyspark import SparkContext, SparkConf
import re

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

if invalidSign.value < 0.1 * validSign.value: # invalid value should be less than 10% of valid values
    contactCount.saveAsTextFile("hdfs:///tmp/ContactCount")
else:
    print "Too many errors %d incorrect compared to %d correct", invalidSign.value, validSign.value