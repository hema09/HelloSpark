from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("UnionApp")
sc = SparkContext(conf = conf)

def containsWarnings(s):
    return "error" in s

lines = sc.textFile("/tmp/service.log")
errors = lines.filter(lambda line: 'error' in line)
warnings = lines.filter(containsWarnings) # notice no (), we passed only function


print "total errors = ", errors.count()
print "total warnings = ", warnings.count()

#filter and union are transformations
badLines = errors.union(warnings)
print "total errors or warnings = ", badLines.count()

# better way
errorsOrWarnings = lines.filter(lambda line: 'error' in line or 'warning' in line)
print "total errors or warnings = ", errorsOrWarnings.count()

#count and take are actions
for line in errorsOrWarnings.take(10):
    print line

errorsOrWarnings.collect() # use only for small datasets that can fit into memory locally

errorsOrWarnings.saveAsTextFile("/tmp/errorsorwarnings.txt")




