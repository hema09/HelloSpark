from pyspark import SparkContext, SparkConf, StorageLevel

conf = SparkConf().setAppName("filterApp")
sc = SparkContext(conf=conf)

lines = sc.textFile("/tmp/README.md")
filteredLines = lines.filter(lambda line: 'Python' in line)

filteredLines.persist(StorageLevel.useMemory)
filteredLines.count()
filteredLines.first()
filteredLines.unpersist()
