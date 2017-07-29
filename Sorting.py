from pyspark import SparkContext, SparkConf

# set up configuration and context
conf = SparkConf().setAppName("SortApp")
sc = SparkContext(conf=conf)

input = sc.parallelize([3,7,9,3,2,1])

input = input.sortBy(keyfunc= lambda x: x, ascending= True, numPartitions=None)

#prints [1,2,3,3,7,9]
print input.collect()

inputMap = sc.parallelize([(1,"aa"), (2,"aa"), (1,"aa"), (31,"aa"), (8,"aa"), (16,"aa"), (1,"aa")])

# sort map with int keys treated as strings
inputMap = inputMap.sortByKey(True, numPartitions=None, keyfunc= lambda x: str(x))

# prints [(1, 'aa'), (1, 'aa'), (1, 'aa'), (16, 'aa'), (2, 'aa'), (31, 'aa'), (8, 'aa')]
print inputMap.collect()


