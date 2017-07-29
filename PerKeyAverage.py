from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("PerKeyAverage")
sc = SparkContext(conf = conf)

# input = number of burgers the animal ate
input = sc.parallelize([("panda",2), ("cat",3), ("dog",7), ("dog",8), ("panda",4), ("cat",5), ("lion",8)])

# calculate average number of burgers eaten by animal
output = input.mapValues(lambda val: (val, 1)).reduceByKey(lambda a,b: (a[0] + b[0], a[1] + b[1]))

# now output is like this: [("panda",(6,2)), ("cat",(8,2)), ("dog",(15,2)), ("lion",(8,1))]

#print average
output = output.mapValues(lambda v : v[0]/v[1])

#prints [("panda",3), ("cat",4), ("dog",7), ("lion",8)]
print output.collect()

#second way using combineByKey
output = input.combineByKey((lambda a : (a,1)),                            # createCombiner method, produces (panda, (2,1)) when key noticed first in a partition
                            (lambda x,y : (x[0] + y, x[1] + 1)),           # mergeValue method, produces (panda, (6,2)) when panda key found again in the partition
                            (lambda x,y : (x[0] + y[0], x[1] + y[1])))     # mergeCombiners method, produces (panda, (6,2)) again assuming everything was in one partition

print output.map(lambda (key, xy): (key, xy[0]/xy[1])).collectAsMap()



