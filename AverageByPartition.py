from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("AverageApp")
sc = SparkContext(conf=conf)

nums = sc.parallelize([1,2,3,4,5,6,7,8,9,10])

def partitionCtrs(nums):
    sumCount = [0,0]
    for num in nums:
        sumCount[0] += num
        sumCount[1] += 1
    return [sumCount]

def combineCtrs(c1, c2):
    return (c1[0]+c2[0], c1[1]+c2[1])

def fastAvg(nums):
    sumCount = nums.mapPartitions(partitionCtrs).reduce(combineCtrs)
    return sumCount[0]/float(sumCount[1])

print "average = %f" % fastAvg(nums)


