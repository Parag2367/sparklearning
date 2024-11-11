from pyspark import SparkContext
from sys import stdin

sc = SparkContext("local[*]", "customers")

base_rdd = sc.textFile("path")

rdd1 = base_rdd.map( lambda x: (x.split(",")[0], float(x.split(",")[2])))

rdd2 = rdd1.reduceByKey( lambda x,y: (x+y))

rdd3 = rdd2.filter( lambda x: x[1] > 5000 )

rdd4 = rdd2.map(lambda x: (x[0], x[1]*2)).persist(storageLevel="MEMORY_ONLY")
# rdd4 = rdd2.map(lambda x: (x[0], x[1]*2)).cache()

result = rdd4.collect()

for x in result:
    print(x)
    
print(rdd4.count())

stdin.readlines()   # for programing to keep running and stop when we want it