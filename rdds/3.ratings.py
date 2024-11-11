from pyspark import SparkContext

sc = SparkContext("local[*]", "ratings")
rdd1 = sc.textFile("path")

rdd2 = rdd1.map(lambda x: x.split("\t")[2])

rdd3 = rdd2.map(lambda x : (x,1))

result = rdd3.reduceByKey(lambda x,y : (x+y)).collect()

for a in result:
    print(a)
