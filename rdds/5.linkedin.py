def parseline (line):
    fields = line.split(",")
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age,numFriends)
    
    

from pyspark import SparkContext
sc = SparkContext("local[*]", "linkedin")
input = sc.textFile("path")

rdd1 = input.map(parseline)

rdd2 = rdd1.mapValues(lambda x : (x,1))

rdd3 = rdd2.reduceByKey(lambda x,y: (x[0]+y[0] , x[1]+y[1]))

rdd4 = rdd3.mapValues(lambda x : x[0]/x[1] )

result = rdd4.collect()

for a in result:
    print(a)