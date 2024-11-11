from pyspark import SparkContext

def loadBoringWords():
    boringWords = set( line.strip() for line in open("path"))
    return boringWords

sc = SparkContext("local[*]", "boringwords")

name_set = sc.broadcast(loadBoringWords)

initial_rdd = sc.textFile("filepath")
req = initial_rdd.map( lambda x : (float(x.split(",")[10]), x.split(",")[0]))
flatten = req.flatMapValues( lambda x: x.split(" "))
final_mapped = flatten.map( lambda x : (x[1].lower(), x[0]))

filtered = final_mapped.filter(lambda x: x[0] not in name_set.value)

final = filtered.reduceByKey( lambda x,y: x+y)
sorted = final.sortBy(lambda x: x[1] ,False)