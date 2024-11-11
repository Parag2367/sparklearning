from pyspark import SparkContext

sc = SparkContext("local[*]", "Keyword")

initial_rdd = sc.textFile("filepath")
req = initial_rdd.map( lambda x : (float(x.split(",")[10]), x.split(",")[0]))
flatten = req.flatMapValues( lambda x: x.split(" "))
final_mapped = flatten.map( lambda x : (x[1].lower(), x[0]))
final = final_mapped.reduceByKey( lambda x,y: x+y)
sorted = final.sortBy(lambda x: x[1] ,False)
result = sorted.take(20)

for x in result :
    print(x)