from pyspark import SparkContext

sc = SparkContext("local[*]", "loglevel")
sc.setLogLevel("INFO")

if __name__ == "__main__":  # this will be executed when it is directly ran from here
    
    mylist = ["WARN: this is my",
        "ERROR: my gpaytk",
        "ERROR: 28 September",
        "WARN: this 54 times"]

    original_rdd = sc.parallelize(mylist)
    
else : # this will be executed when it will run from or called from other module , like import sparkad1
    original_rdd = sc.textFile("path")
    print("executing else part")

logs = original_rdd.map( lambda x : (x.split(":")[0],1))
result = logs.reduceByKey( lambda x,y : (x+y))

final = result.collect()

for x in final:
    print(x)
