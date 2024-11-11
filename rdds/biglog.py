from pyspark import SparkContext

sc = SparkContext("local[*]", "biglog")
sc.setLogLevel("INFO")
base_rdd = sc.textFile("filepath")

rdd1 = base_rdd.map(lambda x: (x.split(":")[0], x.split(":")[1]))

rdd2 = rdd1.groupByKey()

rdd3 = rdd2.map(lambda x: (x[0], len(x[1]))).collect()

for a in rdd3 :
    print(a)

