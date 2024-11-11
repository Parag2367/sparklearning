from pyspark import SparkContext

sc = SparkContext("local[*]", "join")

base_rdd = sc.textFile("C:\Users\pp255070\OneDrive - Teradata\Documents\sparklearning\ratings-201019-002101.dat")

rdd1 = base_rdd.map( lambda x: (x.split("::")[1], x.split("::")[2]))

rdd2 = rdd1.mapValues( lambda x : (float(x),1.0))

rdd3 = rdd2.reduceByKey( lambda x,y : (x[0]+y[0], x[1]+y[1]))

rdd4 = rdd3.filter( lambda x : (x[1][0] > 1000))

rdd5 = rdd4.map( lambda x : (x[0]/x[1])).filter( lambda x: x[1] > 4.5)

base_rdd2 = sc.textFile("C:\Users\pp255070\OneDrive - Teradata\Documents\sparklearning\movies-201019-002101.dat")

m_rdd1 = base_rdd2.map( lambda x : ( x.split("::")[0], ( x.split("::")[1], x.split("::")[2])))

m_rdd2 = m_rdd1.join(rdd5)   # join condition

topmovies = m_rdd2.map( lambda x: x[1][0])

result = topmovies.collect()

for x in result:
    print(x)
    
print(topmovies.count())