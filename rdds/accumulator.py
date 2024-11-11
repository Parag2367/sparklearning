from pyspark import SparkContext

def lenaccum(line):
    if len(line) == 0:
        myaccum.add(1)

sc = SparkContext("local[*]", "accum")

myrdd = sc.textFile("path")

myaccum = sc.accumulator(0)  # sc.accumulator(0.0) there are two types of accumulator int, float

myrdd.foreach(lenaccum)

print(myaccum.value)