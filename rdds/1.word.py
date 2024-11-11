from pyspark import SparkContext

sc = SparkContext("local[*]", "wordcount")
sc.setLogLevel("ERROR")
input = sc.textFile(
    r"C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\sparklearning\\words.txt"
)

# one input row will give multiple output rows
words = input.flatMap(lambda x: x.split(" "))

# one input will be giving one output
wordCounts = words.map(lambda x: (x, 1))

count = wordCounts.reduceByKey(lambda x, y: (x + y))

result = count.collect()

for a in result:
    print(a)
