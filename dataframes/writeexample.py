from pyspark import SparkConf
from pyspark.sql import SparkSession 


my_conf = SparkConf()
my_conf.set("spark.app.name" ,"my_app")
my_conf.set("spark.master" , "local[*]")
#my_conf.set("spark.jars", "path_to Jar") if we are adding jars , for eg: for saving in avro we should have jar

spark = SparkSession.builder.config(conf = my_conf).getOrCreate()

orders = spark.read.format("csv").option("header",True).option("path","").load()

# write methods:
# mode("overwrite")
# mode("append")
# mode("errorIfExists")
# mode("ignore")

orders.write.format("csv").mode("overwrite").option("path","").save()

print(" number of partitions are ", orders.rdd.getNumPartitions())

# giving max number of records
orders.write.format("csv").mode("overwrite").option("path","").option("maxRecordsPerFile" , 2000).save()

# repartition

ordersRep = orders.repartition(4)

ordersRep.write.format("csv").mode("overwrite").option("path","").save()

# partitionBy

orders.write.format("csv").mode("overwrite").option("path","").partitionBy("column_name").save()

# bucketBy

orders.write.format("csv").mode("overwrite").option("path","").bucketBy( 4 , "column_name").saveAsTable()
# save("file_path") we can give file path under this also , instead of option