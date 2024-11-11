from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

my_conf = SparkConf()
my_conf.set("spark.app.name" ,"my_app")
my_conf.set("spark.master" , "local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

mylist = [(1,"2023-04-05",2345,"COMPLETED"),
    (2,"2022-04-05",3476,"COMPLETED"),
    (3,"2021-09-12",2255,"PENDING"),
    (4,"2020-09-09",4477,"CLOSED")]


ordersDf = spark.createDataFrame(mylist).\
    toDF("order_id","date","cust_id","status")
    
newDf = ordersDf.withColumn("date", unix_timestamp(col("date").\
            cast(date_format()))).\
            withColumn("newId" , monotonically_increasing_id()).\
            dropDuplicates("date" , "cust_id").\
            drop("order_id").\
            sort("date")
