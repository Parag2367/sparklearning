from pyspark import SparkConf
from pyspark.sql import SparkSession 

my_conf = SparkConf()
my_conf.set("spark.app.name" ,"my_app")
my_conf.set("spark.master" , "local[*]")

spark = SparkSession.builder.config(conf = my_conf).enableHiveSupport().getOrCreate()


ordersDf = spark.read.\
format("csv").\
option("path","C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\data folder\\week12\\orders-201025-223502.csv").\
option("header",True).\
option("inferSchema", True).\
load()

spark.sql("create database if not exist retail")

ordersDf.write.format("csv").mode("overwrite").saveAsTable("orders1")

ordersDf.write.format("csv").mode("overwrite").saveAsTable("retail.orders2")

ordersDf.write.format("csv").mode("overwrite").partitionby("order_status").bucketBy( 4,"order_customer_id").saveAsTable("retail.orders3")