from pyspark import SparkConf
from pyspark.sql import SparkSession   # spark context is not needed in dataframes
from pyspark.sql.functions import *


my_conf = SparkConf()
my_conf.set("spark.app.name", "my_app")
my_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf = my_conf).getorCreate()

ordersDf = spark.read.format("csv").option("path","").option("header",True).option("inferSchema", True).load()


# column string method
ordersDf.select("order_id","order_status").show()

# column object method
ordersDf.select(col("order_id"). col("order_status")).show()

