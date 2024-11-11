from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

my_conf = SparkConf()
my_conf.set("spark.app.name" ,"my_app")
my_conf.set("spark.master" , "local[*]")

spark = SparkSession.builder.config(conf = my_conf).getOrCreate()

# in this we will be using dataframe not like we did in scala for same program

myregex = r'^(\S+) (\S+)\t(\S+)\,(\S+)'

lines_df = spark.read.text("path")

lines_df.select(regexp_extract('value',myregex,1).alias("order_id"),regexp_extract('value',myregex,2).alias("date"), regexp_extract('value',myregex,3).alias("customer_id"), regexp_extract('value',myregex,4).alias("status"))