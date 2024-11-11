from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

my_conf = SparkConf()
my_conf.set("spark.app.name" ,"my_app")
my_conf.set("spark.master" , "local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

invoice = spark.read.\
    format("csv").\
    option("path" , "c:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\data folder\\week12\\windowdata-201025-223502.csv").\
    option("header", True).\
    option("inferSchema" , True).\
    load()
# note: wheneever we use window agg, we have to define these three things, partiton,order,window

wind = window.partitionBy("country").orderBy("weeknum").\
rowsBetween(window.unboundedPreceding , window.currentRow)

myDf = invoice.withColumn("RunningTotal" , sum("invoicevalue").over(wind))

myDf.show()