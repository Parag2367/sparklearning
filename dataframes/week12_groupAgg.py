from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

my_conf = SparkConf()
my_conf.set("spark.app.name" ,"my_app")
my_conf.set("spark.master" , "local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

invoice = spark.read.\
    format("csv").\
    option("path" , "c:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\data folder\\week12\\order_data-201025-223502.csv").\
    option("header", True).\
    option("inferSchema" , True).\
    load()
    
    
    # column object method

summary = invoice.groupBy("Country" , "InvoiceNo").\
    agg(sum("Quantity").alias("TotalQuantity"),
    sum(expr("Quantity * UnitPrice")).alias("InvoiceValue"))


    # column string method

Stringsummary = invoice.groupBy("Country" , "InvoiceNo").\
    agg(expr(" sum(Quantity) as TotalQuantity"),
    expr(" sum (Quantity * UnitPrice) as InvoiceValue"))


    # Spark SQL way

invoice.createOrReplaceTempView("Details")

SQLSummary = spark.sql(""" select Country, InvoiceNo, sum(Quantity) as TotalQuantity , sum(Quantity * UnitPrice) as InvoiceValue 
    from Details
    group by Country, InvoiceNo """)