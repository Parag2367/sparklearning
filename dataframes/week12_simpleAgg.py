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

invoice.select(
    count("*").alias("RowCount"),
    sum("Quantity").alias("TotalQuantity"),
    avg("UnitPrice").alias("Average"),
    countDistinct("InvoiceNo").alias("DistinctInvoice")).\
    show()

# column string method

invoice.selectExpr(
        "count(*) as RowCount",
        "sum(Quantity) as TotalQuantity",
        "avg(UnitPrice) as Average",
        "count(Distinct(InvoiceNo)) as DistinctInvoice").\
        show()

# Spark Sql , easier way as compared to dataframne and datasets

invoice.createOrReplaceTempView("sales")

spark.sql(" select count(*), sum(Quantity) , avg(UnitPrice) , count(Distinct(InvoiceNo)) from sales").show()