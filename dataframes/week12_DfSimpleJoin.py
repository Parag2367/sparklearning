from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

my_conf = SparkConf()
my_conf.set("spark.app.name" ,"my_app")
my_conf.set("spark.master" , "local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
orders = spark.read.\
                format("csv").\
                option("path" ,"c:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\data folder\\week12\\orders-201025-223502.csv").\
                option("header" ,True).\
                load()

customers = spark.read.\
                format("csv").\
                option("path","c:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\data folder\\week12\\customers-201025-223502.csv").\
                option("header" ,True).\
                load()
                
joinCondition =  orders.order_customer_id == customers.customer_id
joinType = "inner"
joinDf = orders.join( customers , joinCondition , joinType)

joinDf.show()