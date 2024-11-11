from pyspark import SparkConf
from pyspark.sql import SparkSession 

my_conf = SparkConf()
my_conf.set("spark.app.name" ,"my_app")
my_conf.set("spark.master" , "local[*]")

spark = SparkSession.builder.config(conf = my_conf).getOrCreate()

ordersDf = spark.read.\
format("csv").\
option("path","C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\data folder\\week12\\orders-201025-223502.csv").\
option("header",True).\
option("inferSchema", True).\
load()


ordersDf.createOrReplaceTempView("orders")

countDf = spark.sql(" select order_customer_id , count(*) as total_orders from orders where order_status ='CLOSED' group by order_customer_id order by total_orders desc")
countDf.show()