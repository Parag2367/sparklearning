
from pyspark import SparkConf
from pyspark.sql import SparkSession  # spark context is not needed in dataframes

my_conf = SparkConf()
my_conf.set("spark.app.name", "my_app")
my_conf.set("spark.master", "local[*]")

# defining schema StructField way
orderschema = StructType(
    List(
        StructField("order_id", IntegerType, false)
    ),  # false states that nulls are not allowed
    (StructField("orderdate", TimestampType)),
    (StructField("order_customer_id", IntegerType)),
    (StructField("status", StringType)),
)

# defining schema DDL way
orderDDL = "orderid Integer, orderdate Timestamp, custid Integer, status String"

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

ordersDf = spark.read.option("header", True).option("inferSchema", True).csv("path")

grouppedDf = (
    ordersDf.repartition(4)
    .where("order_customer_id > 1000")
    .select("order_id", "order_customer_id")
    .groupBy("order_customer_id")
    .count()
)

grouppedDf.show()

# ordersDf.show()


# standard way of creating dataframes

orderscsv = (
    spark.read.format("csv").path("path of file").option("header", True).load()
)  # load is neccesray when using standard approach

ordersjson = spark.read.format("json").path("path of file").load()

ordersparquet = spark.read.path("path of file").load()


# handling data which is coming in

orders = (
    spark.read.format("json")
    .option(
        "path",
        "C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\sparklearning\\orders-201019-002101.json",
    )
    .option("mode", "DROPMALFORMED")
    .load()
)
# .option("mode","PERMISSIVE") # this is by default we dont have to mention
# .option("mode","FAILFAST")\  #another way of read mode


# using schema defined earlier using StructField

ordersch = (
    spark.read.format("csv")
    .option(
        "path",
        "C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\sparklearning\\orders-201019-002101.csv",
    )
    .schema(orderschema)
    .load()
)

# using schema defined earlier using DDL

ordersddl = (
    spark.read.format("csv")
    .option(
        "path",
        "C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\sparklearning\\orders-201019-002101.csv",
    )
    .schema(orderDDL)
    .load()
)
