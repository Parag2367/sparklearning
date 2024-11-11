from pyspark import SparkConf
from pyspark.sql import SparkSession  # spark context is not needed in dataframes

my_conf = SparkConf()
my_conf.set("spark.app.name", "my_app")
my_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(
    conf=my_conf
).getOrCreate()  # spark session is created , in cli it is already available
