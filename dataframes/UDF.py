from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

my_conf = SparkConf()
my_conf.set("spark.app.name" ,"my_app")
my_conf.set("spark.master" , "local[*]")

spark = SparkSession.builder.config(conf = my_conf).getOrCreate()

details = spark.read.\
format("csv").\
option("path","C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\data folder\\week12\\dataset").\
option("header",True).\
option("inferSchema", True).\
load()

df1 = details.toDF("name" , "age" , "city")

def ageCheck(age) :
    if age >18:
        return "Y"
    else:
        return "N"

# used column object method: so that is why function is not registered in catalog 
parseAgeFunction = udf(ageCheck , StringType())

df2 = df1.withColumn("adults" ,parseAgeFunction("age") )


# mind the difference between both the method, in col object type func is not in double quotes, but in other it is in

# using SQL / String expression way : so function will be registerd in catalog

spark.udf.register("parseAgeFunction", ageCheck, StringType())


for x in spark.catalog.listFunctions():
    print (x)


df2 = df1.withColumn("adults" , expr("parseAgeFunction(age)"))