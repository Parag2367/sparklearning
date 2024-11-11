
// spark-shell --conf spark.dynamicAllocationEnabled=false -- master yarn --num-executors 21

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._ // for structtype

object SparkOptimization_broadcastDF extends App {

    val sparkconf = new SparkConf
    sparkconf.set("spark.app.name" , "invoice")
    sparkconf.set("spark.master" , "local[*]")

    val spark = SparkSession.builder()
    .config(sparkconf)
    .getOrCreate()

// we should not use infer schema as it take time , better to use struct schema or other schema method

val dfcust = spark.read.format("csv").
    option("path", "").
    option("header", True).
    option("inferSchema" , True).
    load()
    
val dforders = spark.read.format("csv").
    option("path", "").
    option("header", True).
    option("inferSchema" , True).
    load()
 
// this way also we can stop auto broadcast , as we know spark itself goes for broadcast, just to check

spark.conf.set(" spark.sql.autoBroadcastJoinThreshold",-1) 


dfjoined = dfcust.join( dforders , dfcust("col1") === dforders("col2") , "inner")
// this join will take more time


// going for a optimized approach
    // 1. using a schema rather than inferschema
    // 2. using a broadcast join

val sch = StructType(List
(StructField("order_id", IntrgerType),
StructField("Col_2", StringType),
StructField("Col_3", TimeStampType)))

val dfcust = spark.read.format("csv").
    option("path", "").
    option("header", True).
    option("inferSchema" , True).
    load()

val dforders = spark.read.format("csv").
    option("path", "").
    option("header", True).
    schema(sch) //it was completed instantly
    load()

// by default it will go for broadcast join
dfjoined = dfcust.join( dforders , dfcust("col1") === dforders("col2") , "inner")

// this way also we can get same thing but we have to stop the defaultb broadcast join
spark.conf.set(" spark.sql.autoBroadcastJoinThreshold",-1)
dfjoin = dforders.join( broadcast(dfcust) , dfcust("col1") === dforders("col2") , "inner")



}