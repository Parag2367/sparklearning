import org.apache.spark._
import org.apache.spark.streaming.* 
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SparkSession

object example1 extends App {

    val myconf = new SparkConf
    myconf.set("spark.app.name" , "structurestreaming")
    myconf.set("spark.master" ,"local[2]")
    myconf.set("spark.sql.shuffle.partitions", 3)// reduce the default partition from 200 to 3 to make it fast as data is small
    myconf.set("spark.streaming.stopGraceFullyOnShutdown", "true")
    // myconf.set("spark.sql.streaming.schemaInference", "true") // this if for schema infer for json format or other file format, different way then spark batch processing
    // giving our own schema why? because in this aggregation and grouping is involved, in example1 it was a simple select and count


    val spark = SparkSession.builder().
    config(myconf).
    getOrCreate()

    val sch = Structtype(List(
        StructField("order_id", IntegerType),
        StructField("order_date", TimesstampType),
        StructField("order_customer_id", IntegerType),
        StructField("order_status", StringType),
        StructField("amount", IntegerType)
        
    ))

    val ordersdf = spark.readStream.format("socket").option("host","localhost").option("port",4448).load()

    // processing

    val valuedf = ordersdf.select(from_json(col("value"), sch).alias("values")) //this is a way to inferschema manually using for json
    // this will give a schema which will be under values, for eg : values.order_id, values_order_status, values. , etc we will have to pass values. everytime
    // so to avoid it we doo following

    val refinedf = valuedf.select("value.*")

    // this will give us a root structure just like normal schema

    val windowaggdf = refinedf.groupBy(window(col("order_date"), "15 minute"))// 11 - 11:15, 11:15 - 11:30, 11:30 - 11:45, etc
    .agg(sum("amount")).alias("totalamount")

    // in here also window is giving two layer struct for start, end time

    val outputdf = windowaggdf.select("window.start", "window.end", "totalamount")


    // writing to sink

    val ordersquery = outputdf.writeStream.format("console").outputmode("update").option("checkpointlocation" ,"checkpoint-dir")
    .trigger(Trigger.ProcessingTime("15 second")) // this is a trigger time , and processing happens as per event time so do not get confuse
    .start()

    ordersquery.awaitTermination()


}