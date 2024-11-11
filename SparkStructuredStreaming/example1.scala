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
    myconf.set("spark.sql.streaming.schemaInference", "true") // this if for schema infer for json format or other file format, different way then spark batch processing



    val spark = SparkSession.builder().
    config(myconf).
    getOrCreate()

    // read the stream
    // File source: very popular

    val filedf = spark.readStream.format("json").option("path","C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\sparklearning\\inputfolder").
    option("maxFilePerTrigger",1).
    load()


    // val filterDf = filedf.filter(col("order_status") === "COMPLETED")

    filedf.createOrReplaceTempView("orders")

    val orderDf = spark.sql(" select * from orders eher order_status ='COMPLETED' ")

    val countDf = spark.sql(" select count(*) from orders where order_status ='COMPLETED' ")


    val writeDf = orderDf.writeStream.format("json").option("path", "").outputMode("append").
    option("checkpointLocation", "checkpoint-file2").trigger(Trigger.ProcessingTime("30 seconds")).start()

    writeDf.awaitTermination()

}