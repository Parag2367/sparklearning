import org.apache.spark._
import org.apache.spark.streaming.* 
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SparkSession

object rightouterjoin extends App {

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

    // Streaming dataframe
    val cardSch = Structtype(List(
        StructField("card_id", IntegerType),
        StructField("transaction_date", TimesstampType),
        StructField("pincode", IntegerType),
        StructField("status", StringType),
        StructField("amount", IntegerType)
        
    ))

    val transacdf = spark.readStream.format("socket").option("host","localhost").option("port", 23476).load()

    val formatdf = transacdf.select(from_json(col("value"),cardSch).alias("values"))

    val refineddf  = formatdf.select("values.*")


    // static dataframe

    val memberdf = spark.read.format("csv").option("header", true).option("inferSchema" , true).load()


    // join condition

    val joinCond = refineddf.col("card_id") === memberdf.col("card_id")

    val joinType = "rightouter"  // only works if right table is streaming and if not it will throw out an error

    val finaldf = memberdf.join( refineddf, joinCond, joinType)
    .drop(memberdf.col("card_id")) // dropping col as same name of col


    
    // write to the sink

    val transactionquery = finaldf.writeStream.format("console").outputMode("update")
    .option("checkpointlocation" , "check-pointdir")
    .trigger(Trigger.ProcessingTime("15 second"))
    .start()

    transactionquery.awaitTermination()

}