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

    val spark = SparkSession.builder().
    config(myconf).
    getOrCreate()

    val impresssch = StructType(List(
        StructField("impression_id", IntegerType),
        StructField("impression_time", TimestampType),
        StructField("campaign_name", StringType)
    ))

    val impresdf = spark.readStream.format("socket").option("host","localhost").option("port", 2343).load()
// did this everywhere to format the json tree data , as we only need values. and it forms a tree like structure
    val iformatdf = impresdf.select( from_json(col("value"),impresssch).alias("values"))
    val fimp = iformatdf.select("value.*").
    withWatermark("impression_time","30 minute") // adding watermark to clean state store

    val clicksch = StructType(List(
        StructField("click_id", IntegerType),
        StructField("click_time", TimestampType)
    ))

    val clickdf = spark.readStream.format("socket").option("host","localhost").option("port", 2343).load()

    val cformatdf = clickdf.select( from_json(col("value"),impresssch).alias("values"))
    val fclick = cformatdf.select("value.*")
    .withWatermark("click_time", "30 minute") // adding watermark to clean state store


    val joincondition = fimp.col("impression_id") === fclick.col("click_id")
    //val joincondition = expr("impression_id == click_id") another way of achieving same
    val joinType = "inner"

    val resultDf = fimp.join( fclick, joincondition, joinType)
    .drop(fclick.col("click_id")) // dropping to keep only one column for id

    val addDf = resultDf.writeStream.format("console").
    outputMode("append").
    option("checkpointlocation", "check_dir").trigger(Trigger.ProcessingTime("15 second")).start()

    addDf.awaitTermination()


}