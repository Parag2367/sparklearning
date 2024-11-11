import org.apache.spark._
import org.apache.spark.streaming.* 
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SparkSession

object leftrightouterjoin_streamtostream extends App {

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


    //val joincondition = fimp.col("impression_id") === fclick.col("click_id") //another way of achieving same is below
    val joincondition = expr("impression_id == click_id AND ClickTime between ImpressionTime and ImpressionTime + interval 15 minute ")
    // max time interval is 15 minute 
    // watermark for click will be : Max(time) -30 minute - 15 minute
    val joinType = "leftouter"

    val resultDf = fimp.join( fclick, joincondition, joinType)

    val addDf = resultDf.writeStream.format("console").
    outputMode("append").
    option("checkpointlocation", "check_dir").trigger(Trigger.ProcessingTime("15 second")).start()

    addDf.awaitTermination()


}