import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object week12_3 extends App {


    val sparkconf = new SparkConf
    sparkconf.set("spark.app.name","sparksql")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
    .config(sparkconf)
    .getOrCreate()

    import spark.implicits._

    case class scorecard ( matches : Int, player:String, team: String, runscored: Int, strike: Double )

    val rdd1 = spark.sparkContext.textFile("path")

    val rdd2 = rdd1.map( x => x.split("\t")).map( x=> scorecard( x(0).toInt, x(1), x(2), x(3).toInt, x(4).toDouble))

    val scoresdf = rdd2.toDF()

    val runScoredDF = scoresdf.groupBy("player").
    agg(avg("runscored")).as("AvgRunscored").select("player" , "AvgRunscored")

    // now for other file we are using an alternative approach unlike case class

    val rdd3 = spark.sparkContext.textFile("path")

    val rdd4 = rdd3.map( x => x.split("\t")).map( fields => Row(fields(0), fields(1))) // new approach


    val playerSch = StructType(List
    (StructField("player", StringType, false)),
    (StructField("team", StringType)))

    val playerdf = spark.createDataFrame(rdd4, playerSch)

    spark.sql(" SET spark.sql.autoBrodcastJoinThreshold = -1")

    val joinCondition = runScoredDF.col("player") === playerSch.col("player")

    val joinType = "inner"

    val finalDF = runScoredDF.join( broadcast(playerSch), joinCondition, joinType).drop(runScoredDF.col("player"))



}