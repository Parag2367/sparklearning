import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object week9 extends App {
    val sparkconf = new SparkConf
    sparkconf.set("spark.app.name","sparksql")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
    .config(sparkconf)
    .getOrCreate()

    val temp = "stationId string, timeOfTheReading timestamp, readingType string, temperatureRecorded float,a int,b int,c string"

    val df1 = spark.read.
                format("csv").
                option("path" , "c:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\assignment\\week9\\tempdata-201125-161348.csv").
                schema(temp).
                option("header",true).
                load()

    df1.createOrReplaceTempView("temperatures")

    val minTemp = spark.sql("select stationId , min(temperatureRecorded) from temperatures group by stationId")


}