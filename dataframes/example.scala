import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf


object example extends App {

//another way of creating spark session
    val sparkconf = new SparkConf
    sparkconf.set("spark.app.name","my app")
    sparkconf.set("spark.master","local[2]")

    val spark = SparkSession.builder()
    .config(sparkconf)
    .getOrCreate()          // creating a spark session



// one way of creating spark session
    //val spark = SparkSession.builder()
    //.appName("My app")
    //.master("local[2]")
    //.getOrCreate()      

    spark.stop()      // stopping a spark session

}